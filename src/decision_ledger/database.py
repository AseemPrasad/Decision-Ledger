"""SQLite persistence layer for decisions and outcomes.

The layer owns the durable copy of the ledger: every decision that passed
through the gatekeeper, its independent outcome observations, the flattened
decision-outcome join records used for calibration, and the policy history.

Design notes (MVP):

* **Thread-local connections** -- one connection per thread is created on
  first use (``threading.local``), so concurrent threads never share a
  connection object. No explicit connection pool; SQLite is slow to open and
  fully safe this way. Each connection runs in autocommit mode
  (``isolation_level = None``), so every write is durable immediately and
  ``execute_write`` never needs an explicit commit.
* **Foreign keys** are enforced per connection
  (``PRAGMA foreign_keys = ON``); the ``outcomes.decision_id`` reference
  rejects outcomes that do not point at a stored decision.
* **Locked retries** -- writers that hit ``database is locked`` retry up to
  three times with a short backoff, then raise :class:`DatabaseError`.
* **No WAL** -- the workspace is an OneDrive-synced folder and WAL adds
  ``-wal``/``-shm`` sidecar files that the sync layer churns on; the default
  rollback journal keeps the ledger a single file. A ``busy_timeout`` plus
  the locked retry above covers contention.
* **``outcome_source`` is TEXT** per the schema
  (``'human'``, ``'task_metric'``, ``'model_verification'``). The in-memory
  domain type :class:`~decision_ledger.outcomes.OutcomeSource` is an int
  enum; use :func:`outcome_source_to_text` / :func:`outcome_source_from_text`
  at the boundary.
* **``:memory:`` caveat** -- each thread gets its *own* connection, so an
  in-memory database is per-thread by construction. Use a file path for any
  database shared across threads.

All SQL errors are logged and surfaced as :class:`DatabaseError` (or
:class:`DatabaseIntegrityError` for constraint violations). Schema creation
and verification are idempotent and safe to run repeatedly.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_CONNECT_TIMEOUT_S = 2.0
_BUSY_TIMEOUT_MS = 2000
_LOCK_RETRIES = 3
_LOCK_RETRY_DELAY_S = 0.05

_COLUMN_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DatabaseError(Exception):
    """Raised when a database operation fails for any reason."""


class DatabaseIntegrityError(DatabaseError):
    """Raised when an operation violates a constraint (PK, FK, NOT NULL)."""


# outcome_source ints mirror decision_ledger.outcomes.OutcomeSource in order.
OUTCOME_SOURCE_TEXT: tuple[str, ...] = (
    "human",
    "task_metric",
    "model_verification",
    "user_report",
)


def outcome_source_to_text(source: int) -> str:
    """Map an :class:`OutcomeSource` int to the ``outcomes.outcome_source`` TEXT value."""
    if not 0 <= source < len(OUTCOME_SOURCE_TEXT):
        raise ValueError(f"unknown outcome source: {source}")
    return OUTCOME_SOURCE_TEXT[source]


def outcome_source_from_text(text: str) -> int:
    """Map an ``outcomes.outcome_source`` TEXT value back to an :class:`OutcomeSource` int."""
    try:
        return OUTCOME_SOURCE_TEXT.index(text)
    except ValueError:
        raise ValueError(f"unknown outcome source text: {text!r}") from None


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS decisions (
    decision_id      TEXT    NOT NULL PRIMARY KEY,
    timestamp_ns     INTEGER NOT NULL,
    context_hash     BLOB    NOT NULL,
    decision_type    TEXT    NOT NULL,
    model_confidence REAL    NOT NULL,
    non_conformity   REAL    NOT NULL,
    action_taken     TEXT    NOT NULL,
    latency_us       INTEGER
);

CREATE INDEX IF NOT EXISTS idx_decisions_timestamp_ns
    ON decisions (timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_decisions_context_hash
    ON decisions (context_hash);
CREATE INDEX IF NOT EXISTS idx_decisions_ctx_action
    ON decisions (context_hash, action_taken);

CREATE TABLE IF NOT EXISTS outcomes (
    outcome_id     TEXT NOT NULL PRIMARY KEY,
    decision_id    TEXT NOT NULL REFERENCES decisions (decision_id),
    timestamp_ns   INTEGER NOT NULL,
    outcome_value  REAL NOT NULL,
    outcome_source TEXT NOT NULL DEFAULT 'task_metric',
    metadata       TEXT
);

CREATE INDEX IF NOT EXISTS idx_outcomes_decision_id
    ON outcomes (decision_id);
CREATE INDEX IF NOT EXISTS idx_outcomes_source
    ON outcomes (outcome_source);

CREATE TABLE IF NOT EXISTS joined_records (
    joined_id             TEXT PRIMARY KEY,
    decision_id           TEXT,
    context_hash          BLOB,
    decision_type         TEXT,
    model_confidence      REAL,
    non_conformity        REAL,
    action_taken          TEXT,
    outcome_value         REAL,
    decision_timestamp_ns INTEGER,
    outcome_timestamp_ns  INTEGER,
    latency_delta_ns      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_joined_context_hash
    ON joined_records (context_hash);
CREATE INDEX IF NOT EXISTS idx_joined_decision_timestamp
    ON joined_records (decision_timestamp_ns);

CREATE TABLE IF NOT EXISTS policies (
    policy_id      TEXT PRIMARY KEY,
    version_string TEXT,
    generated_at   INTEGER,
    policy_yaml    TEXT,
    is_active      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_policies_is_active
    ON policies (is_active);
CREATE INDEX IF NOT EXISTS idx_policies_generated_at
    ON policies (generated_at);
"""

_EXPECTED_TABLES: tuple[str, ...] = (
    "decisions",
    "outcomes",
    "joined_records",
    "policies",
)

_EXPECTED_INDEXES: tuple[str, ...] = (
    "idx_decisions_timestamp_ns",
    "idx_decisions_context_hash",
    "idx_decisions_ctx_action",
    "idx_outcomes_decision_id",
    "idx_outcomes_source",
    "idx_joined_context_hash",
    "idx_joined_decision_timestamp",
    "idx_policies_is_active",
    "idx_policies_generated_at",
)

_ALLOWED_TABLES: tuple[str, ...] = (
    "decisions",
    "outcomes",
    "joined_records",
    "policies",
)


def init_database(db_path: str = "ledger.db") -> None:
    """Create ``db_path`` (and its parent directory) with the full schema.

    Creates the database file if it does not exist, creates every table and
    index, and verifies the resulting schema. Safe to call repeatedly; a
    second run on an existing database is a no-op.

    Args:
        db_path: Path to the SQLite file. Relative paths are resolved against
            the current working directory.

    Raises:
        DatabaseError: If the schema cannot be created or verified.
    """
    database = Database(db_path)
    try:
        database.init_schema()
        database.verify_schema()
    finally:
        database.close()


class Database:
    """Thread-local SQLite connection manager for the ledger schema.

    Connections are created lazily, one per thread, and the schema is
    (re)applied idempotently when a new connection is opened. Writes are
    autocommitted; reads never mutate state.

    Example:
        >>> db = Database("ledger.db")
        >>> db.execute_write(
        ...     "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
        ...     " decision_type, model_confidence, non_conformity, action_taken)"
        ...     " VALUES (?, ?, ?, ?, ?, ?, ?)",
        ...     ("d-1", 1, b"\\x00" * 16, "route", 0.9, 0.1, "DELEGATE"),
        ... )
        1
        >>> db.get_decisions()[0]["decision_id"]
        'd-1'
    """

    def __init__(self, db_path: str = "ledger.db") -> None:
        self._db_path = Path(db_path)
        self._local: threading.local = threading.local()
        self._connections: set[sqlite3.Connection] = set()
        self._connections_lock = threading.Lock()
        self._closed = False

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Connection lifecycle
    # ------------------------------------------------------------------ #

    def _get_conn(self) -> sqlite3.Connection:
        """Return the calling thread's connection, opening it if necessary."""
        if self._closed:
            raise DatabaseError("database is closed")
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._open_new()
            self._local.conn = conn
        return conn

    def _open_new(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=_CONNECT_TIMEOUT_S)
        conn.row_factory = sqlite3.Row
        conn.isolation_level = None  # autocommit: every write is durable
        conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys = ON")
        with self._connections_lock:
            self._connections.add(conn)
        self._write_schema(conn)
        return conn

    def close(self) -> None:
        """Close every connection opened by this instance.

        After closing, calling any method on this instance raises
        :class:`DatabaseError`.
        """
        if self._closed:
            return
        self._closed = True
        with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()
        for conn in connections:
            try:
                conn.close()
            except sqlite3.Error as exc:
                logger.warning("error closing database connection: %s", exc)
        self._local.conn = None

    # ------------------------------------------------------------------ #
    # Schema
    # ------------------------------------------------------------------ #

    def init_schema(self) -> None:
        """Create all tables and indexes.

        Idempotent: uses ``CREATE ... IF NOT EXISTS`` and is safe to call
        repeatedly, on the same instance or across processes, while other
        threads are writing.
        """
        self._write_schema(self._get_conn())

    def _write_schema(self, conn: sqlite3.Connection) -> None:
        try:
            conn.executescript(_SCHEMA_SQL)
        except sqlite3.Error as exc:
            logger.error("schema creation failed: %s", exc)
            raise DatabaseError(f"schema creation failed: {exc}") from exc

    def verify_schema(self) -> None:
        """Assert every expected table and index exists and the file is sound.

        Checks ``sqlite_master`` for the full table/index set and runs
        ``PRAGMA quick_check``.

        Raises:
            DatabaseError: If any table or index is missing or the integrity
                check fails.
        """
        conn = self._get_conn()
        self._verify_schema(conn)

    def _verify_schema(self, conn: sqlite3.Connection) -> None:
        check = conn.execute("PRAGMA quick_check").fetchone()
        if check is None or check[0] != "ok":
            raise DatabaseError(f"quick_check failed: {check!r}")

        missing: List[str] = []
        for kind, name in (("table", _EXPECTED_TABLES), ("index", _EXPECTED_INDEXES)):
            for item in name:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?",
                    (kind, item),
                ).fetchone()
                if row is None:
                    missing.append(f"{kind} {item}")
        if missing:
            raise DatabaseError("schema incomplete; missing: " + ", ".join(missing))

    # ------------------------------------------------------------------ #
    # Lock retry
    # ------------------------------------------------------------------ #

    def _run_with_lock_retry(self, operation: str, fn: Callable[[], T]) -> T:
        """Run ``fn``, retrying up to ``_LOCK_RETRIES`` times on ``database is locked``."""
        last_error: Optional[sqlite3.OperationalError] = None
        for attempt in range(_LOCK_RETRIES):
            try:
                return fn()
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                last_error = exc
                logger.warning(
                    "database is locked; %s retry %d/%d",
                    operation,
                    attempt + 1,
                    _LOCK_RETRIES,
                )
                time.sleep(_LOCK_RETRY_DELAY_S * (attempt + 1))
        raise DatabaseError(
            f"database is locked after {_LOCK_RETRIES} attempts ({operation})"
        ) from last_error

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def execute_query(
        self, query: str, params: tuple[Any, ...] = ()
    ) -> List[sqlite3.Row]:
        """Execute a SELECT and return the matching rows.

        Rows are :class:`sqlite3.Row` objects: they support both tuple-style
        indexing (``row[0]``) and mapping access (``row["col"]``). A scalar
        result is therefore ``rows[0][0]``; an empty result is an empty list.

        Args:
            query: Any ``SELECT`` statement.
            params: Bound parameters, in order of the ``?`` placeholders.

        Returns:
            List of result rows (or single value as ``rows[0][0]``).

        Raises:
            DatabaseError: If the query or its parameters are invalid, or
                retries are exhausted on a locked database.
        """
        if not isinstance(params, tuple):
            params = tuple(params)
        conn = self._get_conn()

        def _do() -> List[sqlite3.Row]:
            return list(conn.execute(query, params))

        try:
            return self._run_with_lock_retry("query", _do)
        except sqlite3.Error as exc:
            logger.error("database query failed: %s", exc)
            raise DatabaseError(f"{exc} (query: {query})") from exc

    def execute_write(self, query: str, params: tuple[Any, ...] = ()) -> int:
        """Execute an INSERT/UPDATE/DELETE and return the affected row count.

        The write is autocommitted immediately. Constraint violations
        (duplicate primary key, foreign key, NOT NULL) raise
        :class:`DatabaseIntegrityError`.

        Args:
            query: Any data-modifying SQL statement.
            params: Bound parameters, in order of the ``?`` placeholders.

        Returns:
            Number of rows modified.

        Raises:
            DatabaseError: On any SQL failure after lock retries are exhausted.
            DatabaseIntegrityError: On constraint violations.
        """
        if not isinstance(params, tuple):
            params = tuple(params)
        conn = self._get_conn()

        def _do() -> int:
            cursor = conn.execute(query, params)
            return int(cursor.rowcount)

        try:
            return self._run_with_lock_retry("write", _do)
        except sqlite3.IntegrityError as exc:
            logger.error("constraint violation on write: %s", exc)
            raise DatabaseIntegrityError(f"{exc} (query: {query})") from exc
        except sqlite3.Error as exc:
            logger.error("database write failed: %s", exc)
            raise DatabaseError(f"{exc} (query: {query})") from exc

    def batch_insert(self, table: str, records: List[dict[str, Any]]) -> int:
        """Insert many records into one table with a single prepared statement.

        The whole batch runs inside one transaction, so it is atomic: either
        every record is written or none is. Column names are taken from the
        first record and must match exactly (same names, same order) in every
        record. ``table`` must be one of the known ledger tables.

        Args:
            table: One of ``decisions``, ``outcomes``, ``joined_records``,
                ``policies``.
            records: Row dictionaries keyed by column name.

        Returns:
            Number of records inserted.

        Raises:
            DatabaseError: For an unknown table, ragged records, or any SQL
                failure after lock retries are exhausted.
            DatabaseIntegrityError: On constraint violations.
        """
        if table not in _ALLOWED_TABLES:
            raise DatabaseError(f"unknown table: {table!r}")
        if not records:
            return 0

        columns = [str(name) for name in records[0].keys()]
        for name in columns:
            if not _COLUMN_NAME_RE.match(name):
                raise DatabaseError(f"invalid column name: {name!r}")

        rows: List[tuple[Any, ...]] = []
        for record in records:
            if list(record.keys()) != columns:
                raise DatabaseError("all records must have identical columns")
            rows.append(tuple(record[name] for name in columns))

        columns_sql = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        statement = f"INSERT INTO {table} ({columns_sql}) VALUES ({placeholders})"
        conn = self._get_conn()

        def _do() -> int:
            conn.execute("BEGIN")
            try:
                conn.executemany(statement, rows)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            return len(rows)

        try:
            return self._run_with_lock_retry("batch_insert", _do)
        except sqlite3.IntegrityError as exc:
            logger.error("constraint violation during batch_insert(%s): %s", table, exc)
            raise DatabaseIntegrityError(f"{exc} (table: {table})") from exc
        except sqlite3.Error as exc:
            logger.error("batch_insert(%s) failed: %s", table, exc)
            raise DatabaseError(f"{exc} (table: {table})") from exc

    # ------------------------------------------------------------------ #
    # Domain queries
    # ------------------------------------------------------------------ #

    def get_decisions(
        self,
        context_hash: Optional[bytes] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[dict[str, Any]]:
        """Query decisions, optionally filtered, ordered oldest-first.

        Args:
            context_hash: If given, only decisions for this 16-byte context
                hash. Pass the raw bytes, not a hex string.
            start_time: If given, only decisions with ``timestamp_ns >= start_time``.
            end_time: If given, only decisions with ``timestamp_ns <= end_time``.

        Returns:
            List of decision rows as dicts; ``latency_us`` may be ``None`` and
            ``context_hash`` is bytes.
        """
        clauses: List[str] = []
        params: List[Any] = []
        if context_hash is not None:
            clauses.append("context_hash = ?")
            params.append(context_hash)
        if start_time is not None:
            clauses.append("timestamp_ns >= ?")
            params.append(start_time)
        if end_time is not None:
            clauses.append("timestamp_ns <= ?")
            params.append(end_time)

        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT * FROM decisions{where} ORDER BY timestamp_ns ASC"
        rows = self.execute_query(query, tuple(params))
        return [dict(row) for row in rows]

    def get_outcomes(self, decision_id: Optional[str] = None) -> List[dict[str, Any]]:
        """Query outcome records, optionally for one decision.

        Args:
            decision_id: If given, only outcomes attached to this decision.

        Returns:
            List of outcome rows as dicts; ``metadata`` is a JSON string (or
            ``None``) and ``outcome_source`` is TEXT.
        """
        if decision_id is None:
            rows = self.execute_query(
                "SELECT * FROM outcomes ORDER BY timestamp_ns ASC"
            )
        else:
            rows = self.execute_query(
                "SELECT * FROM outcomes WHERE decision_id = ? ORDER BY timestamp_ns ASC",
                (decision_id,),
            )
        return [dict(row) for row in rows]
