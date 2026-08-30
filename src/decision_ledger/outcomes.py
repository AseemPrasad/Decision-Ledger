"""Outcome collection and decision-outcome joining.

Outcomes arrive from sources *independent* of the gatekeeper (human review,
task metrics, model verification, user reports) anywhere from seconds to days
after a decision. Each outcome carries the original ``decision_id`` so the
calibration engine can join it back and estimate empirical risk.

Two collectors live here:

* :class:`OutcomeCollector` (the primary path) -- persists outcomes to SQLite
  through the database layer. Every record is validated (decision exists,
  ``outcome_value`` in [0.0, 1.0], a known :class:`OutcomeSource`, metadata
  that is valid JSON), stamped with a UUIDv7 ``outcome_id`` and a nanosecond
  timestamp, then inserted with :meth:`Database.batch_insert
  <decision_ledger.database.Database.batch_insert>` so a batch is a single
  transaction.
* :class:`InMemoryOutcomeCollector` (offline demos/tests) -- accumulates
  :class:`OutcomeRecord` objects and can export/import JSONL without a
  database. It feeds :class:`DecisionOutcomeJoiner` the same way.

A CLI is available: ``python -m decision_ledger.outcomes``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, TypedDict

from .database import Database, DatabaseError
from .telemetry import DecisionRecord
from .utils import generate_uuidv7, now_ns

logger = logging.getLogger(__name__)

_MISSING_DECISION_CHUNK = 500


class _OutcomeRow(TypedDict):
    """Row shape for inserts into the ``outcomes`` table."""

    outcome_id: str
    decision_id: str
    timestamp_ns: int
    outcome_value: float
    outcome_source: str
    metadata: Optional[str]


class OutcomeSource(Enum):
    """Source of an outcome observation (stored as TEXT in the schema)."""

    HUMAN = "human"
    TASK_METRIC = "task_metric"
    MODEL_VERIFICATION = "model_verification"
    USER_REPORT = "user_report"


class DecisionNotFoundError(Exception):
    """Raised when an outcome refers to a decision the ledger has not recorded."""


class InvalidOutcomeValueError(Exception):
    """Raised when ``outcome_value`` is not a number in [0.0, 1.0]."""


class InvalidOutcomeSourceError(Exception):
    """Raised when ``outcome_source`` is not a known :class:`OutcomeSource`."""


class InvalidMetadataError(Exception):
    """Raised when ``metadata`` is not valid JSON."""


# --------------------------------------------------------------------------- #
# Validation helpers (shared by the collectors and the CLI)
# --------------------------------------------------------------------------- #


def _coerce_outcome_value(value: Any) -> float:
    """Validate and normalize an outcome value to a float in [0.0, 1.0]."""
    if isinstance(value, bool) or value is None:
        raise InvalidOutcomeValueError(
            f"outcome_value must be a number in [0.0, 1.0], got {value!r}"
        )
    if isinstance(value, str):
        try:
            number = float(value)
        except ValueError:
            raise InvalidOutcomeValueError(
                f"outcome_value must be a number in [0.0, 1.0], got {value!r}"
            ) from None
    elif isinstance(value, (int, float)):
        number = float(value)
    else:
        raise InvalidOutcomeValueError(
            f"outcome_value must be a number in [0.0, 1.0], got {value!r} "
            f"of type {type(value).__name__}"
        )
    if not 0.0 <= number <= 1.0:
        raise InvalidOutcomeValueError(f"outcome_value must be in [0.0, 1.0], got {value!r}")
    return number


def _coerce_outcome_source(source: Any) -> OutcomeSource:
    """Accept an :class:`OutcomeSource` member or one of its string values."""
    if isinstance(source, OutcomeSource):
        return source
    if isinstance(source, str):
        try:
            return OutcomeSource(source)
        except ValueError:
            pass
    raise InvalidOutcomeSourceError(
        f"outcome_source must be one of {[s.value for s in OutcomeSource]}," f" got {source!r}"
    )


def _coerce_metadata(metadata: Any) -> Optional[str]:
    """Validate JSON metadata; return ``None`` for empty input."""
    if metadata is None or metadata == "":
        return None
    if isinstance(metadata, (dict, list)):
        try:
            return json.dumps(metadata)
        except (TypeError, ValueError) as exc:
            raise InvalidMetadataError(f"metadata is not JSON-serializable: {exc}") from None
    if isinstance(metadata, str):
        try:
            json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise InvalidMetadataError(
                f"metadata must be valid JSON, got {metadata!r}: {exc}"
            ) from None
        return metadata
    raise InvalidMetadataError(
        f"metadata must be a JSON string or dict, got {type(metadata).__name__}"
    )


# --------------------------------------------------------------------------- #
# Domain records (offline/join pipeline)
# --------------------------------------------------------------------------- #


@dataclass
class OutcomeRecord:
    """A single outcome observation for a past decision."""

    decision_id: str
    outcome_timestamp_ns: int = 0
    outcome_source: OutcomeSource = OutcomeSource.HUMAN
    outcome_value: float = 1.0
    metadata: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.outcome_timestamp_ns == 0:
            self.outcome_timestamp_ns = now_ns()

    def to_dict(self) -> dict[str, Any]:
        """Serializable snapshot of the outcome as a plain dict."""
        return {
            "decision_id": self.decision_id,
            "outcome_timestamp_ns": self.outcome_timestamp_ns,
            "outcome_source": self.outcome_source.value,
            "outcome_value": self.outcome_value,
            "metadata": self.metadata or {},
        }


@dataclass
class JoinedRecord:
    """A decision linked to its eventual outcome."""

    decision_id: str
    context_hash: bytes
    decision_type: str
    model_confidence: float
    non_conformity: float
    action_taken: str
    outcome_source: OutcomeSource
    outcome_value: float
    decision_timestamp_ns: int
    outcome_timestamp_ns: int

    @property
    def latency_delta_ns(self) -> int:
        """Elapsed nanoseconds between the decision and its outcome."""
        return self.outcome_timestamp_ns - self.decision_timestamp_ns

    @property
    def loss(self) -> float:
        """0.0 = correct, 1.0 = incorrect (binary outcome aggregation)."""
        return 0.0 if self.outcome_value >= 0.5 else 1.0


class InMemoryOutcomeCollector:
    """Offline collector for demos and tests: accumulates records in memory.

    Deprecated in favor of the SQLite-backed :class:`OutcomeCollector`, but
    kept so the calibration example pipeline (collect -> join -> calibrate)
    runs without a database. ``record`` validates its inputs with the same
    rules as the durable collector.
    """

    def __init__(self, paths: List[Path] | None = None) -> None:
        """Create a collector; ``paths`` (deprecated) is kept for compatibility."""
        self._records: List[OutcomeRecord] = []
        self._paths = paths or []

    def record(
        self,
        decision_id: str,
        outcome_value: float,
        *,
        outcome_source: OutcomeSource = OutcomeSource.HUMAN,
        metadata: Optional[dict[str, Any]] = None,
    ) -> OutcomeRecord:
        """Validate and append one outcome."""
        value = _coerce_outcome_value(outcome_value)
        source = _coerce_outcome_source(outcome_source)
        outcome = OutcomeRecord(
            decision_id=decision_id,
            outcome_source=source,
            outcome_value=value,
            metadata=metadata,
        )
        self._records.append(outcome)
        return outcome

    def iter_records(self) -> Iterable[OutcomeRecord]:
        """Iterate over all collected outcomes."""
        return iter(self._records)

    def export(self, path: str | Path) -> Path:
        """Write every outcome as newline-delimited JSON to ``path``."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in self._records:
                handle.write(json.dumps(record.to_dict()))
                handle.write("\n")
        return path

    def import_file(self, path: str | Path) -> int:
        """Load newline-delimited JSON outcomes from ``path``; returns the count."""
        imported = 0
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                data = json.loads(line)
                self._records.append(
                    OutcomeRecord(
                        decision_id=data["decision_id"],
                        outcome_timestamp_ns=int(data["outcome_timestamp_ns"]),
                        outcome_source=_coerce_outcome_source(data["outcome_source"]),
                        outcome_value=float(data["outcome_value"]),
                        metadata=data.get("metadata"),
                    )
                )
                imported += 1
        return imported


class DecisionOutcomeJoiner:
    """Join decision records to outcomes by ``decision_id``."""

    def __init__(self, outcomes: Iterable[OutcomeRecord] | None = None) -> None:
        """Pre-index ``outcomes`` by ``decision_id`` for later joins."""
        self._by_id: Dict[str, OutcomeRecord] = {}
        for outcome in outcomes or []:
            self._by_id[outcome.decision_id] = outcome

    def add_outcome(self, outcome: OutcomeRecord) -> None:
        """Index (or replace) the outcome for its ``decision_id``."""
        self._by_id[outcome.decision_id] = outcome

    def join(self, decisions: Iterable[DecisionRecord]) -> List[JoinedRecord]:
        """Pair each decision with its outcome; decisions without one are skipped."""
        joined: List[JoinedRecord] = []
        for decision in decisions:
            outcome = self._by_id.get(decision.decision_id)
            if outcome is None:
                continue
            joined.append(
                JoinedRecord(
                    decision_id=decision.decision_id,
                    context_hash=decision.context_hash,
                    decision_type=decision.decision_type,
                    model_confidence=decision.model_confidence,
                    non_conformity=decision.non_conformity,
                    action_taken=decision.action_taken,
                    outcome_source=outcome.outcome_source,
                    outcome_value=outcome.outcome_value,
                    decision_timestamp_ns=decision.timestamp_ns,
                    outcome_timestamp_ns=outcome.outcome_timestamp_ns,
                )
            )
        return joined


# --------------------------------------------------------------------------- #
# Durable collector
# --------------------------------------------------------------------------- #


class OutcomeCollector:
    """Log and query outcome observations against SQLite.

    The collector is the author of the ``outcomes`` table: it validates every
    record, stamps it with a UUIDv7 ``outcome_id`` and a nanosecond
    timestamp, and inserts through ``Database.batch_insert``. A batch is
    atomic -- either every record lands or none does.

    Example:
        >>> from decision_ledger import Database, DecisionRecord
        >>> db = Database("ledger.db")
        >>> collector = OutcomeCollector(db)
        >>> outcome_id = collector.log_outcome(
        ...     decision_id="018d...", outcome_value=1.0,
        ...     outcome_source=OutcomeSource.TASK_METRIC,
        ... )
    """

    def __init__(self, database: Database) -> None:
        """Bind the collector to a durable :class:`Database` store."""
        self.database = database
        self._outcomes_logged = 0
        self._batches_logged = 0
        self._last_logged_at = 0.0

    # -- lifecycle metrics -------------------------------------------------- #

    def get_metrics(self) -> Dict[str, Any]:
        """Counters for monitoring the collector's write path."""
        return {
            "outcomes_logged": self._outcomes_logged,
            "batches_logged": self._batches_logged,
            "last_logged_at": self._last_logged_at,
        }

    # -- single outcome ----------------------------------------------------- #

    def log_outcome(
        self,
        decision_id: str,
        outcome_value: float,
        outcome_source: OutcomeSource,
        metadata: str | Mapping[str, Any] | None = None,
    ) -> str:
        """Validate and log one outcome for an existing decision.

        Args:
            decision_id: The logged decision's ``decision_id`` (must exist in
                the ``decisions`` table).
            outcome_value: Float (or numeric string) in [0.0, 1.0]; 0.0 =
                fail, 1.0 = success.
            outcome_source: An :class:`OutcomeSource` member or its string
                value (e.g. ``"human"``).
            metadata: Optional JSON string (or dict) with source-specific data.

        Returns:
            The generated ``outcome_id`` (UUIDv7, 36 chars).

        Raises:
            DecisionNotFoundError: If ``decision_id`` is not in the ledger.
            InvalidOutcomeValueError: If ``outcome_value`` is out of range.
            InvalidOutcomeSourceError: If the source is unknown.
            InvalidMetadataError: If ``metadata`` is not valid JSON.
            DatabaseError: On a persistent store failure.
        """
        self._ensure_decision_exists(decision_id)
        row = self._build_row(
            decision_id=decision_id,
            outcome_value=outcome_value,
            outcome_source=outcome_source,
            metadata=metadata,
        )
        self._insert_rows([row])
        outcome_id = row["outcome_id"]
        logger.info(
            "[Logged outcome %s for decision %s: %s]",
            outcome_id,
            decision_id,
            row["outcome_value"],
        )
        return outcome_id

    def get_outcome(self, outcome_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single outcome row by ``outcome_id``; ``None`` if absent.

        The returned dict has the schema's TEXT ``outcome_source``; ``metadata``
        is the stored JSON string or ``None``.
        """
        rows = self.database.execute_query(
            "SELECT * FROM outcomes WHERE outcome_id = ?", (outcome_id,)
        )
        if not rows:
            logger.warning("outcome %s not found", outcome_id)
            return None
        return dict(rows[0])

    def get_outcomes_for_decision(self, decision_id: str) -> List[Dict[str, Any]]:
        """Return all outcomes attached to a decision, oldest first."""
        return self.database.get_outcomes(decision_id=decision_id)

    # -- batch outcomes ----------------------------------------------------- #

    def log_outcomes_batch(self, records: List[Dict[str, Any]]) -> List[str]:
        """Validate and log many outcomes in one ``batch_insert`` transaction.

        ``records`` is a list of dicts with keys ``decision_id`` (required),
        ``outcome_value`` (required), ``source`` (or ``outcome_source``), and
        optional ``metadata``.

        Validation is all-or-nothing: if any record is invalid the batch is
        rejected before any insert happens, and the first offending record's
        index is included in the error.

        Args:
            records: Outcome records as dicts, at least one.

        Returns:
            List of generated ``outcome_id`` strings, in input order.

        Raises:
            DecisionNotFoundError: If a ``decision_id`` is missing or unknown.
            InvalidOutcomeValueError: On an out-of-range value.
            InvalidOutcomeSourceError: On an unknown source.
            InvalidMetadataError: On non-JSON metadata.
            DatabaseError: On a persistent store failure.
        """
        if not records:
            return []
        rows: List[_OutcomeRow] = []
        for index, record in enumerate(records):
            decision_id = record.get("decision_id")
            if not decision_id:
                raise DecisionNotFoundError(f"record {index} is missing decision_id")
            try:
                rows.append(
                    self._build_row(
                        decision_id=decision_id,
                        outcome_value=record.get("outcome_value"),
                        outcome_source=record.get("source", record.get("outcome_source")),
                        metadata=record.get("metadata", ""),
                    )
                )
            except (
                InvalidOutcomeValueError,
                InvalidOutcomeSourceError,
                InvalidMetadataError,
            ) as exc:
                raise type(exc)(f"record {index}: {exc}") from exc

        missing = self._missing_decision_ids([row["decision_id"] for row in rows])
        if missing:
            shown = ", ".join(repr(item) for item in missing[:5])
            raise DecisionNotFoundError(f"{len(missing)} decision(s) not found: {shown}")

        self._insert_rows(rows)
        outcome_ids = [row["outcome_id"] for row in rows]
        logger.info(
            "[Batch logged %d outcomes for %d decisions in one transaction]",
            len(outcome_ids),
            len(set(row["decision_id"] for row in rows)),
        )
        return outcome_ids

    # -- internals ---------------------------------------------------------- #

    def _ensure_decision_exists(self, decision_id: str) -> None:
        """Raise ``DecisionNotFoundError`` unless the decision is in the ledger."""
        rows = self.database.execute_query(
            "SELECT 1 FROM decisions WHERE decision_id = ?", (decision_id,)
        )
        if not rows:
            raise DecisionNotFoundError(f"no decision found for decision_id {decision_id!r}")

    def _missing_decision_ids(self, decision_ids: List[str]) -> List[str]:
        """Return the subset of ``decision_ids`` not present in ``decisions``."""
        missing: List[str] = []
        for start in range(0, len(decision_ids), _MISSING_DECISION_CHUNK):
            chunk = decision_ids[start : start + _MISSING_DECISION_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT decision_id FROM decisions" f" WHERE decision_id IN ({placeholders})"
            found = {row["decision_id"] for row in self.database.execute_query(query, tuple(chunk))}
            missing.extend(item for item in chunk if item not in found)
        return missing

    def _build_row(
        self,
        decision_id: str,
        outcome_value: Any,
        outcome_source: Any,
        metadata: Any,
    ) -> _OutcomeRow:
        """Build a validated, ID- and timestamp-stamped row for ``outcomes``."""
        return {
            "outcome_id": generate_uuidv7(),
            "decision_id": decision_id,
            "timestamp_ns": now_ns(),
            "outcome_value": _coerce_outcome_value(outcome_value),
            "outcome_source": _coerce_outcome_source(outcome_source).value,
            "metadata": _coerce_metadata(metadata),
        }

    def _insert_rows(self, rows: List[_OutcomeRow]) -> None:
        """Persist ``rows`` atomically and update lifecycle counters."""
        try:
            inserted = self.database.batch_insert("outcomes", rows)
        except DatabaseError as exc:
            logger.error("outcome database write failed: %s", exc)
            raise
        self._outcomes_logged += inserted
        self._batches_logged += 1
        self._last_logged_at = time.monotonic()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for outcome logging."""
    parser = argparse.ArgumentParser(
        prog="python -m decision_ledger.outcomes",
        description=(
            "Record a decision outcome into the ledger SQLite database."
            " Exit codes: 0 success, 2 validation error, 3 database error."
        ),
    )
    parser.add_argument(
        "--decision-id",
        required=True,
        help="UUIDv7 of a decision already logged through the gatekeeper.",
    )
    parser.add_argument(
        "--outcome-value",
        required=True,
        type=float,
        help="Float in [0.0, 1.0]; 0.0 = fail, 1.0 = success.",
    )
    parser.add_argument(
        "--source",
        required=True,
        choices=[source.value for source in OutcomeSource],
        help="Outcome source: %(choices)s",
    )
    parser.add_argument(
        "--metadata",
        default="",
        help='Optional JSON string, e.g. \'{"metric": "pass@1"}\'.',
    )
    parser.add_argument(
        "--db",
        default="ledger.db",
        help="Path to the ledger SQLite file (default: ledger.db).",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Record one outcome; returns a process exit code (0/2/3)."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        uuid.UUID(args.decision_id)
    except ValueError:
        parser.error(f"--decision-id must be a valid UUID, got {args.decision_id!r}")

    database = Database(args.db)
    try:
        outcome_id = OutcomeCollector(database).log_outcome(
            decision_id=args.decision_id,
            outcome_value=args.outcome_value,
            outcome_source=args.source,
            metadata=args.metadata,
        )
    except (
        DecisionNotFoundError,
        InvalidOutcomeValueError,
        InvalidOutcomeSourceError,
        InvalidMetadataError,
    ) as exc:
        logger.error("outcome validation failed: %s", exc)
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except DatabaseError as exc:
        logger.error("outcome database error: %s", exc)
        print(f"database error: {exc}", file=sys.stderr)
        return 3
    finally:
        database.close()
    print(f"logged outcome {outcome_id} for decision {args.decision_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
