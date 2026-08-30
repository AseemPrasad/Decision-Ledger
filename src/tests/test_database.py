"""Tests for the SQLite persistence layer (decision_ledger.database)."""

import sqlite3
import threading

import pytest

from decision_ledger import database as dbmod
from decision_ledger.database import (
    Database,
    DatabaseError,
    DatabaseIntegrityError,
    init_database,
    outcome_source_from_text,
    outcome_source_to_text,
)

CONTEXT_A = bytes(range(16))
CONTEXT_B = bytes(reversed(range(16)))


def decision_row(decision_id: str, **overrides):
    row = {
        "decision_id": decision_id,
        "timestamp_ns": int(decision_id.split("-")[1]) * 1_000,
        "context_hash": CONTEXT_A,
        "decision_type": "route",
        "model_confidence": 0.9,
        "non_conformity": 0.1,
        "action_taken": "DELEGATE",
        "latency_us": 5,
    }
    row.update(overrides)
    return row


def outcome_row(outcome_id: str, decision_id: str, **overrides):
    row = {
        "outcome_id": outcome_id,
        "decision_id": decision_id,
        "timestamp_ns": 999_000,
        "outcome_value": 1.0,
        "outcome_source": "task_metric",
        "metadata": '{"key": "value"}',
    }
    row.update(overrides)
    return row


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    yield database
    database.close()


@pytest.fixture
def seed_decisions(db):
    db.batch_insert(
        "decisions",
        [
            decision_row("d-1", timestamp_ns=1000, context_hash=CONTEXT_A),
            decision_row("d-2", timestamp_ns=2000, context_hash=CONTEXT_A),
            decision_row("d-3", timestamp_ns=3000, context_hash=CONTEXT_B),
        ],
    )
    return db


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


def test_init_creates_all_tables_and_indexes(db):
    tables = {
        row["name"]
        for row in db.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'table'" " AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert {"decisions", "outcomes", "joined_records", "policies"} <= tables

    indexes = {
        row["name"]
        for row in db.execute_query(
            "SELECT name FROM sqlite_master WHERE type = 'index'" " AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert {
        "idx_decisions_timestamp_ns",
        "idx_decisions_context_hash",
        "idx_decisions_ctx_action",
        "idx_outcomes_decision_id",
        "idx_outcomes_source",
        "idx_joined_context_hash",
        "idx_joined_decision_timestamp",
        "idx_policies_is_active",
        "idx_policies_generated_at",
    } <= indexes
    assert len(indexes) == 9  # nothing extra lurks in the schema


def test_init_schema_is_idempotent(db):
    db.init_schema()
    db.init_schema()
    db.verify_schema()  # must not raise

    db.execute_write(
        "INSERT OR REPLACE INTO decisions (decision_id, timestamp_ns, context_hash,"
        " decision_type, model_confidence, non_conformity, action_taken)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("d-1", 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
    )
    assert len(db.get_decisions()) == 1


def test_init_database_function(tmp_path):
    path = str(tmp_path / "nested" / "ledger.db")
    init_database(path)

    with Database(path) as database:
        database.verify_schema()
        assert database.execute_query("SELECT COUNT(*) FROM decisions")[0][0] == 0


def test_verify_schema_detects_missing_table(tmp_path):
    path = str(tmp_path / "test.db")
    database = Database(path)
    try:
        database.verify_schema()  # database + schema exist

        with sqlite3.connect(path) as raw:
            raw.execute("DROP TABLE decisions")

        with pytest.raises(DatabaseError, match="decisions"):
            database.verify_schema()
    finally:
        database.close()


# --------------------------------------------------------------------------- #
# Writes
# --------------------------------------------------------------------------- #


def test_execute_write_returns_rowcount(db):
    assert (
        db.execute_write(
            "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
            " decision_type, model_confidence, non_conformity, action_taken)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("d-1", 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
        )
        == 1
    )
    assert (
        db.execute_write("UPDATE decisions SET latency_us = 9 WHERE decision_id = ?", ("d-1",)) == 1
    )
    assert db.execute_write("DELETE FROM decisions WHERE decision_id = ?", ("d-1",)) == 1
    assert db.execute_write("DELETE FROM decisions WHERE decision_id = ?", ("d-missing",)) == 0


def test_batch_insert_counts_and_persists(seed_decisions):
    assert len(seed_decisions.get_decisions()) == 3


def test_batch_insert_is_atomic_on_duplicate(db):
    records = [decision_row("d-1"), decision_row("d-1")]
    with pytest.raises(DatabaseIntegrityError):
        db.batch_insert("decisions", records)
    assert len(db.get_decisions()) == 0


def test_batch_insert_rejects_unknown_table(db):
    with pytest.raises(DatabaseError, match="unknown table"):
        db.batch_insert("admin_users", [decision_row("d-1")])


def test_batch_insert_rejects_ragged_records(db):
    records = [decision_row("d-1"), decision_row("d-2", extra=1)]
    with pytest.raises(DatabaseError, match="identical columns"):
        db.batch_insert("decisions", records)


def test_batch_insert_empty_is_noop(db):
    assert db.batch_insert("decisions", []) == 0


def test_batch_insert_policies(db):
    count = db.batch_insert(
        "policies",
        [
            {
                "policy_id": "policy_v001",
                "version_string": "20260826-120000",
                "generated_at": 123,
                "policy_yaml": "q_hat: 0.8",
                "is_active": 1,
            }
        ],
    )
    assert count == 1
    rows = db.execute_query("SELECT policy_id, is_active FROM policies WHERE is_active = 1")
    assert rows[0]["policy_id"] == "policy_v001"


# --------------------------------------------------------------------------- #
# Constraints
# --------------------------------------------------------------------------- #


def test_foreign_keys_enforced(seed_decisions):
    with pytest.raises(DatabaseIntegrityError):
        seed_decisions.batch_insert("outcomes", [outcome_row("o-bogus", "no-such-decision")])


def test_valid_outcome_insert_roundtrips(seed_decisions):
    seed_decisions.batch_insert("outcomes", [outcome_row("o-1", "d-1")])
    outcomes = seed_decisions.get_outcomes("d-1")
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_source"] == "task_metric"
    assert outcomes[0]["metadata"] == '{"key": "value"}'


def test_duplicate_primary_key_raises(seed_decisions):
    with pytest.raises(DatabaseIntegrityError):
        seed_decisions.execute_write(
            "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
            " decision_type, model_confidence, non_conformity, action_taken)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("d-1", 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
        )


# --------------------------------------------------------------------------- #
# Domain queries
# --------------------------------------------------------------------------- #


def test_get_decisions_shape(seed_decisions):
    rows = seed_decisions.get_decisions()
    assert len(rows) == 3
    assert set(rows[0].keys()) == {
        "decision_id",
        "timestamp_ns",
        "context_hash",
        "decision_type",
        "model_confidence",
        "non_conformity",
        "action_taken",
        "latency_us",
    }
    assert rows[0]["context_hash"] == CONTEXT_A
    assert [row["timestamp_ns"] for row in rows] == [1000, 2000, 3000]


def test_get_decisions_context_hash_filter(seed_decisions):
    rows = seed_decisions.get_decisions(context_hash=CONTEXT_B)
    assert [row["decision_id"] for row in rows] == ["d-3"]


def test_get_decisions_time_range_filter(seed_decisions):
    rows = seed_decisions.get_decisions(start_time=1500, end_time=2500)
    assert [row["decision_id"] for row in rows] == ["d-2"]


def test_get_outcomes_no_filter_returns_all(seed_decisions):
    seed_decisions.batch_insert(
        "outcomes",
        [outcome_row("o-1", "d-1"), outcome_row("o-2", "d-2")],
    )
    assert len(seed_decisions.get_outcomes()) == 2
    assert len(seed_decisions.get_outcomes(decision_id="d-1")) == 1


# --------------------------------------------------------------------------- #
# Concurrency, retry, lifecycle
# --------------------------------------------------------------------------- #


def test_thread_local_connections_share_file(tmp_path):
    path = str(tmp_path / "shared.db")
    database = Database(path)
    errors = []

    def writer(decision_id):
        try:
            database.execute_write(
                "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
                " decision_type, model_confidence, non_conformity, action_taken)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (decision_id, 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
            )
        except Exception as exc:  # pytest reports thread failures via list
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(f"d-{i}",)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(database.get_decisions()) == 2


class _DelegatingConnection:
    """Proxy a live sqlite3 connection, failing ``execute`` while locked.

    sqlite3.Connection is immutable in Python 3.14 (its ``execute`` cannot be
    monkeypatched), so the locked-write tests route calls through this proxy.
    """

    def __init__(self, conn):
        self._conn = conn
        self._locked_remaining = 0

    def execute(self, sql, params=()):
        if self._locked_remaining > 0:
            self._locked_remaining -= 1
            raise sqlite3.OperationalError("database is locked")
        return self._conn.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_locked_database_retries_then_succeeds(db):
    conn = db._get_conn()
    proxy = _DelegatingConnection(conn)
    proxy._locked_remaining = 2
    db._local.conn = proxy

    rowcount = db.execute_write(
        "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
        " decision_type, model_confidence, non_conformity, action_taken)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("d-retry", 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
    )
    assert rowcount == 1
    assert len(db.get_decisions()) == 1


def test_locked_database_raises_after_exhaustion(db, monkeypatch):
    conn = db._get_conn()
    proxy = _DelegatingConnection(conn)
    proxy._locked_remaining = 10**6
    db._local.conn = proxy
    monkeypatch.setattr(dbmod.time, "sleep", lambda _seconds: None)

    with pytest.raises(DatabaseError, match="is locked after 3 attempts"):
        db.execute_write(
            "INSERT INTO decisions (decision_id, timestamp_ns, context_hash,"
            " decision_type, model_confidence, non_conformity, action_taken)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("d-x", 1, CONTEXT_A, "route", 0.9, 0.1, "DELEGATE"),
        )


def test_non_locked_operational_error_is_not_retried(db):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise sqlite3.OperationalError("no such table: nope")

    with pytest.raises(sqlite3.OperationalError):
        db._run_with_lock_retry("write", fn)
    assert calls["n"] == 1


def test_context_manager_closes(db):
    with Database(str(db._db_path)) as closed:
        closed.execute_query("SELECT 1")
    with pytest.raises(DatabaseError, match="closed"):
        closed.execute_query("SELECT 1")


# --------------------------------------------------------------------------- #
# Outcome source mapping
# --------------------------------------------------------------------------- #


def test_outcome_source_mapping_roundtrip():
    for value in (0, 1, 2, 3):
        text = outcome_source_to_text(value)
        assert outcome_source_from_text(text) == value
    assert outcome_source_to_text(1) == "task_metric"
    assert outcome_source_from_text("human") == 0


def test_outcome_source_mapping_rejects_unknown():
    with pytest.raises(ValueError):
        outcome_source_to_text(99)
    with pytest.raises(ValueError):
        outcome_source_from_text("nope")
