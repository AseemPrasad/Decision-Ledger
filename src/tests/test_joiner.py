"""Tests for the SQLite decision-outcome joiner (decision_ledger.Joiner)."""

import pytest

from decision_ledger import Database, DatabaseError, Joiner

CONTEXT_A = bytes(range(16))
CONTEXT_B = bytes(reversed(range(16)))


def decision_row(decision_id: str, **overrides) -> dict:
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


def outcome_row(outcome_id: str, decision_id: str, **overrides) -> dict:
    row = {
        "outcome_id": outcome_id,
        "decision_id": decision_id,
        "timestamp_ns": 999_000,
        "outcome_value": 1.0,
        "outcome_source": "task_metric",
        "metadata": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    yield database
    database.close()


@pytest.fixture
def seeded(db):
    db.batch_insert(
        "decisions",
        [
            decision_row("d-1"),
            decision_row("d-2"),
            decision_row("d-3", context_hash=CONTEXT_B),
        ],
    )
    db.batch_insert(
        "outcomes",
        [outcome_row("o-1", "d-1"), outcome_row("o-2", "d-2")],
    )
    return db


# --------------------------------------------------------------------------- #
# Join creation
# --------------------------------------------------------------------------- #


def test_join_left_joins_matched_and_unmatched(seeded):
    created = Joiner(seeded).join_decisions_and_outcomes()

    assert created == 3  # LEFT JOIN: every decision gets a row, matched or not
    rows = {row["decision_id"]: row for row in seeded.get_joined_records()}

    matched = rows["d-1"]
    assert matched["outcome_value"] == 1.0
    assert matched["outcome_timestamp_ns"] == 999_000
    assert matched["latency_delta_ns"] == 999_000 - 1_000
    assert matched["decision_id"] == matched["joined_id"]

    unmatched = rows["d-3"]
    assert unmatched["outcome_value"] is None
    assert unmatched["latency_delta_ns"] is None


def test_join_is_idempotent(seeded):
    joiner = Joiner(seeded)
    assert joiner.join_decisions_and_outcomes() == 3
    assert joiner.join_decisions_and_outcomes() == 0  # nothing new

    assert len(seeded.get_joined_records()) == 3


def test_join_is_idempotent_when_outcomes_grow(seeded):
    """A decision already joined is not duplicated by a later run."""
    joiner = Joiner(seeded)
    assert joiner.join_decisions_and_outcomes() == 3
    db = seeded
    db.batch_insert("outcomes", [outcome_row("o-3", "d-2", outcome_value=0.0)])
    assert joiner.join_decisions_and_outcomes() == 0

    rows = db.get_joined_records()
    assert len(rows) == 3
    by_id = {row["decision_id"]: row for row in rows}
    assert by_id["d-2"]["outcome_value"] == 1.0  # first outcome kept


def test_join_writes_one_row_per_decision_with_multiple_outcomes(seeded):
    db = seeded
    db.batch_insert(
        "outcomes",
        [
            outcome_row("o-3", "d-1", outcome_value=0.0, timestamp_ns=100_000),
            outcome_row("o-4", "d-1", outcome_value=0.5, timestamp_ns=200_000),
        ],
    )
    joiner = Joiner(db)
    assert joiner.join_decisions_and_outcomes() == 3

    rows = db.get_joined_records()
    assert len(rows) == 3  # one row per decision, still no duplicates
    by_id = {row["decision_id"]: row for row in rows}
    assert by_id["d-1"]["outcome_value"] == 1.0  # first-inserted outcome wins

    matched = db.get_joined_records(include_unmatched=False)
    assert {row["decision_id"] for row in matched} == {"d-1", "d-2"}


def test_join_nothing_to_join(db):
    db.batch_insert("decisions", [decision_row("d-1")])
    joiner = Joiner(db)
    assert joiner.join_decisions_and_outcomes() == 1
    assert joiner.join_decisions_and_outcomes() == 0


def test_join_empty_database(db):
    assert Joiner(db).join_decisions_and_outcomes() == 0


def test_join_respects_time_range(seeded):
    joiner = Joiner(seeded)
    # d-1 at ts 1000, d-2 at 2000, d-3 at 3000; strict bounds keep d-2 only.
    assert joiner.join_decisions_and_outcomes(start_time=1_000, end_time=3_000) == 1
    assert {row["decision_id"] for row in seeded.get_joined_records()} == {"d-2"}


def test_join_is_available_via_database_property(seeded):
    created = seeded.joiner.join_decisions_and_outcomes()
    assert created == 3
    assert len(seeded.get_joined_records()) == 3


def test_join_reports_store_failure(seeded):
    seeded.close()
    with pytest.raises(DatabaseError):
        Joiner(seeded).join_decisions_and_outcomes()


# --------------------------------------------------------------------------- #
# Read back
# --------------------------------------------------------------------------- #


def test_get_joined_records_filters_by_context_hash(seeded):
    seeded.joiner.join_decisions_and_outcomes()

    rows = seeded.get_joined_records(context_hash=CONTEXT_A)
    assert {row["decision_id"] for row in rows} == {"d-1", "d-2"}
    assert {row["context_hash"] for row in rows} == {CONTEXT_A}


def test_get_joined_records_excludes_unmatched(seeded):
    seeded.joiner.join_decisions_and_outcomes()

    matched = seeded.get_joined_records(include_unmatched=False)
    assert {row["decision_id"] for row in matched} == {"d-1", "d-2"}
    assert all(row["outcome_value"] is not None for row in matched)


def test_get_joined_records_update_after_join(seeded):
    # Before a join run the table is empty but the stats are still accurate.
    assert seeded.get_join_statistics()["joined_count"] == 2
    assert seeded.get_joined_records() == []
    seeded.joiner.join_decisions_and_outcomes()
    assert len(seeded.get_joined_records()) == 3


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #


def test_get_join_statistics(seeded):
    stats = seeded.get_join_statistics()

    assert stats["total_decisions"] == 3
    assert stats["total_outcomes"] == 2
    assert stats["joined_count"] == 2  # distinct decisions with an outcome
    assert stats["match_rate"] == pytest.approx(2 / 3)


def test_join_statistics_count_distinct_decisions(db):
    db.batch_insert("decisions", [decision_row("d-1"), decision_row("d-2")])
    db.batch_insert(
        "outcomes",
        [
            outcome_row("o-1", "d-1"),
            outcome_row("o-2", "d-1"),  # second outcome for the same decision
        ],
    )
    stats = db.get_join_statistics()
    assert stats["total_outcomes"] == 2
    assert stats["joined_count"] == 1
    assert stats["match_rate"] == pytest.approx(0.5)


def test_get_join_statistics_empty(db):
    stats = db.get_join_statistics()
    assert stats["total_decisions"] == 0
    assert stats["total_outcomes"] == 0
    assert stats["joined_count"] == 0
    assert stats["match_rate"] == 0.0


def test_joiner_read_side_delegates(seeded):
    seeded.batch_insert("outcomes", [outcome_row("o-x", "d-3")])
    seeded.joiner.join_decisions_and_outcomes()

    joined = Joiner(seeded).get_joined_records(include_unmatched=False)
    assert len(joined) == 3
    assert seeded.joiner.get_join_statistics()["match_rate"] == 1.0
