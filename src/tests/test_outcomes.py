"""Tests for outcome collection (decision_ledger.outcomes)."""

import re

import pytest

from decision_ledger import Database, DatabaseError
from decision_ledger.outcomes import (
    DecisionNotFoundError,
    DecisionOutcomeJoiner,
    InMemoryOutcomeCollector,
    InvalidMetadataError,
    InvalidOutcomeSourceError,
    InvalidOutcomeValueError,
    OutcomeCollector,
    OutcomeSource,
    main,
)
from decision_ledger.telemetry import DecisionRecord
from decision_ledger.utils import generate_uuidv7

CONTEXT = bytes(range(16))


def decision_row(decision_id: str, **overrides):
    row = {
        "decision_id": decision_id,
        "timestamp_ns": 1_000,
        "context_hash": CONTEXT,
        "decision_type": "route",
        "model_confidence": 0.9,
        "non_conformity": 0.1,
        "action_taken": "DELEGATE",
        "latency_us": 5,
    }
    row.update(overrides)
    return row


def insert_decision(db: Database, decision_id: str, **overrides) -> None:
    db.batch_insert("decisions", [decision_row(decision_id, **overrides)])


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "test.db"))
    yield database
    database.close()


@pytest.fixture
def one_decision(db):
    insert_decision(db, "d-1")
    return "d-1"


# --------------------------------------------------------------------------- #
# Single-outcome logging
# --------------------------------------------------------------------------- #


def test_log_outcome_roundtrip(db, one_decision):
    collector = OutcomeCollector(db)
    outcome_id = collector.log_outcome(
        decision_id=one_decision,
        outcome_value=1.0,
        outcome_source=OutcomeSource.TASK_METRIC,
    )

    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
        outcome_id,
    )
    row = db.get_outcomes(decision_id=one_decision)[0]
    assert row["outcome_id"] == outcome_id
    assert row["outcome_value"] == 1.0
    assert row["outcome_source"] == "task_metric"
    assert row["metadata"] is None


def test_log_outcome_accepts_string_value_and_source(db, one_decision):
    collector = OutcomeCollector(db)
    collector.log_outcome(
        decision_id=one_decision,
        outcome_value="0.7",
        outcome_source="human",
    )
    row = db.get_outcomes(decision_id=one_decision)[0]
    assert row["outcome_value"] == 0.7
    assert row["outcome_source"] == "human"


def test_log_outcome_stores_metadata_as_json(db, one_decision):
    collector = OutcomeCollector(db)
    collector.log_outcome(
        decision_id=one_decision,
        outcome_value=0.5,
        outcome_source=OutcomeSource.USER_REPORT,
        metadata={"note": "flagged"},
    )
    row = db.get_outcomes(decision_id=one_decision)[0]
    assert row["metadata"] == '{"note": "flagged"}'


def test_log_outcome_missing_decision_raises(db):
    collector = OutcomeCollector(db)
    with pytest.raises(DecisionNotFoundError, match="d-unknown"):
        collector.log_outcome("d-unknown", 1.0, OutcomeSource.HUMAN)
    assert db.get_outcomes() == []


@pytest.mark.parametrize("bad", [1.5, -0.01, True, None, "abc", object()])
def test_log_outcome_rejects_bad_values(db, one_decision, bad):
    collector = OutcomeCollector(db)
    with pytest.raises(InvalidOutcomeValueError):
        collector.log_outcome(one_decision, bad, OutcomeSource.HUMAN)


@pytest.mark.parametrize("bad", [1, "nope", None])
def test_log_outcome_rejects_unknown_source(db, one_decision, bad):
    collector = OutcomeCollector(db)
    with pytest.raises(InvalidOutcomeSourceError):
        collector.log_outcome(one_decision, 1.0, bad)


@pytest.mark.parametrize("bad", ["{not json", {"x": object()}, 42])
def test_log_outcome_rejects_bad_metadata(db, one_decision, bad):
    collector = OutcomeCollector(db)
    with pytest.raises(InvalidMetadataError):
        collector.log_outcome(one_decision, 1.0, OutcomeSource.HUMAN, bad)


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #


def test_get_outcome_roundtrip(db, one_decision):
    collector = OutcomeCollector(db)
    outcome_id = collector.log_outcome(one_decision, 1.0, OutcomeSource.HUMAN)
    row = collector.get_outcome(outcome_id)
    assert row is not None
    assert row["decision_id"] == one_decision


def test_get_outcome_missing_returns_none(db):
    assert OutcomeCollector(db).get_outcome("no-such-outcome") is None


def test_get_outcomes_for_decision_oldest_first(db):
    insert_decision(db, "d-1")
    insert_decision(db, "d-2")
    collector = OutcomeCollector(db)
    first = collector.log_outcome("d-1", 0.3, OutcomeSource.HUMAN)
    second = collector.log_outcome("d-1", 0.9, OutcomeSource.TASK_METRIC)

    rows = collector.get_outcomes_for_decision("d-1")
    assert [row["outcome_id"] for row in rows] == [first, second]
    assert all(row["decision_id"] == "d-1" for row in rows)
    assert collector.get_outcomes_for_decision("d-2") == []


# --------------------------------------------------------------------------- #
# Batch logging
# --------------------------------------------------------------------------- #


def test_batch_logs_all_in_one_transaction(db):
    for decision_id in ("d-1", "d-2", "d-3"):
        insert_decision(db, decision_id)
    collector = OutcomeCollector(db)
    outcome_ids = collector.log_outcomes_batch(
        [
            {"decision_id": "d-1", "outcome_value": 1.0, "source": "human"},
            {
                "decision_id": "d-2",
                "outcome_value": 0.0,
                "source": OutcomeSource.TASK_METRIC,
            },
            {
                "decision_id": "d-3",
                "outcome_value": 0.5,
                "outcome_source": "user_report",
            },
        ]
    )

    assert len(outcome_ids) == 3
    assert len(set(outcome_ids)) == 3
    assert len(db.get_outcomes()) == 3
    assert collector.get_metrics()["batches_logged"] == 1  # single transaction


def test_batch_is_all_or_nothing_on_bad_record(db):
    insert_decision(db, "d-1")
    collector = OutcomeCollector(db)
    with pytest.raises(InvalidOutcomeSourceError, match=r"record 1"):
        collector.log_outcomes_batch(
            [
                {"decision_id": "d-1", "outcome_value": 1.0, "source": "human"},
                {"decision_id": "d-1", "outcome_value": 1.0, "source": "nope"},
            ]
        )
    assert db.get_outcomes() == []
    assert collector.get_metrics()["outcomes_logged"] == 0


def test_batch_reports_missing_decisions(db):
    insert_decision(db, "d-1")
    collector = OutcomeCollector(db)
    with pytest.raises(DecisionNotFoundError, match="1 decision"):
        collector.log_outcomes_batch(
            [
                {"decision_id": "d-1", "outcome_value": 1.0, "source": "human"},
                {"decision_id": "d-ghost", "outcome_value": 0.0, "source": "human"},
            ]
        )
    assert db.get_outcomes() == []


def test_batch_requires_decision_id(db):
    collector = OutcomeCollector(db)
    with pytest.raises(DecisionNotFoundError, match="record 0 is missing decision_id"):
        collector.log_outcomes_batch([{"outcome_value": 1.0, "source": "human"}])


def test_batch_empty_returns_empty(db):
    collector = OutcomeCollector(db)
    assert collector.log_outcomes_batch([]) == []


def test_batch_chunks_missing_decision_check(db):
    decision_ids = [f"d-{index}" for index in range(600)]
    insert_decision(db, decision_ids[0])
    collector = OutcomeCollector(db)
    with pytest.raises(DecisionNotFoundError, match="599 decision"):
        collector.log_outcomes_batch(
            [
                {"decision_id": decision_id, "outcome_value": 1.0, "source": "human"}
                for decision_id in decision_ids
            ]
        )
    assert db.get_outcomes() == []


def test_batch_with_many_valid_decisions(db):
    decision_ids = [f"d-{index}" for index in range(600)]
    db.batch_insert(
        "decisions", [decision_row(decision_id) for decision_id in decision_ids]
    )
    collector = OutcomeCollector(db)
    outcome_ids = collector.log_outcomes_batch(
        [
            {"decision_id": decision_id, "outcome_value": 1.0, "source": "task_metric"}
            for decision_id in decision_ids
        ]
    )
    assert len(outcome_ids) == 600
    assert len(db.get_outcomes()) == 600


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


def test_metrics_start_at_zero(db):
    collector = OutcomeCollector(db)
    metrics = collector.get_metrics()
    assert metrics["outcomes_logged"] == 0
    assert metrics["batches_logged"] == 0
    assert metrics["last_logged_at"] == 0.0


def test_metrics_accumulate(db, one_decision):
    collector = OutcomeCollector(db)
    collector.log_outcome(one_decision, 1.0, OutcomeSource.HUMAN)
    metrics = collector.get_metrics()
    assert metrics["outcomes_logged"] == 1
    assert metrics["batches_logged"] == 1
    assert metrics["last_logged_at"] > 0.0


# --------------------------------------------------------------------------- #
# In-memory collector (offline demos/tests)
# --------------------------------------------------------------------------- #


def test_in_memory_collector_validates_and_rejects():
    collector = InMemoryOutcomeCollector()
    with pytest.raises(InvalidOutcomeValueError):
        collector.record("d-1", 1.5)
    record = collector.record(
        "d-1", outcome_value=0.8, outcome_source=OutcomeSource.TASK_METRIC
    )
    assert record.outcome_source == OutcomeSource.TASK_METRIC
    assert len(list(collector.iter_records())) == 1


def test_in_memory_collector_export_import_roundtrip(tmp_path):
    collector = InMemoryOutcomeCollector()
    collector.record("d-1", outcome_value=1.0, outcome_source=OutcomeSource.HUMAN)
    collector.record("d-2", outcome_value=0.0, outcome_source=OutcomeSource.TASK_METRIC)
    path = collector.export(tmp_path / "outcomes.jsonl")

    loaded = InMemoryOutcomeCollector()
    assert loaded.import_file(path) == 2
    records = list(loaded.iter_records())
    assert [record.decision_id for record in records] == ["d-1", "d-2"]
    assert records[0].outcome_source == OutcomeSource.HUMAN
    assert records[1].outcome_value == 0.0


# --------------------------------------------------------------------------- #
# Joiner
# --------------------------------------------------------------------------- #


def _decision_record(decision_id: str, timestamp_ns: int = 1_000) -> DecisionRecord:
    return DecisionRecord(
        decision_id=decision_id,
        timestamp_ns=timestamp_ns,
        context_hash=CONTEXT,
        decision_type="route",
        model_confidence=0.9,
        non_conformity=0.1,
        action_taken="DELEGATE",
        latency_us=5,
    )


def test_joiner_produces_joined_records():
    collector = InMemoryOutcomeCollector()
    collector.record("d-1", outcome_value=1.0, outcome_source=OutcomeSource.TASK_METRIC)
    joined = DecisionOutcomeJoiner(collector.iter_records()).join(
        [_decision_record("d-1"), _decision_record("d-2")]
    )

    assert len(joined) == 1  # only decisions with an outcome
    record = joined[0]
    assert record.decision_id == "d-1"
    assert record.outcome_source == OutcomeSource.TASK_METRIC
    assert record.loss == 0.0
    assert record.latency_delta_ns >= 0


def test_joiner_loss_for_failed_outcome():
    collector = InMemoryOutcomeCollector()
    collector.record("d-1", outcome_value=0.4)
    joined = DecisionOutcomeJoiner(collector.iter_records()).join(
        [_decision_record("d-1")]
    )
    assert joined[0].loss == 1.0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _cli_args(db_path, **overrides):
    args = {
        "decision-id": generate_uuidv7(),
        "outcome-value": "1.0",
        "source": "task_metric",
        "db": str(db_path),
    }
    args.update(overrides)
    return [f"--{key}={value}" for key, value in args.items()]


def test_cli_logs_outcome(db, tmp_path, capsys):
    decision_id = generate_uuidv7()
    insert_decision(db, decision_id)
    db_path = tmp_path / "test.db"

    code = main(
        [
            "--decision-id",
            decision_id,
            "--outcome-value",
            "1.0",
            "--source",
            "task_metric",
            "--db",
            str(db_path),
        ]
    )
    assert code == 0

    out = capsys.readouterr().out
    match = re.search(r"logged outcome ([0-9a-f-]+)", out)
    assert match

    rows = db.get_outcomes(decision_id=decision_id)
    assert len(rows) == 1
    assert rows[0]["outcome_id"] == match.group(1)
    assert rows[0]["outcome_source"] == "task_metric"


def test_cli_rejects_non_guid_decision_id(db, tmp_path):
    with pytest.raises(SystemExit) as excinfo:
        main(_cli_args(tmp_path / "test.db", **{"decision-id": "not-a-uuid"}))
    assert excinfo.value.code == 2


def test_cli_rejects_unknown_source_choice(db, tmp_path):
    with pytest.raises(SystemExit) as excinfo:
        main(_cli_args(tmp_path / "test.db", **{"source": "nope"}))
    assert excinfo.value.code == 2


def test_cli_unknown_decision_returns_2(db, tmp_path, capsys):
    code = main(_cli_args(tmp_path / "test.db"))
    assert code == 2
    assert "error:" in capsys.readouterr().err
    assert db.get_outcomes() == []


def test_cli_out_of_range_value_returns_2(db, tmp_path):
    insert_decision(db, generate_uuidv7())
    args = _cli_args(tmp_path / "test.db", **{"outcome-value": "1.5"})
    assert main(args) == 2


def test_cli_invalid_metadata_returns_2(db, tmp_path):
    insert_decision(db, generate_uuidv7())
    args = _cli_args(tmp_path / "test.db", **{"metadata": "{not json"})
    assert main(args) == 2


def test_cli_database_error_returns_3(db, tmp_path, monkeypatch):
    insert_decision(db, generate_uuidv7())

    def boom(*args, **kwargs):
        raise DatabaseError("db exploded")

    monkeypatch.setattr(OutcomeCollector, "log_outcome", boom)
    code = main(_cli_args(tmp_path / "test.db"))
    assert code == 3
