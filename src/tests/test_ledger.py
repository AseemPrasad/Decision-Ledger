"""End-to-end tests for the :class:`DecisionLedger` orchestrator."""

import json
from pathlib import Path

import pytest

from decision_ledger import DecisionLedger, make_context_hash
from decision_ledger.outcomes import (
    DecisionNotFoundError,
    InvalidOutcomeValueError,
)


def _decision_ids(ledger) -> list:
    """Return the durable ``decision_id`` values in the store (oldest first)."""
    rows = ledger.database.execute_query("SELECT decision_id FROM decisions")
    return [row["decision_id"] for row in rows]


# --------------------------------------------------------------------------- #
# Construction
# --------------------------------------------------------------------------- #


def test_init_auto_starts_consumer(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=True)
    try:
        assert ledger.consumer.is_alive()
        assert ledger.gatekeeper.policy == {}
        assert ledger.ring_buffer is ledger.gatekeeper.telemetry
        assert ledger.outcome_collector is not None
    finally:
        ledger.shutdown()


def test_init_consumer_stopped_when_disabled(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        assert not ledger.consumer.is_alive()
    finally:
        ledger.shutdown()


def test_init_rejects_bad_exploration_rate(tmp_path):
    with pytest.raises(ValueError):
        DecisionLedger(tmp_path / "ledger.db", exploration_rate=1.5, auto_start_consumer=False)


# --------------------------------------------------------------------------- #
# evaluate
# --------------------------------------------------------------------------- #


def test_evaluate_fails_closed_and_records(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        assert ledger.evaluate(ctx, confidence=0.85, decision_type="route") == "ESCALATE"
        assert ledger.evaluate(ctx, model_confidence=0.2) == "ESCALATE"
        assert ledger.consumer.drain_now() == 2
        assert len(_decision_ids(ledger)) == 2
        rows = ledger.database.execute_query("SELECT action_taken FROM decisions")
        assert [row["action_taken"] for row in rows] == ["ESCALATE", "ESCALATE"]
    finally:
        ledger.shutdown()


def test_evaluate_unknown_decision_type_warns_and_escalates(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        assert ledger.evaluate(ctx, confidence=0.9, decision_type="bogus") == "ESCALATE"
    finally:
        ledger.shutdown()


def test_evaluate_requires_exactly_one_confidence(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        with pytest.raises(ValueError):
            ledger.evaluate(ctx)
        with pytest.raises(ValueError):
            ledger.evaluate(ctx, model_confidence=0.5, confidence=0.5)
    finally:
        ledger.shutdown()


def test_evaluate_records_but_does_not_flush_until_drain(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        ledger.evaluate(ctx, confidence=0.8)
        assert ledger.ring_buffer.size() == 1
        assert _decision_ids(ledger) == []
        assert ledger.consumer.drain_now() == 1
        assert ledger.ring_buffer.size() == 0
        assert len(_decision_ids(ledger)) == 1
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# log_outcome
# --------------------------------------------------------------------------- #


def test_log_outcome_roundtrip_and_join(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        ledger.evaluate(ctx, confidence=0.9)
        ledger.evaluate(ctx, confidence=0.1)
        ledger.consumer.drain_now()
        ids = _decision_ids(ledger)

        first = ledger.log_outcome(ids[0], 1.0, "human", metadata='{"correct": true}')
        second = ledger.log_outcome(ids[1], 0.0, "task_metric")
        assert len(first) == 36 and len(second) == 36 and first != second

        ledger.database.joiner.join_decisions_and_outcomes()
        stats = ledger.stats()
        assert stats["total_decisions"] == 2
        assert stats["total_outcomes"] == 2
        assert stats["join_rate"] == 1.0
        assert stats["decisions_by_action"] == {"ESCALATE": 2}
    finally:
        ledger.shutdown()


def test_log_outcome_missing_decision_raises(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        with pytest.raises(DecisionNotFoundError):
            ledger.log_outcome("does-not-exist", 1.0)
    finally:
        ledger.shutdown()


def test_log_outcome_invalid_value_raises(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ledger.evaluate(make_context_hash("a", "b"), confidence=0.9)
        ledger.consumer.drain_now()
        decision_id = _decision_ids(ledger)[0]
        with pytest.raises(InvalidOutcomeValueError):
            ledger.log_outcome(decision_id, 1.7)
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# calibrate
# --------------------------------------------------------------------------- #


def test_calibrate_end_to_end_activates_context(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        ctx = make_context_hash("qwen-7b", "routing")
        for _ in range(120):
            ledger.evaluate(ctx, confidence=1.0, decision_type="route")
        ledger.consumer.drain_now()
        for decision_id in _decision_ids(ledger):
            ledger.log_outcome(decision_id, 1.0, "human")

        policy_path = ledger.calibrate()
        assert Path(policy_path).exists()

        calibrated = ledger.gatekeeper.policy[ctx]
        assert calibrated.q_hat is not None
        assert calibrated.is_active

        actions = {ledger.evaluate(ctx, confidence=1.0) for _ in range(5)}
        assert "DELEGATE" in actions

        stats = ledger.stats()
        assert stats["total_decisions"] == 120
        assert stats["total_outcomes"] == 120
        assert stats["join_rate"] == 1.0
        assert stats["contexts_active"] == 1
        assert stats["policy_version"] is not None
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# stats
# --------------------------------------------------------------------------- #


def test_stats_initial_shape(tmp_path):
    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False)
    try:
        stats = ledger.stats()
        assert stats["total_decisions"] == 0
        assert stats["total_outcomes"] == 0
        assert stats["join_rate"] == 0.0
        assert stats["decisions_by_action"] == {}
        assert stats["contexts_active"] == 0
        assert stats["contexts_draining"] == 0
        assert stats["ring_buffer_fill"] == 0.0
        assert stats["ring_buffer_size"] == 0
        assert stats["dropped_records"] == 0
        assert stats["policy_version"] is None
        assert "gatekeeper" in stats and "consumer" in stats
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# lifecycle
# --------------------------------------------------------------------------- #


def test_shutdown_drains_saves_stats_and_locks(tmp_path):
    db = tmp_path / "ledger.db"
    ledger = DecisionLedger(db, auto_start_consumer=False)
    ledger.evaluate(make_context_hash("a", "b"), confidence=0.5)

    final = ledger.shutdown()
    assert final["total_decisions"] == 1

    stats_file = tmp_path / "ledger_stats.json"
    assert stats_file.exists()
    assert json.loads(stats_file.read_text())["total_decisions"] == 1

    with pytest.raises(RuntimeError):
        ledger.evaluate(make_context_hash("a", "b"), confidence=0.5)
    with pytest.raises(RuntimeError):
        ledger.shutdown()


def test_context_manager_flushes_and_closes(tmp_path):
    with DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=False) as ledger:
        ledger.evaluate(make_context_hash("a", "b"), confidence=0.7)
    assert not ledger.consumer.is_alive()
    with pytest.raises(RuntimeError):
        ledger.evaluate(make_context_hash("a", "b"), confidence=0.7)


def test_background_consumer_shutdown_closes_db_cross_thread(tmp_path):
    """A running consumer opens its own SQLite connection on its thread; the
    main thread must still be able to close() it at shutdown without a
    ProgrammingError (check_same_thread regression)."""
    import time

    ledger = DecisionLedger(tmp_path / "ledger.db", auto_start_consumer=True)
    ctx = make_context_hash("qwen-7b", "routing")
    for _ in range(50):
        ledger.evaluate(ctx, confidence=0.5)

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        flushed = ledger.consumer.get_metrics()["total_records_flushed"]
        if flushed >= 50:
            break
        time.sleep(0.1)
    else:
        raise AssertionError("consumer never flushed the 50 records")

    final = ledger.shutdown()  # must not raise ProgrammingError
    assert final["total_decisions"] == 50
