"""Tests for the Conformal Gatekeeper.

Covers: the three action paths, deterministic epsilon exploration, policy
reloads, thread-safety during concurrent reads + writes, edge-case
confidences (0, 1, NaN, negative, over-1), invalid decision types, metrics,
telemetry integration, and a <1ms/1000-call latency benchmark.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from decision_ledger import (
    CalibrationContext,
    DecisionType,
    GateAction,
    Gatekeeper,
)
from decision_ledger.telemetry import RingBuffer

CTX = b"\x01" * 16
OTHER_HASH = b"\x02" * 16
Q_HAT = 0.2


def _context(
    *,
    context_hash: bytes = CTX,
    q_hat: float | None = Q_HAT,
    min_sample_size: int = 100,
    current_sample_size: int = 200,
    is_active: bool = True,
) -> CalibrationContext:
    return CalibrationContext(
        context_hash=context_hash,
        q_hat=q_hat,
        min_sample_size=min_sample_size,
        current_sample_size=current_sample_size,
        is_active=is_active,
    )


def _policy(**kwargs) -> dict[bytes, CalibrationContext]:
    return {CTX: _context(**kwargs)}


def _gatekeeper(**kwargs) -> Gatekeeper:
    exploration_rate = kwargs.pop("exploration_rate", 0.02)
    telemetry = kwargs.pop("telemetry", None)
    return Gatekeeper(
        _policy(**kwargs), exploration_rate=exploration_rate, telemetry=telemetry
    )


# --- Data structures ------------------------------------------------------


def test_gate_action_enum_values():
    assert GateAction.DELEGATE == 0
    assert GateAction.ESCALATE == 1
    assert GateAction.EXPLORE_SHADOW == 2


def test_decision_type_enum_aliases_decisions():
    assert DecisionType.ROUTE == 0
    assert DecisionType.JUDGE == 1
    assert DecisionType.SPECULATE == 2
    assert DecisionType.MUTATE == 3
    assert DecisionType.SUMMARIZE == 4
    assert DecisionType.ABSTAIN == 5


def test_calibration_context_defaults():
    ctx = CalibrationContext(context_hash=b"\x09" * 16)
    assert ctx.min_sample_size == 100
    assert ctx.q_hat is None
    assert ctx.current_sample_size == 0
    assert ctx.is_active is False
    assert ctx.has_enough_data is False


# --- The three action paths -----------------------------------------------


def test_delegate_when_within_threshold():
    gk = _gatekeeper(exploration_rate=0.0)
    assert gk.evaluate(CTX, 0.95, "route") == GateAction.DELEGATE


def test_escalate_when_above_threshold():
    gk = _gatekeeper(exploration_rate=0.0)
    assert gk.evaluate(CTX, 0.5, "route") == GateAction.ESCALATE


def test_explore_shadow_when_sampling_hits():
    gk = _gatekeeper(exploration_rate=1.0)
    for _ in range(5):
        assert gk.evaluate(CTX, 0.95, "route") == GateAction.EXPLORE_SHADOW


def test_actions_are_distinct():
    gk = _gatekeeper(exploration_rate=0.0)
    results = [
        gk.evaluate(CTX, 0.95, "route"),
        gk.evaluate(CTX, 0.50, "route"),
    ]
    assert len(set(results)) == 2
    assert GateAction.DELEGATE in results and GateAction.ESCALATE in results


# --- Fail-closed paths -----------------------------------------------------


def test_unknown_context_fails_closed():
    gk = Gatekeeper({}, exploration_rate=0.0)
    assert gk.evaluate(b"\xff" * 16, 0.99, "route") == GateAction.ESCALATE


def test_inactive_context_fails_closed():
    gk = _gatekeeper(exploration_rate=0.0, is_active=False)
    assert gk.evaluate(CTX, 0.99, "route") == GateAction.ESCALATE


def test_insufficient_samples_fails_closed():
    gk = _gatekeeper(exploration_rate=0.0, current_sample_size=10, min_sample_size=100)
    assert gk.evaluate(CTX, 0.99, "route") == GateAction.ESCALATE


def test_missing_q_hat_fails_closed():
    gk = _gatekeeper(exploration_rate=0.0, q_hat=None)
    assert gk.evaluate(CTX, 0.99, "route") == GateAction.ESCALATE


# --- Exploration -----------------------------------------------------------


def test_exploration_never_at_zero_rate():
    gk = _gatekeeper(exploration_rate=0.0)
    for _ in range(500):
        assert gk.evaluate(CTX, 0.95, "route") != GateAction.EXPLORE_SHADOW


def test_exploration_is_deterministic_by_call_number():
    gk = _gatekeeper(exploration_rate=0.02)
    actions = [gk.evaluate(CTX, 0.95, "route") for _ in range(60)]
    explored_at = [
        i + 1 for i, action in enumerate(actions) if action == GateAction.EXPLORE_SHADOW
    ]
    assert explored_at == [50]  # every 50th eligible call samples


def test_exploration_rate_matches_parameter():
    gk = _gatekeeper(exploration_rate=0.02)
    for _ in range(10_000):
        gk.evaluate(CTX, 0.95, "route")
    metrics = gk.get_metrics()
    assert metrics["explore"] == 200  # exact deterministic count
    assert metrics["exploration_rate"] == pytest.approx(0.02, abs=0.01)


# --- Policy reload ---------------------------------------------------------


def test_reload_policy_swaps_behavior():
    gk = _gatekeeper(exploration_rate=0.0)
    assert gk.evaluate(CTX, 0.8, "route") == GateAction.DELEGATE

    gk.reload_policy(_policy(q_hat=0.0))
    assert gk.evaluate(CTX, 0.8, "route") == GateAction.ESCALATE

    gk.reload_policy(_policy())
    assert gk.evaluate(CTX, 0.8, "route") == GateAction.DELEGATE


def test_reload_policy_does_not_interrupt_evaluations():
    gk = _gatekeeper(exploration_rate=0.0)
    results = []
    for i in range(200):
        if i == 75:
            gk.reload_policy(_policy(q_hat=0.0))  # strict mid-stream
        if i == 150:
            gk.reload_policy(_policy())  # back to loose
        results.append(gk.evaluate(CTX, 0.8, "route"))

    assert results[74] == GateAction.DELEGATE
    assert results[75] == GateAction.ESCALATE
    assert results[149] == GateAction.ESCALATE
    assert results[150] == GateAction.DELEGATE


# --- Thread safety ---------------------------------------------------------


def test_thread_safety_concurrent_reads_during_reload():
    strict = {CTX: _context(q_hat=0.0), OTHER_HASH: _context(context_hash=OTHER_HASH)}
    gk = Gatekeeper(dict(_policy()), exploration_rate=0.0)

    def worker() -> None:
        for _ in range(500):
            gk.evaluate(CTX, 0.95, "route")

    def reloader() -> None:
        for round_ in range(20):
            gk.reload_policy(dict(strict if round_ % 2 else _policy()))

    with ThreadPoolExecutor(max_workers=9) as executor:
        futures = [executor.submit(worker) for _ in range(8)]
        futures.append(executor.submit(reloader))
        for future in futures:
            future.result(timeout=30)

    metrics = gk.get_metrics()
    assert metrics["total"] == 4000  # every evaluation counted exactly once
    assert metrics["delegate"] + metrics["escalate"] + metrics["explore"] == 4000
    per_type = metrics["per_decision_type"]["route"]
    assert per_type["calls"] == 4000


# --- Edge-case confidences -------------------------------------------------


def test_confidence_of_zero_escalates():
    gk = _gatekeeper(exploration_rate=0.0)
    assert gk.evaluate(CTX, 0.0, "route") == GateAction.ESCALATE


def test_confidence_of_one_delegates():
    gk = _gatekeeper(exploration_rate=0.0)
    assert gk.evaluate(CTX, 1.0, "route") == GateAction.DELEGATE


def test_nan_confidence_warns_and_escalates(caplog):
    gk = _gatekeeper(exploration_rate=0.0)
    with caplog.at_level(logging.WARNING, logger="decision_ledger.gatekeeper"):
        action = gk.evaluate(CTX, float("nan"), "route")
    assert action == GateAction.ESCALATE
    assert any("NaN" in record.getMessage() for record in caplog.records)


def test_negative_confidence_warns_and_clamps(caplog):
    gk = _gatekeeper(exploration_rate=0.0)
    with caplog.at_level(logging.WARNING, logger="decision_ledger.gatekeeper"):
        action = gk.evaluate(CTX, -0.5, "route")
    assert action == GateAction.ESCALATE  # clamped to 0.0 -> non-conformity 1.0
    assert any("negative" in record.getMessage() for record in caplog.records)


def test_confidence_above_one_clamps_without_warning(caplog):
    gk = _gatekeeper(exploration_rate=0.0)
    with caplog.at_level(logging.WARNING, logger="decision_ledger.gatekeeper"):
        action = gk.evaluate(CTX, 1.5, "route")
    assert action == GateAction.DELEGATE  # clamped to 1.0
    assert caplog.records == []


def test_invalid_decision_type_warns_but_proceeds(caplog):
    gk = _gatekeeper(exploration_rate=0.0)
    with caplog.at_level(logging.WARNING, logger="decision_ledger.gatekeeper"):
        action = gk.evaluate(CTX, 0.95, "teleport")
    assert action == GateAction.DELEGATE
    assert any(
        "unknown decision_type" in record.getMessage() for record in caplog.records
    )


def test_invalid_exploration_rate_rejected():
    with pytest.raises(ValueError):
        Gatekeeper(_policy(), exploration_rate=1.5)
    with pytest.raises(ValueError):
        Gatekeeper(_policy(), exploration_rate=-0.01)


# --- Metrics ---------------------------------------------------------------


def test_get_metrics_initial_state():
    gk = _gatekeeper(exploration_rate=0.0)
    metrics = gk.get_metrics()
    assert metrics["total"] == 0
    assert metrics["delegate"] == 0
    assert metrics["escalate"] == 0
    assert metrics["explore"] == 0
    assert metrics["escalation_rate"] == 0.0
    assert metrics["exploration_rate"] == 0.0
    for name in ("route", "judge", "speculate", "mutate", "summarize", "abstain"):
        assert metrics["per_decision_type"][name] == {
            "calls": 0,
            "escalations": 0,
            "escalation_rate": 0.0,
        }


def test_get_metrics_tracks_per_type_rates():
    gk = _gatekeeper(exploration_rate=0.0)
    gk.evaluate(CTX, 0.95, "route")  # delegate
    gk.evaluate(CTX, 0.50, "route")  # escalate
    gk.evaluate(CTX, 0.95, "judge")  # delegate
    metrics = gk.get_metrics()
    assert metrics["delegate"] == 2
    assert metrics["escalate"] == 1
    assert metrics["total"] == 3
    assert metrics["escalation_rate"] == pytest.approx(1 / 3)

    route = metrics["per_decision_type"]["route"]
    assert route["calls"] == 2
    assert route["escalations"] == 1
    assert route["escalation_rate"] == pytest.approx(0.5)

    judge = metrics["per_decision_type"]["judge"]
    assert judge["calls"] == 1
    assert judge["escalation_rate"] == 0.0


# --- Telemetry integration -------------------------------------------------


def test_telemetry_records_when_ring_buffer_attached():
    buffer = RingBuffer(capacity=32)
    gk = _gatekeeper(exploration_rate=0.0, telemetry=buffer)
    gk.evaluate(CTX, 0.95, "route")  # delegate
    gk.evaluate(CTX, 0.50, "route")  # escalate

    records = buffer.pop_batch(max_records=10)
    assert len(records) == 2
    assert records[0].decision_type == "route"
    assert records[0].model_confidence == pytest.approx(0.95)
    assert records[0].non_conformity == pytest.approx(0.05)
    assert records[0].action_taken == GateAction.DELEGATE.name
    assert records[1].action_taken == GateAction.ESCALATE.name
    assert records[0].decision_id != "00000000-0000-0000-0000-000000000000"


# --- Benchmark -------------------------------------------------------------


def test_benchmark_1000_evaluations_under_1ms():
    gk = _gatekeeper(exploration_rate=0.0)
    for _ in range(3000):  # warm up (JIT/OS caches, first-touch branches)
        gk.evaluate(CTX, 0.95, "route")

    timings = []
    for _ in range(9):
        start = time.perf_counter()
        for _ in range(1000):
            gk.evaluate(CTX, 0.95, "route")
        timings.append(time.perf_counter() - start)

    median = sorted(timings)[len(timings) // 2]
    assert median < 0.001, f"median latency {median * 1e6:.0f}us exceeded 1ms budget"
