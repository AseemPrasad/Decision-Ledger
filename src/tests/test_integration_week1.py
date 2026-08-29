"""Integration tests: the gatekeeper and ring buffer working together.

What "working together" means here
----------------------------------
Every ``evaluate`` call on a gatekeeper with a ring buffer attached must
produce exactly one logged ``DecisionRecord`` -- under high frequency,
concurrent policy reloads, deterministic exploration, and a buffer whose
backpressure warnings are firing. None of that may add pathological latency
to the serving path (p99 < 1ms) and the buffer must never drop a decision
unless its overload drop tier is explicitly engaged.

Gaps it guards against
----------------------
* a telemetry path that silently skips `EXPLORE_SHADOW` or escalate records;
* a `reload_policy` that races an in-flight evaluation and loses a record;
* a saturated buffer that truncates the audit trail without warning;
* an unbounded drain cost that would make the consumer the bottleneck.

Scenarios and expected runtime (Ryzen 3 5300U, Python 3.14, venv):

1. `test_high_frequency_...`     10k evals, p50/p99 latency    ~0.5-1.5s
2. `test_concurrent_policy_reload...` 5s reload stress (slow)  ~5-6s
3. `test_exploration_coverage_...`    5k evals @ 2% behavior   ~0.3-0.6s
4. `test_ring_buffer_backpressure_...` 90% fill + saturation   ~<0.5s
5. `test_benchmark_*`              pytest-benchmark pair        ~3-8s

Run:  pytest src/tests/test_integration_week1.py
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import pytest

from decision_ledger import (
    CalibrationContext,
    CalibrationResult,
    DecisionRecord,
    GateAction,
    Gatekeeper,
    RingBuffer,
    ServingPolicy,
    context_hash,
    policy_from_results,
)

# Three distinct serving contexts: each has its own decision type, prompt
# template, and calibration envelope (q_hat), so the tests exercise several
# policy dict keys instead of one trivial context.
CTX_SPECS: List[Tuple[str, str, float]] = [
    ("route", "SELECT * FROM {table} LIMIT {n};", 0.20),
    ("judge", "Is the claim '{c}' true?", 0.30),
    ("speculate", "Predict the next value in {series}", 0.40),
]

CONTEXT_MAP: Dict[bytes, Tuple[str, float]] = {
    context_hash(decision_type, prompt_template=template): (decision_type, q_hat)
    for decision_type, template, q_hat in CTX_SPECS
}


def _build_policy(q_hat_overrides: Dict[bytes, float] | None = None) -> ServingPolicy:
    """Active serving policy for the three mock contexts (all calibrated)."""
    q_hat_overrides = q_hat_overrides or {}
    results = {
        ctx: CalibrationResult(
            q_hat=q_hat_overrides.get(ctx, q_hat),
            sample_size=1000,
            coverage_lower_bound=0.95,
            achieved_empirical_risk=0.02,
        )
        for ctx, (_, q_hat) in CONTEXT_MAP.items()
    }
    return policy_from_results(results, version_id=1, min_sample_size=100)


def _confidence(i: int) -> float:
    """Deterministic pseudo-random confidence in [0.60, 0.99).

    Not from ``random`` so the scenario is reproducible across runs; spread
    wide enough that both DELEGATE and ESCALATE happen for any q_hat <= 0.39.
    """
    return 0.60 + ((i * 7) % 40) / 100.0


@pytest.fixture
def mock_gatekeeper() -> Gatekeeper:
    """A gatekeeper over the three contexts with exploration disabled."""
    return Gatekeeper(_build_policy().contexts, exploration_rate=0.0)


# --- Test 1: high-frequency evaluation with logging -------------------------


def test_high_frequency_evaluation_logs_every_decision():
    """10k sequential evaluations, every one logged; p99 latency < 1ms.

    The ring buffer is the audit trail: a decision lost here is a silent gap
    in the ledger. Latency is measured per call (``perf_counter_ns``) and
    summarized as p50/p99 against the 1ms serving budget.

    Expected runtime: ~0.5-1.5s.
    """
    policy = _build_policy()
    buffer = RingBuffer(capacity=100_000)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)

    contexts = list(policy.contexts)
    n = 10_000
    latencies_us: List[float] = []
    for i in range(n):
        ctx = contexts[i % len(contexts)]
        decision_type = CONTEXT_MAP[ctx][0]
        start = time.perf_counter_ns()
        gk.evaluate(ctx, _confidence(i), decision_type)
        latencies_us.append((time.perf_counter_ns() - start) / 1000.0)

    # No data loss: the buffer holds exactly one record per evaluation.
    assert buffer.size() == n, f"expected {n} records buffered, got {buffer.size()}"
    assert (
        buffer.total_pushed == n
    ), f"pushed {buffer.total_pushed} records for {n} evaluations"
    assert (
        buffer.dropped_count == 0
    ), f"{buffer.dropped_count} decisions lost before any overload"

    records = buffer.pop_batch(max_records=n + 1)
    assert len(records) == n, f"drained {len(records)} records, expected all {n}"
    # Every record carries its own identifier and both action outcomes appear,
    # i.e. the telemetry path captured the full decision, not a stub.
    assert all(r.decision_id for r in records), "record without a decision_id"
    actions = {r.action_taken for r in records}
    assert actions <= {
        GateAction.DELEGATE.name,
        GateAction.ESCALATE.name,
    }, f"unexpected actions logged: {actions}"

    p50 = float(np.percentile(latencies_us, 50))
    p99 = float(np.percentile(latencies_us, 99))
    mean_us = float(np.mean(latencies_us))
    print(
        f"\n  high-freq (n={n}): p50={p50:.1f}us p99={p99:.1f}us mean={mean_us:.1f}us"
    )
    assert p99 < 1000.0, f"p99 latency {p99:.1f}us exceeded the 1ms budget"


# --- Test 2: concurrent policy reload ---------------------------------------


@pytest.mark.slow
def test_concurrent_policy_reload_without_crash_or_data_loss():
    """Evaluator thread streams 250k decisions while a reloader thread swaps
    the policy every ~100ms across a 5-second window.

    Reload swaps the context dict under an ``RLock``; evaluations must keep
    working off either snapshot and every one must still reach the ring
    buffer. Exceptions in either thread are collected and surfaced in the
    test body, so a crash would fail loudly instead of silently.

    Expected runtime: ~5s (the reload window); slow-marked.
    """
    policy = _build_policy()
    keys = list(policy.contexts)
    buffer = RingBuffer(capacity=500_000)  # headroom: no drops by design
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)

    # Five reload variants (different q_hat per context) so each swap is a
    # real behavior change, not a no-op assignment to the same dict.
    variants: List[Dict[bytes, CalibrationContext]] = []
    for q_hat in (0.15, 0.20, 0.25, 0.30, 0.35):
        results = {
            ctx: CalibrationResult(
                q_hat=q_hat,
                sample_size=1000,
                coverage_lower_bound=0.95,
                achieved_empirical_risk=0.02,
            )
            for ctx in keys
        }
        variants.append(
            policy_from_results(results, version_id=1, min_sample_size=100).contexts
        )

    stop = threading.Event()
    errors: List[BaseException] = []
    evaluated = [0]
    EVAL_TARGET = 250_000
    WINDOW_S = 5.0
    pacing_start = time.perf_counter()

    def evaluator() -> None:
        try:
            step = WINDOW_S / EVAL_TARGET  # stretch 250k evals across 5s
            while not stop.is_set() and evaluated[0] < EVAL_TARGET:
                ctx = keys[evaluated[0] % len(keys)]
                decision_type = CONTEXT_MAP[ctx][0]
                gk.evaluate(ctx, _confidence(evaluated[0]), decision_type)
                evaluated[0] += 1
                # Pacing: busy-wait to the next slot (sleep granularity on
                # Windows is ~1ms, far coarser than the ~20us budget).
                target = pacing_start + evaluated[0] * step
                while time.perf_counter() < target:
                    pass
        except BaseException as exc:
            errors.append(exc)

    reloads = [0]

    def _run() -> None:
        try:
            variant = 0
            while not stop.is_set():
                gk.reload_policy(variants[variant % len(variants)])
                variant += 1
                reloads[0] = variant
                stop.wait(0.1)
        except BaseException as exc:
            errors.append(exc)

    thread_ev = threading.Thread(target=evaluator, name="evaluator")
    thread_rl = threading.Thread(target=_run, name="reloader")
    thread_ev.start()
    thread_rl.start()

    stop.wait(WINDOW_S)  # let the scenario run for 5 seconds
    stop.set()
    thread_ev.join(5)
    thread_rl.join(5)

    assert not errors, f"concurrent scenario raised: {errors}"
    assert not thread_ev.is_alive(), "evaluator thread hung"
    assert not thread_rl.is_alive(), "reloader thread hung"
    assert (
        reloads[0] >= 30
    ), (  # ~50 expected over 5s at 100ms cadence
        f"only {reloads[0]} policy reloads in {WINDOW_S}s"
    )

    expected = evaluated[0]
    assert (
        expected == EVAL_TARGET
    ), f"evaluator completed {expected}/{EVAL_TARGET} decisions"
    # No data loss under reload stress: counting agrees on both sides.
    assert (
        buffer.total_pushed == expected
    ), f"buffer accepted {buffer.total_pushed} pushes for {expected} evaluations"
    assert (
        buffer.dropped_count == 0
    ), f"{buffer.dropped_count} decisions dropped during reload stress"
    assert (
        buffer.size() == expected
    ), f"buffered {buffer.size()} != evaluated {expected}"
    records = buffer.pop_batch(max_records=expected + 1)
    assert (
        len(records) == expected
    ), f"drained {len(records)} records, expected all {expected}"


# --- Test 3: exploration coverage -------------------------------------------


def test_exploration_coverage_within_one_percent():
    """5k evaluations at exploration_rate=0.02 yield ~100 EXPLORE_SHADOW
    decisions (deterministically: exactly one per ``int(1/0.02)=50`` eligible
    call), and every one of them is still logged.

    Epsilon exploration produces the counterfactual sample the calibration
    engine needs; if the ring buffer or the gatekeeper mis-accounted shadow
    decisions, that sample would be biased and the risk guarantee would not
    hold.

    Expected runtime: ~0.3-0.6s.
    """
    policy = _build_policy()
    ctx = next(iter(policy.contexts))
    buffer = RingBuffer(capacity=10_000)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.02, telemetry=buffer)

    n = 5000
    for i in range(n):
        gk.evaluate(ctx, _confidence(i), CONTEXT_MAP[ctx][0])

    records = buffer.pop_batch(max_records=n + 1)
    assert len(records) == n, f"drained {len(records)} records, expected all {n}"
    explored = sum(1 for r in records if r.action_taken == "EXPLORE_SHADOW")
    assert (
        98 <= explored <= 102
    ), f"explored {explored}/5000, expected ~100 (98-102, got {explored})"
    metrics = gk.get_metrics()
    assert metrics["explore"] == explored, (
        f"metrics.explore={metrics['explore']} disagrees with ring buffer "
        f"count {explored}"
    )


# --- Test 4: ring buffer filling behavior -----------------------------------


def test_ring_buffer_backpressure_warnings_at_90_percent(caplog):
    """Drive the ring buffer to ~90% fill, assert backpressure warnings are
    logged, then saturate it and confirm evaluations still work and the
    survivors drain cleanly.

    Without warning tiers a saturated buffer truncates the audit trail
    exactly when it matters most; with them, operators can scale the consumer
    before the drop tier engages. Drops are *counted*, never a crash.

    Expected runtime: < 0.5s.
    """
    capacity = 100
    policy = _build_policy()
    ctx = next(iter(policy.contexts))
    buffer = RingBuffer(capacity=capacity)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)

    caplog.set_level(logging.WARNING, logger="decision_ledger.telemetry")

    # Fill to ~90%: every evaluation must still succeed and be accepted.
    while buffer.fill_level() < 0.90:
        action = gk.evaluate(ctx, 0.8, CONTEXT_MAP[ctx][0])
        assert action in (
            GateAction.DELEGATE,
            GateAction.ESCALATE,
        ), f"evaluation misbehaved while filling: {action}"
    assert buffer.size() == 90, f"expected 90% fill, got {buffer.size()}"

    warnings = "\n".join(
        r.getMessage() for r in caplog.records if r.name == "decision_ledger.telemetry"
    )
    assert (
        "full" in warnings
    ), f"expected backpressure warnings near 90% fill, logged: {warnings!r}"

    # New evaluations still work once the buffer is saturated.
    for _ in range(30):
        action = gk.evaluate(ctx, 0.8, CONTEXT_MAP[ctx][0])
        assert action in (
            GateAction.DELEGATE,
            GateAction.ESCALATE,
        ), f"evaluation misbehaved while saturated: {action}"
    assert (
        buffer.size() == capacity
    ), f"buffer held {buffer.size()} records instead of its capacity {capacity}"
    assert buffer.dropped_count >= 1, "expected counted drops once the buffer was full"

    drained = buffer.pop_batch(max_records=capacity + 5)
    assert (
        len(drained) == capacity
    ), f"drained {len(drained)}, expected the {capacity} surviving records"


# --- Benchmarking (pytest-benchmark) ----------------------------------------


def _evaluate_target(gk: Gatekeeper, ctx: bytes) -> GateAction:
    return gk.evaluate(ctx, 0.9, "route")


@pytest.mark.benchmark
def test_benchmark_single_evaluation_latency(benchmark: Any):
    """Single ``evaluate`` call latency via pytest-benchmark (target < 1ms).

    The end-to-end p99 assertion lives in ``test_high_frequency_...``; here
    pytest-benchmark reports the full statistics (mean/min/max/std) for one
    evaluation so numbers are comparable across machines and commits.

    Expected runtime: a few seconds (auto-calibration).
    """
    policy = _build_policy()
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0)
    ctx = next(iter(policy.contexts))

    benchmark(_evaluate_target, gk, ctx)
    mean_us = benchmark.stats.get("mean") * 1e6
    assert (
        mean_us < 1000.0
    ), f"mean single-evaluation latency {mean_us:.1f}us >= 1ms budget"


@pytest.mark.benchmark
def test_benchmark_batch_pop_latency(benchmark: Any, caplog):
    """``pop_batch(1000)`` drain latency via pytest-benchmark (target < 100us).

    A full 1000-record FIFO drain is the batch consumer's per-poll cost; it
    must stay well under the polling interval so the drainer can keep up with
    the producer. The measured cost is the *drain* itself: records are moved
    into consumer hands, not destroyed in place (destroying 1000 records is
    consumer-side teardown of ~8-field frozen dataclasses and costs another
    ~110us -- a discard-style consumer should budget ~170us per poll, still
    far below the ~8ms it takes the producer to emit 1000 records). ``pedantic``
    times exactly one pop per round against a pre-filled buffer; the pre-fill
    is excluded and its backpressure tier warnings are silenced.

    Expected runtime: a few seconds.
    """
    capacity = 200_000
    policy = _build_policy()
    buffer = RingBuffer(capacity=capacity)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)
    ctx = list(policy.contexts)[0]

    caplog.set_level(logging.CRITICAL + 1, logger="decision_ledger.telemetry")
    for _ in range(capacity):  # pre-fill, not timed
        gk.evaluate(ctx, 0.9, "route")

    sink: List[DecisionRecord] = []  # consumer hand: records are moved

    def _pop_1000() -> int:
        popped = buffer.pop_batch(max_records=1000)
        sink.extend(popped)  # retain: teardown is not charged to the drain
        return len(popped)

    # 60 pops x 1000 records = 60k drained from the 200k pre-fill.
    benchmark.pedantic(_pop_1000, rounds=60, warmup_rounds=5)
    mean_us = benchmark.stats.get("mean") * 1e6
    assert (
        mean_us < 100.0
    ), f"mean pop_batch(1000) latency {mean_us:.1f}us >= 100us budget"
