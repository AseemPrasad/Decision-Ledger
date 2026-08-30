"""End-to-end integration, performance and stress tests.

Covers the full serving path end-to-end:

* integration: full single-context workflow, multi-context calibration,
  policy reload/roll-forward, and drift detection over mixed traffic;
* performance: 1000 sequential evaluations (p50/p99 must stay well under
  the 1 ms per-evaluation budget), 1000 concurrent evaluations, full
  consumer durability flush, and calibration timing over 100k records;
* stress: ring buffer at 90% fill, a held SQLite write lock (graceful
  degradation), missing outcomes, and an extreme exploration rate.

Runs with the rest of the suite: ``pytest tests/test_end_to_end.py``
(or ``pytest`` once ``tests`` is in ``testpaths``). Heavy cases are marked
``slow`` and can be skipped with ``-m 'not slow'``; the micro-benchmarks are
marked ``benchmark``.
"""

import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from decision_ledger import DecisionLedger, Gatekeeper, make_context_hash
from decision_ledger.calibration import CalibrationResult, ConformalCalibrator
from decision_ledger.database import DatabaseError
from decision_ledger.pipeline import CalibrationPipeline
from decision_ledger.policy import PolicyGenerator, load_policy
from decision_ledger.utils import generate_uuidv7

LATENCY_BUDGET_US = 1_000  # < 1 ms per evaluate(), the documented budget


def _decision_ids(ledger) -> list:
    """Durable ``decision_id`` values in the store, oldest first."""
    rows = ledger.database.execute_query(
        "SELECT decision_id FROM decisions ORDER BY timestamp_ns ASC"
    )
    return [row["decision_id"] for row in rows]


def _new_ledger(tmp_path: Path, name: str = "ledger.db", **kwargs) -> DecisionLedger:
    """Build a ledger with an isolated policies dir under ``tmp_path``.

    Pointing ``policy_file`` at a placeholder artifact gives the ledger's
    ``PolicyGenerator`` a tmp-scoped directory, so different tests can never
    collide on same-second policy versions in the repo's ``data/policies``.
    """
    policies = tmp_path / "policies"
    policies.mkdir(exist_ok=True)
    generator = PolicyGenerator(str(policies))
    artifact = generator.generate_policy(
        {}, policy_version="20000101-000000", force=True
    )
    kwargs.setdefault("auto_start_consumer", True)
    kwargs.setdefault("exploration_rate", 0.0)
    policy_file = kwargs.pop("policy_file", artifact)
    return DecisionLedger(tmp_path / name, policy_file=policy_file, **kwargs)


def _recalibrate(ledger: DecisionLedger, tmp_path: Path) -> str:
    """Re-run the calibration pipeline into a fresh policies dir.

    ``ledger.calibrate()`` derives its artifact version from the wall clock and
    refuses to overwrite; a second facade calibration inside the same second
    would collide. A fresh ``PolicyGenerator`` directory sidesteps that while
    exercising the real pipeline + hot reload. Keeps the facade's drain + join
    semantics so the pipeline sees the latest decisions/outcomes.
    """
    ledger.consumer.drain_now()
    ledger.database.joiner.join_decisions_and_outcomes()
    generator = PolicyGenerator(str(tmp_path / "policies-recal"))
    pipeline = CalibrationPipeline(ledger.database, ledger.gatekeeper, generator)
    return pipeline.run_calibration()


def _evaluate(ledger, ctx, n, confidence=None, decision_type="route") -> None:
    """Evaluate ``n`` decisions against ``ctx`` with a confidence sweep."""
    for i in range(n):
        conf = (
            confidence
            if confidence is not None
            else 0.60 + (i % 40) / 100.0  # 0.60 .. 0.99
        )
        ledger.evaluate(ctx, confidence=conf, decision_type=decision_type)


def _log_outcomes_consistent(ledger, ctx, threshold=0.8) -> int:
    """Log one outcome per decision, correct iff confidence >= threshold."""
    rows = ledger.database.get_decisions(context_hash=ctx)
    for row in rows:
        good = 1.0 if row["model_confidence"] >= threshold else 0.0
        ledger.log_outcome(
            row["decision_id"], good, outcome_source="task_metric", metadata=""
        )
    return len(rows)


# --------------------------------------------------------------------------- #
# 1. End-to-end scenarios
# --------------------------------------------------------------------------- #


def test_full_workflow(tmp_path):
    """Initialize, evaluate 500, log 400 outcomes, calibrate, inspect stats."""
    ledger = _new_ledger(tmp_path)
    try:
        ctx = make_context_hash("qwen-7b", "routing")

        _evaluate(ledger, ctx, 500)
        ledger.consumer.drain_now()
        assert len(_decision_ids(ledger)) == 500

        decisions = ledger.database.get_decisions(context_hash=ctx)
        assert len(decisions) == 500
        for row in decisions[:400]:
            good = 1.0 if row["model_confidence"] >= 0.8 else 0.0
            ledger.log_outcome(
                row["decision_id"],
                good,
                outcome_source="human",
                metadata='{"reviewer": "ops"}',
            )

        policy_path = ledger.calibrate()
        assert policy_path.endswith(".yaml")
        assert Path(policy_path).exists()

        s = ledger.stats()
        assert s["total_decisions"] == 500
        assert s["total_outcomes"] == 400
        assert s["join_rate"] == pytest.approx(0.8)
        assert s["contexts_active"] == 1
        assert s["policy_version"]

        context = ledger.gatekeeper.policy[ctx]
        assert context.is_active and context.q_hat is not None

        artifact = load_policy(policy_path)
        assert [c["state"] for c in artifact["contexts"]] == ["ACTIVE"]

        # The fresh policy now delegates high-confidence traffic.
        assert ledger.evaluate(ctx, confidence=0.98) == "DELEGATE"
        assert ledger.stats()["gatekeeper"]["delegate"] >= 1
    finally:
        ledger.shutdown()


def test_multi_context_workflow(tmp_path):
    """Five contexts calibrate independently and all activate."""
    ledger = _new_ledger(tmp_path)
    contexts = [make_context_hash(f"qwen-7b-model-{i}", "routing") for i in range(5)]
    try:
        for ctx in contexts:
            _evaluate(ledger, ctx, 150)
        ledger.consumer.drain_now()

        for ctx in contexts:
            decisions = ledger.database.get_decisions(context_hash=ctx)
            assert len(decisions) == 150
            for row in decisions[:120]:
                good = 1.0 if row["model_confidence"] >= 0.75 else 0.0
                ledger.log_outcome(
                    row["decision_id"], good, outcome_source="task_metric"
                )

        policy_path = ledger.calibrate()

        s = ledger.stats()
        assert s["total_decisions"] == 750
        assert s["total_outcomes"] == 600
        assert s["contexts_active"] == 5

        artifact = load_policy(policy_path)
        states = [c["state"] for c in artifact["contexts"]]
        assert states == ["ACTIVE"] * 5

        for ctx in contexts:
            context = ledger.gatekeeper.policy[ctx]
            assert context.is_active and context.q_hat is not None
            assert ledger.evaluate(ctx, confidence=0.99) == "DELEGATE"
    finally:
        ledger.shutdown()


def test_policy_reload_workflow(tmp_path):
    """A stronger policy (higher q_hat) changes decisions after hot reload."""
    policies = tmp_path / "artifact-policies"
    policies.mkdir()
    gen = PolicyGenerator(str(policies))
    ctx = make_context_hash("qwen-7b", "routing")

    def result(q_hat):
        return CalibrationResult(
            q_hat=q_hat,
            sample_size=200,
            coverage_lower_bound=0.95,
            achieved_empirical_risk=0.05,
        )

    v1 = gen.generate_policy({ctx: result(0.10)}, policy_version="20000101-000000")
    v2 = gen.generate_policy({ctx: result(0.90)}, policy_version="20000101-000001")

    ledger = _new_ledger(tmp_path, policy_file=v1)
    try:
        # v1: q_hat 0.10 -> confidence 0.95 (non-cf 0.05) delegates, 0.50 does not.
        assert ledger.gatekeeper.policy[ctx].q_hat == pytest.approx(0.10)
        assert ledger.evaluate(ctx, confidence=0.95) == "DELEGATE"
        assert ledger.evaluate(ctx, confidence=0.50) == "ESCALATE"
        delegate_before = ledger.gatekeeper.get_metrics()["delegate"]

        # v2: q_hat 0.90 -> 0.50 now delegates too, 0.05 still escalates.
        ledger.gatekeeper.reload_policy_from_file(v2)
        assert ledger.gatekeeper.policy[ctx].q_hat == pytest.approx(0.90)
        assert ledger.evaluate(ctx, confidence=0.50) == "DELEGATE"
        assert ledger.evaluate(ctx, confidence=0.05) == "ESCALATE"
        delegate_after = ledger.gatekeeper.get_metrics()["delegate"]
        assert delegate_after > delegate_before
        assert ledger.stats()["policy_version"] == "20000101-000001"

        # Policies are interchangeable snapshots: a throwaway gate on v1 sees v1.
        gk_v1 = Gatekeeper.from_policy_file(v1, exploration_rate=0.0)
        assert gk_v1.evaluate(ctx, 0.50, "route").name == "ESCALATE"
    finally:
        ledger.shutdown()


def test_drift_detection_workflow(tmp_path):
    """Mixed delegated/exploratory traffic; drift is detected when present."""
    ledger = _new_ledger(tmp_path)
    ctx = make_context_hash("qwen-7b", "routing")
    try:
        # Phase A: seed a healthy active context (all correct, no exploration).
        _evaluate(ledger, ctx, 220, confidence=0.90)
        ledger.consumer.drain_now()
        _log_outcomes_consistent(ledger, ctx)
        ledger.calibrate()
        assert ledger.gatekeeper.policy[ctx].is_active

        # Phase B: adversarial mix -- delegated (conf 0.99) and shadow (0.60).
        ledger.gatekeeper.exploration_rate = 0.5
        for i in range(200):
            conf = 0.99 if i % 2 == 0 else 0.60
            ledger.evaluate(ctx, confidence=conf)
        ledger.consumer.drain_now()

        rows = ledger.database.execute_query(
            "SELECT decision_id, action_taken FROM decisions"
            " WHERE context_hash = ? ORDER BY timestamp_ns DESC LIMIT 200",
            (ctx,),
        )
        delegated = [r for r in rows if r["action_taken"] == "DELEGATE"]
        explored = [r for r in rows if r["action_taken"] == "EXPLORE_SHADOW"]
        assert delegated, "expected DELEGATE records in the mix"
        assert explored, "expected EXPLORE_SHADOW records in the mix"

        # Adversarial labels: delegated decisions fail, exploratory succeed.
        for row in delegated:
            ledger.log_outcome(row["decision_id"], 0.0, "task_metric")
        for row in explored:
            ledger.log_outcome(row["decision_id"], 1.0, "task_metric")

        # Run calibration again (materializes joins + publishes a new policy).
        _recalibrate(ledger, tmp_path)

        cal = ConformalCalibrator(ledger.database)
        drift = cal.detect_drift(ctx)
        assert drift["drift_detected"] is True
        assert drift["active_range_accuracy"] < drift["full_range_accuracy"]
        assert drift["divergence"] > 0.05

        # A control context with healthy data must not drift.
        ctx_ok = make_context_hash("qwen-7b", "summarize")
        _evaluate(ledger, ctx_ok, 120, confidence=0.90)
        ledger.consumer.drain_now()
        _log_outcomes_consistent(ledger, ctx_ok)
        _recalibrate(ledger, tmp_path)
        assert cal.detect_drift(ctx_ok)["drift_detected"] is False

        # System stays healthy through the drift event.
        action = ledger.evaluate(ctx, confidence=0.90)
        assert action in ("ESCALATE", "DELEGATE", "EXPLORE_SHADOW")
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# 2. Performance tests
# --------------------------------------------------------------------------- #


@pytest.mark.benchmark
def test_latency_1000_sequential_evaluations(tmp_path):
    """p50/p99 of 1000 sequential evaluations stay under the 1 ms budget."""
    ledger = _new_ledger(tmp_path)
    ctx = make_context_hash("qwen-7b", "routing")
    n = 1_000
    try:
        latencies_us = []
        started = time.perf_counter()
        for _ in range(n):
            t0 = time.perf_counter_ns()
            ledger.evaluate(ctx, confidence=0.90)
            latencies_us.append((time.perf_counter_ns() - t0) / 1000)
        elapsed_s = time.perf_counter() - started

        latencies_us.sort()
        p50 = latencies_us[n // 2]
        p99 = latencies_us[min(int(n * 0.99) - 1, n - 1)]
        throughput = n / elapsed_s

        assert p99 < LATENCY_BUDGET_US, f"p99 {p99:.1f}us exceeds 1ms budget"
        assert p50 < 500, f"p50 {p50:.1f}us unexpectedly high"
        print(
            f"[latency: p50={p50:.1f}us p99={p99:.1f}us "
            f"max={latencies_us[-1]:.1f}us {throughput:,.0f} evals/s]"
        )
    finally:
        ledger.shutdown()


@pytest.mark.benchmark
def test_throughput_1000_concurrent_evaluations(tmp_path):
    """1000 evaluations across 8 threads complete without error or loss."""
    ledger = _new_ledger(tmp_path)
    ctx = make_context_hash("qwen-7b", "routing")
    total = 1_000
    workers = 8

    def run_evaluations(kwargs):
        batch, confidence = kwargs
        results = []
        for _ in range(batch):
            results.append(ledger.evaluate(ctx, confidence=confidence))
        return results

    per_worker = total // workers
    payload = [(per_worker, 0.90 + (i % 5) / 100.0) for i in range(workers)]
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        batches = list(pool.map(run_evaluations, payload))
    elapsed_s = time.perf_counter() - started

    all_actions = [a for batch in batches for a in batch]
    assert len(all_actions) == total
    assert all(a in ("ESCALATE", "DELEGATE", "EXPLORE_SHADOW") for a in all_actions)

    ledger.consumer.drain_now()
    assert ledger.stats()["total_decisions"] == total
    assert ledger.stats()["dropped_records"] == 0
    print(
        f"[throughput: {total} concurrent evals in {elapsed_s:.3f}s "
        f"({total / elapsed_s:,.0f} evals/s), 0 dropped]"
    )

    ledger.shutdown()


def test_consumer_flushes_all_decisions_to_db(tmp_path):
    """Background consumer durably flushes all 1000 decisions."""
    ledger = _new_ledger(tmp_path, flush_interval=0.1)
    ctx = make_context_hash("qwen-7b", "routing")
    n = 1_000
    try:
        _evaluate(ledger, ctx, n)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if ledger.stats()["total_decisions"] >= n:
                break
            time.sleep(0.05)

        s = ledger.stats()
        assert s["total_decisions"] == n
        assert s["consumer"]["total_records_flushed"] >= n
        assert s["consumer"]["total_records_dropped"] == 0
        assert s["consumer"]["backlog_records"] == 0
        assert s["ring_buffer_size"] == 0
    finally:
        ledger.shutdown()


@pytest.mark.slow
def test_calibration_100k_records(tmp_path):
    """Calibration engine processes 100k decision/outcome pairs in one pass."""
    db = _new_db(tmp_path)
    try:
        contexts = [make_context_hash(f"qwen-7b-{i}", "routing") for i in range(100)]
        count = 1_000  # per context -> 100k total
        for ctx in contexts:
            _seed_decision_outcome_pairs(db, ctx, count)

        db.joiner.join_decisions_and_outcomes()
        stats = db.get_join_statistics()
        assert stats["total_decisions"] == 100_000
        assert stats["total_outcomes"] == 100_000

        generator = PolicyGenerator(str(tmp_path / "cal-policies"))
        pipeline = CalibrationPipeline(db, Gatekeeper(policy={}), generator)
        start = time.perf_counter()
        policy_path = pipeline.run_calibration()
        elapsed_s = time.perf_counter() - start

        cfg = pipeline.get_calibration_stats()
        assert cfg["contexts_calibrated"] == 100
        assert cfg["q_hat_distribution"]["activated_count"] == 100
        assert Path(policy_path).exists()
        assert elapsed_s < 120.0, f"calibration took {elapsed_s:.1f}s"
        print(
            f"[calibration 100k records: {elapsed_s:.2f}s "
            f"({100_000 / elapsed_s:,.0f} records/s, {len(contexts)} contexts)]"
        )
    finally:
        db.close()


# --------------------------------------------------------------------------- #
# 3. Stress tests
# --------------------------------------------------------------------------- #


def test_ring_buffer_at_90_percent_capacity(tmp_path):
    """90% fill: backpressure kicks in but the system keeps working."""
    capacity = 20_000
    ledger = _new_ledger(
        tmp_path, ring_buffer_capacity=capacity, auto_start_consumer=False
    )
    ctx = make_context_hash("qwen-7b", "routing")
    try:
        target = int(capacity * 0.90)
        _evaluate(ledger, ctx, target, confidence=0.90)

        s = ledger.stats()
        assert s["ring_buffer_fill"] == pytest.approx(0.90, abs=0.02)
        assert s["ring_buffer_size"] == target
        assert s["dropped_records"] == 0

        # Push past 90% toward the drop tier: still no crash, no drops yet.
        _evaluate(ledger, ctx, 500, confidence=0.90)
        assert ledger.stats()["ring_buffer_fill"] > 0.90
        assert ledger.stats()["dropped_records"] == 0

        # A healthy drain fully recovers (drain_now caps at 10k per call).
        drained = 0
        while (n_rows := ledger.consumer.drain_now()) > 0:
            drained += n_rows
        assert drained == capacity - 1_500
        s = ledger.stats()
        assert s["ring_buffer_fill"] == 0.0
        assert s["dropped_records"] == 0
    finally:
        ledger.shutdown()


@pytest.mark.slow
def test_database_locked_graceful_degradation(tmp_path):
    """A held SQLite write lock raises cleanly, then everything recovers."""
    import sqlite3

    ledger = _new_ledger(tmp_path)
    ctx = make_context_hash("qwen-7b", "routing")
    db = ledger.database
    try:
        # Take an exclusive writer lock on a second connection.
        blocker = sqlite3.connect(str(tmp_path / "ledger.db"), timeout=0.1)
        blocker.execute("BEGIN EXCLUSIVE")
        blocker.execute("CREATE TABLE _hold (x INTEGER)")
        try:
            started = time.perf_counter()
            with pytest.raises(DatabaseError):
                db.execute_write("UPDATE decisions SET latency_us = 1 WHERE 1 = 0")
            lock_wait = time.perf_counter() - started
        finally:
            blocker.rollback()
            blocker.close()

        # Service-order recovery: the write path works again.
        db.execute_write("UPDATE decisions SET latency_us = 1 WHERE 1 = 0")
        ledger.evaluate(ctx, confidence=0.90)
        ledger.consumer.drain_now()
        assert ledger.stats()["total_decisions"] >= 1
        print(f"[lock stress: raised DatabaseError after {lock_wait:.1f}s, recovered]")
    finally:
        ledger.shutdown()


def test_missing_outcomes_calibration_still_works(tmp_path):
    """Contexts without outcomes are skipped, not fatal; others calibrate."""
    ledger = _new_ledger(tmp_path)
    ctx_good = make_context_hash("qwen-7b-model-a", "routing")
    ctx_empty = make_context_hash("qwen-7b-model-b", "routing")
    try:
        _evaluate(ledger, ctx_good, 150, confidence=0.90)
        _evaluate(ledger, ctx_empty, 150, confidence=0.90)
        ledger.consumer.drain_now()
        _log_outcomes_consistent(ledger, ctx_good)

        ledger.calibrate()

        s = ledger.stats()
        assert s["total_outcomes"] == 150
        assert s["join_rate"] == pytest.approx(0.5)
        assert s["contexts_active"] == 1
        assert ctx_empty not in ledger.gatekeeper.policy
        assert ledger.evaluate(ctx_empty, confidence=0.95) == "ESCALATE"
        assert ledger.evaluate(ctx_good, confidence=0.95) == "DELEGATE"
    finally:
        ledger.shutdown()


def test_very_high_exploration_rate(tmp_path):
    """Exploration ~1.0: every eligible call is shadowed, none counted."""
    ledger = _new_ledger(tmp_path)
    ctx = make_context_hash("qwen-7b", "routing")
    try:
        # Calibrate first at zero exploration so the context activates.
        _evaluate(ledger, ctx, 130, confidence=0.90)
        ledger.consumer.drain_now()
        _log_outcomes_consistent(ledger, ctx)
        ledger.calibrate()
        assert ledger.gatekeeper.policy[ctx].is_active

        # Max exploration: every eligible call becomes EXPLORE_SHADOW.
        ledger.gatekeeper.exploration_rate = 1.0
        for _ in range(50):
            assert ledger.evaluate(ctx, confidence=0.99) == "EXPLORE_SHADOW"
        ledger.consumer.drain_now()

        actions = [
            row["action_taken"]
            for row in ledger.database.execute_query(
                "SELECT action_taken FROM decisions"
                " WHERE context_hash = ? ORDER BY timestamp_ns DESC LIMIT 50",
                (ctx,),
            )
        ]
        assert actions == ["EXPLORE_SHADOW"] * 50

        # Exploratory records must not inflate the calibration sample.
        shadow_ids = ledger.database.execute_query(
            "SELECT decision_id FROM decisions"
            " WHERE context_hash = ? AND action_taken = 'EXPLORE_SHADOW'",
            (ctx,),
        )
        for row in shadow_ids:
            ledger.log_outcome(row["decision_id"], 1.0, "task_metric")
        _recalibrate(ledger, tmp_path)
        assert ledger.gatekeeper.policy[ctx].current_sample_size == 130
        assert ledger.stats()["gatekeeper"]["explore"] >= 50

        # Extreme exploration rate is still a valid value (0 <= rate <= 1).
        ledger2 = _new_ledger(tmp_path, "ledger2.db", exploration_rate=1.0)
        ledger2.shutdown()
        with pytest.raises(ValueError):
            _new_ledger(tmp_path, "ledger3.db", exploration_rate=1.5)
    finally:
        ledger.shutdown()


# --------------------------------------------------------------------------- #
# Helpers for the 100k-record calibration bench
# --------------------------------------------------------------------------- #


def _new_db(tmp_path: Path):
    from decision_ledger import Database

    return Database(tmp_path / "calibration.db")


def _seed_decision_outcome_pairs(db, ctx: bytes, count: int, chunk: int = 2_000):
    """Insert ``count`` decision+outcome pairs for ``ctx`` in bounded chunks."""
    base_ns = time.time_ns()
    for start in range(0, count, chunk):
        end = min(start + chunk, count)
        decisions, outcomes = [], []
        for i in range(start, end):
            decision_id = generate_uuidv7()
            ts = base_ns + i
            decisions.append(
                {
                    "decision_id": decision_id,
                    "timestamp_ns": ts,
                    "context_hash": ctx,
                    "decision_type": "route",
                    "model_confidence": 0.90,
                    "non_conformity": 0.10,
                    "action_taken": "DELEGATE",
                    "latency_us": 5,
                }
            )
            outcomes.append(
                {
                    "outcome_id": generate_uuidv7(),
                    "decision_id": decision_id,
                    "timestamp_ns": ts + 1_000,
                    "outcome_value": 1.0,
                    "outcome_source": "task_metric",
                    "metadata": "",
                }
            )
        db.batch_insert("decisions", decisions)
        db.batch_insert("outcomes", outcomes)
