"""End-to-end integration test: the full decision loop.

Decisions -> telemetry -> outcomes -> join -> calibrate -> policy -> enforce.
"""

import math
import random

from decision_ledger import (
    CalibrationRecord,
    ConformalCalibrator,
    DecisionOutcomeJoiner,
    GateAction,
    Gatekeeper,
    OutcomeCollector,
    OutcomeSource,
    RingBuffer,
    context_hash,
    policy_from_results,
)
from decision_ledger.policy import save_policy


def _build_synthetic_stream(seed: int = 42) -> tuple[bytes, RingBuffer]:
    ctx = context_hash(
        "route",
        prompt_template="select * from {table} limit {n};",
        model_id="qwen-2.5-coder-7b-instruct",
        quantization_format="int8",
        temperature=0.2,
    )
    policy = policy_from_results(
        {ctx: _placeholder_result()}, version_id=1, min_sample_size=20
    )
    buffer = RingBuffer(capacity=4096)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)
    rng = random.Random(seed)

    n_requests = 400
    for _ in range(n_requests):
        confidence = rng.uniform(0.5, 1.0)  # small model's self-reported confidence
        gk.evaluate(ctx, confidence, "route")
    return ctx, buffer


def test_full_decision_loop(tmp_path):
    ctx, buffer = _build_synthetic_stream()
    decisions = buffer.pop_batch(max_records=10_000)
    assert len(decisions) == 400

    # Outcomes arrive asynchronously: model is right when confident > 0.7.
    collector = OutcomeCollector()
    for decision in decisions:
        correct = decision.model_confidence > 0.7
        collector.record(
            decision.decision_id,
            outcome_value=1.0 if correct else 0.0,
            outcome_source=OutcomeSource.TASK_METRIC,
        )
    assert len(list(collector.iter_records())) == 400

    # Join decisions to outcomes.
    joiner = DecisionOutcomeJoiner(collector.iter_records())
    joined = joiner.join(decisions)
    assert len(joined) == 400

    # Build calibration records (independent, non-exploratory).
    calibration_records = [
        CalibrationRecord(
            context_hash=record.context_hash,
            non_conformity_score=record.non_conformity,
            loss=record.loss,
            is_independent=True,
            is_exploratory=False,
        )
        for record in joined
    ]

    # Calibrate and build the serving policy.
    calibrator = ConformalCalibrator(target_alpha=0.05, min_sample_size=20)
    results = calibrator.calibrate_by_context(calibration_records)
    assert ctx in results
    assert results[ctx].q_hat is not None

    policy = policy_from_results(results, version_id=2, min_sample_size=20)
    policy_path = save_policy(policy, tmp_path / "policies" / "policy-v2.yaml")
    assert policy_path.exists()

    # Reload the YAML artifact and enforce with a fresh gatekeeper.
    from decision_ledger.policy import load_policy

    fresh = Gatekeeper(load_policy(policy_path).contexts, exploration_rate=0.0)
    assert fresh.evaluate(ctx, 0.95, "route") == GateAction.DELEGATE
    assert fresh.evaluate(ctx, 0.10, "route") == GateAction.ESCALATE


def _placeholder_result():
    from decision_ledger import CalibrationResult

    return CalibrationResult(
        q_hat=0.99,
        sample_size=400,
        coverage_lower_bound=0.8,
        achieved_empirical_risk=0.01,
    )


def test_consumer_writes_jsonl(tmp_path):
    from decision_ledger import BatchConsumer

    ctx, buffer = _build_synthetic_stream()
    consumer = BatchConsumer(buffer, tmp_path / "ledger", flush_interval_ms=50)
    written = consumer.drain_now()
    assert written == 400

    files = list((tmp_path / "ledger" / "decisions").rglob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 400
    first = __import__("json").loads(lines[0])
    assert "decision_id" in first and "context_hash" in first

    assert math.isclose(consumer.ring_buffer.fill_level(), 0.0)
