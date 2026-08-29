"""calibration_demo.py

Synthesizes a calibration set (scores + independent outcomes), runs Split
Conformal Risk Control, prints the resulting threshold, and writes a versioned
policy artifact to policies/demo-policy.yaml.

Run: python src/examples/calibration_demo.py
"""

import random

from decision_ledger import (
    CalibrationRecord,
    ConformalCalibrator,
    policy_from_results,
)
from decision_ledger.policy import save_policy
from decision_ledger.utils import context_hash

N_RECORDS = 2000
ALPHA = 0.05
MIN_SAMPLES = 200


def build_synthetic_records(ctx: bytes, n: int = N_RECORDS) -> list[CalibrationRecord]:
    rng = random.Random(42)
    records = []
    for _ in range(n):
        score = rng.random()
        loss = 1.0 if score > 0.18 else 0.0  # model degrades at low confidence
        records.append(
            CalibrationRecord(
                context_hash=ctx,
                non_conformity_score=score,
                loss=loss,
                is_independent=True,
                is_exploratory=False,
            )
        )
    return records


def main() -> None:
    ctx = context_hash(
        "judge",
        prompt_template="Rate this answer: {answer}",
        model_id="qwen-2.5-coder-7b-instruct",
        temperature=0.0,
    )

    records = build_synthetic_records(ctx)
    calibrator = ConformalCalibrator(target_alpha=ALPHA, min_sample_size=MIN_SAMPLES)
    result = calibrator.compute_threshold(records)

    print(f"context_hash        : {ctx.hex()}")
    print(f"samples             : {result.sample_size}")
    print(f"q_hat               : {result.q_hat}")
    print(f"empirical risk      : {result.achieved_empirical_risk:.4f}")
    print(f"coverage lower bound: {result.coverage_lower_bound:.4f}")

    policy = policy_from_results(
        {ctx: result}, version_id=1, min_sample_size=MIN_SAMPLES
    )
    out = save_policy(policy, "policies/demo-policy.yaml")
    print(f"policy artifact      : {out}")


if __name__ == "__main__":
    main()
