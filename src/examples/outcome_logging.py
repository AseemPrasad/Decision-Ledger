"""outcome_logging.py

Shows the outcome side of the ledger: decisions flow through the gatekeeper
into the ring buffer, a human/task outcome arrives later, and the joiner
links the two. The joined records are exactly what the calibration engine
consumes.

Run: python src/examples/outcome_logging.py
"""

import random

from decision_ledger import (
    DecisionOutcomeJoiner,
    Gatekeeper,
    OutcomeCollector,
    OutcomeSource,
    RingBuffer,
    policy_from_results,
)
from decision_ledger.calibration import CalibrationResult
from decision_ledger.utils import context_hash


def main() -> None:
    ctx = context_hash(
        "summarize",
        prompt_template="Summarize conversation: {messages}",
        model_id="qwen-2.5-coder-7b-instruct",
    )
    result = CalibrationResult(
        q_hat=0.20,
        sample_size=4000,
        coverage_lower_bound=0.96,
        achieved_empirical_risk=0.02,
    )
    policy = policy_from_results({ctx: result}, version_id=3, min_sample_size=500)
    buffer = RingBuffer(capacity=1024)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)

    rng = random.Random(11)
    for _ in range(500):
        gk.evaluate(ctx, rng.uniform(0.6, 1.0), "summarize")

    decisions = buffer.pop_batch(max_records=10_000)

    # Outcomes trickle in over time, independent of the serving path.
    collector = OutcomeCollector()
    for decision in decisions:
        correct = decision.model_confidence > 0.8
        collector.record(
            decision.decision_id,
            outcome_value=1.0 if correct else 0.0,
            outcome_source=OutcomeSource.TASK_METRIC,
        )

    joiner = DecisionOutcomeJoiner(collector.iter_records())
    joined = joiner.join(decisions)

    print(f"decisions logged   : {len(decisions)}")
    print(f"outcomes collected : {len(list(collector.iter_records()))}")
    print(f"joined records     : {len(joined)}")
    print(f"match rate         : {len(joined) / len(decisions):.2f}")
    if joined:
        sample = joined[0]
        print(
            f"example            : loss={sample.loss}, "
            f"score={sample.non_conformity:.3f}, "
            f"action={sample.action_taken}"
        )


if __name__ == "__main__":
    main()
