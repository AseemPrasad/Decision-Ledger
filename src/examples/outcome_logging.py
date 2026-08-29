"""outcome_logging.py

Shows the outcome side of the ledger: decisions flow through the gatekeeper
into the ring buffer, a task-metric outcome arrives later and is persisted to
SQLite, and the joiner links decisions to outcomes. Joined records are exactly
what the calibration engine consumes.

Run: python src/examples/outcome_logging.py
"""

import random
from pathlib import Path

from decision_ledger import (
    Database,
    DecisionOutcomeJoiner,
    Gatekeeper,
    OutcomeCollector,
    OutcomeRecord,
    OutcomeSource,
    RingBuffer,
    policy_from_results,
)
from decision_ledger.calibration import CalibrationResult
from decision_ledger.utils import context_hash


def _outcome_record(row: dict) -> OutcomeRecord:
    return OutcomeRecord(
        decision_id=row["decision_id"],
        outcome_timestamp_ns=row["timestamp_ns"],
        outcome_source=OutcomeSource(row["outcome_source"]),
        outcome_value=row["outcome_value"],
        metadata=row.get("metadata"),
    )


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
    ledger = Database(str(Path.cwd() / "ledger_outcome_demo.db"))
    try:
        collector = OutcomeCollector(ledger)
        outcome_ids = collector.log_outcomes_batch(
            [
                {
                    "decision_id": decision.decision_id,
                    "outcome_value": 1.0 if decision.model_confidence > 0.8 else 0.0,
                    "source": OutcomeSource.TASK_METRIC,
                }
                for decision in decisions
            ]
        )
        outcomes = [_outcome_record(row) for row in ledger.get_outcomes()]
    finally:
        ledger.close()

    joiner = DecisionOutcomeJoiner(outcomes)
    joined = joiner.join(decisions)

    print(f"decisions logged   : {len(decisions)}")
    print(f"outcome ids logged : {len(outcome_ids)}")
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
