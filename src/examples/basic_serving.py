"""basic_serving.py

Minimal serving loop: a "small model" reports confidence for a control-plane
decision, the gatekeeper decides delegate/escalate, and the (stub) large
model takes over when escalated.

Run: python src/examples/basic_serving.py
"""

import random

from decision_ledger import GateAction, Gatekeeper
from decision_ledger.calibration import CalibrationResult
from decision_ledger.policy import policy_from_results
from decision_ledger.utils import context_hash


def main() -> None:
    ctx = context_hash(
        "route",
        prompt_template="SELECT * FROM {table} LIMIT {n};",
        model_id="qwen-2.5-coder-7b-instruct",
        temperature=0.2,
    )

    # A calibration run would produce this; here we hard-code a result so
    # the demo can start without collecting data first.
    result = CalibrationResult(
        q_hat=0.18,
        sample_size=5000,
        coverage_lower_bound=0.972,
        achieved_empirical_risk=0.024,
    )
    policy = policy_from_results({ctx: result}, version_id=1, min_sample_size=500)
    gk = Gatekeeper(policy.contexts, exploration_rate=0.02)

    rng = random.Random(7)
    counts = {action: 0 for action in GateAction}
    for _ in range(2000):
        confidence = rng.uniform(0.4, 1.0)  # small model's confidence
        action = gk.evaluate(ctx, confidence, "route")
        counts[action] += 1

    simulated = Gatekeeper({}, exploration_rate=0.0)
    unknown = simulated.evaluate(b"\x00" * 16, 0.99, "route")

    print(f"Delegated to small model:    {counts[GateAction.DELEGATE]:>6}")
    print(f"Escalated to frontier model: {counts[GateAction.ESCALATE]:>6}")
    print(f"Shadow exploration:          {counts[GateAction.EXPLORE_SHADOW]:>6}")
    print(f"Unknown context action:      {unknown.name} (fail-closed)")


if __name__ == "__main__":
    main()
