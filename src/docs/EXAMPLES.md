# Examples

Three runnable scripts demonstrate each side of the ledger.

## 1. `basic_serving.py` — hot-path enforcement

```bash
python src/examples/basic_serving.py
```

Simulates 2,000 routing decisions against a calibrated context and prints how
often the small model was delegated to, escalated, or shadow-explored — plus
the fail-closed behavior for an unknown context.

## 2. `calibration_demo.py` — conformal calibration

```bash
python src/examples/calibration_demo.py
```

Builds a synthetic calibration set where the model degrades below confidence
0.82, runs Split Conformal Risk Control, prints `q_hat`, the achieved
empirical risk, and the Wilson coverage lower bound, then writes a versioned
policy artifact to `policies/demo-policy.yaml`.

## 3. `outcome_logging.py` — outcomes and joining

```bash
python src/examples/outcome_logging.py
```

Simulates 500 `summarize` decisions through a real gatekeeper + ring buffer,
attaches task-metric outcomes by `decision_id`, joins them, and reports the
match rate.

## Putting it together

```python
from decision_ledger import (
    ConformalCalibrator, DecisionOutcomeJoiner, Gatekeeper, OutcomeCollector,
    RingBuffer, context_hash, policy_from_results,
)
from decision_ledger.calibration import CalibrationRecord
from decision_ledger.policy import load_policy, save_policy

ctx = context_hash("route", prompt_template="...", model_id="...")

# 1. log decisions
buffer = RingBuffer(capacity=1 << 16)
gk = Gatekeeper(policy_from_results({}, version_id=0).contexts, telemetry=buffer)
action = gk.evaluate(ctx, 0.93, "route")

# 2. later, collect an independent outcome
outcomes = OutcomeCollector()
outcomes.record(b"decision-id-16-bytes", outcome_value=1.0)

# 3. join + calibrate
decisions = buffer.pop_batch()
joined = DecisionOutcomeJoiner(outcomes.iter_records()).join(decisions)
records = [
    CalibrationRecord(j.context_hash, j.non_conformity, j.loss) for j in joined
]
result = ConformalCalibrator(target_alpha=0.05, min_sample_size=500)\
    .compute_threshold(records)

# 4. publish + reload
policy = policy_from_results({ctx: result}, version_id=42)
save_policy(policy, "policies/policy-v42.yaml")
gk.reload_policy(load_policy("policies/policy-v42.yaml").contexts)
```