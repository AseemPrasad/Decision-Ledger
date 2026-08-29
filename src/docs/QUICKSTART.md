# Decision Ledger — 5-Minute Quickstart

This gets you a working gatekeeper with a logged audit trail in five minutes.
All snippets are runnable as-is; the first one is a single script.

## 1. Setup (~2 min)

```bash
python -m venv venv
venv\Scripts\activate            # Windows   (source venv/bin/activate on macOS/Linux)
pip install -r requirements.txt  # runtime + dev tools
pip install -e .                 # imports the `decision_ledger` package
pytest                           # sanity: everything should pass (incl. benchmarks)
```

You now have Python 3.14 (recommended: built-in `uuid.uuid7`), the `blake3`
and `uuid6` packages, numpy, and the test toolchain.

## 2. Hello World (~1 min)

A calibrated context, an attached ring buffer, 50 evaluations, and the audit
trail printed back out:

```python
import time
from collections import Counter

from decision_ledger import Gatekeeper, RingBuffer, context_hash, policy_from_results
from decision_ledger.calibration import CalibrationResult

# The serving context. Change ANY of these inputs -> a brand-new context that
# fails closed until recalibrated.
ctx = context_hash(
    "route",
    prompt_template="SELECT * FROM {t} LIMIT {n};",
    model_id="qwen-7b",
    temperature=0.2,
)

# What calibration would produce (see calibration_demo.py for the real thing).
result = CalibrationResult(
    q_hat=0.18,
    sample_size=5000,
    coverage_lower_bound=0.972,
    achieved_empirical_risk=0.024,
)
policy = policy_from_results({ctx: result}, version_id=1, min_sample_size=100)

# Everything on the serving path:
buffer = RingBuffer(capacity=1024)
gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=buffer)

for i in range(50):
    confidence = 0.62 + (i % 40) / 100.0          # simulated small-model confidence
    gk.evaluate(ctx, confidence, "route")

print(gk.get_metrics())                           # counters, escalation/exploration rate

records = buffer.pop_batch()                      # FIFO drain of the audit trail
print(f"drained {len(records)} records; buffer now holds {buffer.size()}")
for r in records[:5]:
    print(r.decision_id, r.action_taken, f"{r.model_confidence:.2f}", f"{r.latency_us}us")
```

## 3. Measure latency

Wrap `evaluate()` in `time.perf_counter_ns` and summarize with numpy (this is
exactly what `test_high_frequency_evaluation_logs_every_decision` asserts
against: p99 must stay under 1 ms). Give the buffer **headroom** — the quoted
reference numbers are for a buffer that is far from full; a saturated buffer
that is dropping pays drop-tier overhead:

```python
import time
import numpy as np

lat_buf = RingBuffer(capacity=100_000)          # headroom for 5k evals
lat_gk = Gatekeeper(policy.contexts, exploration_rate=0.0, telemetry=lat_buf)

latency_us = []
for i in range(5_000):
    start = time.perf_counter_ns()
    lat_gk.evaluate(ctx, 0.85 + (i % 10) / 1000.0, "route")
    latency_us.append((time.perf_counter_ns() - start) / 1000.0)

print(f"p50={np.percentile(latency_us, 50):.1f}us  "
      f"p99={np.percentile(latency_us, 99):.1f}us  "
      f"mean={np.mean(latency_us):.1f}us")
```

Reference numbers on the dev box (Ryzen 3 5300U, Python 3.14): p50 ≈ 7.7 µs,
p99 ≈ 26.6 µs with telemetry attached; ~1.05 µs mean without it. The budget is
1 ms, so there is a huge margin. Track regressions in CI with the benchmark
tests:

```bash
pytest src/tests -k benchmark        # single-eval < 1ms, pop_batch < 100us
```

## 4. Inspect decisions

Three ways, cheapest to most durable:

**In-process** (what Hello World does): `buffer.pop_batch()` returns
`DecisionRecord` objects; `record.to_dict()` gives the JSONL-ready dict:

```python
records = buffer.pop_batch(max_records=1000)
print(records[0].to_dict())
# {'decision_id': '018d...-3a90-71b6-...', 'timestamp_ns': ..., 'context_hash': 'a3...',
#  'decision_type': 'route', 'model_confidence': 0.97, 'non_conformity': 0.03,
#  'action_taken': 'DELEGATE', 'latency_us': 6}
```

**Durable JSONL** via the background consumer — it drains off the hot path
and writes partition directories under `output_dir/decisions/date=…/hour=…/`:

```python
from decision_ledger.consumer import BatchConsumer

consumer = BatchConsumer(ring_buffer=buffer, output_dir="ledger-out")
consumer.start()
# ... keep serving ... (evaluate() calls append to the buffer, never block on disk)
consumer.stop()                      # flush + stop the daemon thread
consumer.drain_now()                 # or: synchronous drain at any time
```

**Roll-up metrics** from the gatekeeper itself (does not touch the buffer):

```python
m = gk.get_metrics()
print(m["delegate"], m["escalate"], m["explore"], m["escalation_rate"])
print(m["per_decision_type"]["route"])
```

> `buffer.dropped_count` is the number to alarm on: >0 while you care about
> completeness means the consumer is not keeping up. See
> [API_TELEMETRY.md](API_TELEMETRY.md) for the drop semantics.

## Next steps

- [ARCHITECTURE_WEEK1.md](ARCHITECTURE_WEEK1.md) — how it all fits together
- [API_GATEKEEPER.md](API_GATEKEEPER.md) and [API_TELEMETRY.md](API_TELEMETRY.md)
- [EXAMPLES.md](EXAMPLES.md) — `basic_serving.py`, `calibration_demo.py`, `outcome_logging.py`