# Week 1 Architecture: Gatekeeper + Ring Buffer

The Week 1 slice is the **trust gate** of the Decision Ledger: every
small-model control-plane decision is scored, gated, and logged with zero
pathological latency, and no decision is ever silently lost while the buffer
has capacity.

> Companion docs: [DESIGN.md](DESIGN.md) (problem + guarantees),
> [API_GATEKEEPER.md](API_GATEKEEPER.md), [API_TELEMETRY.md](API_TELEMETRY.md),
> [BENCHMARKS.md](BENCHMARKS.md) (measured budgets).

## Data flow

```
 request ──▶ ┌──────────────────────────────┐
             │   Gatekeeper.evaluate()      │   hot path, fail-closed
             │   S = 1 - model_confidence   │
             │   eligible & S <= q_hat ?    │
             └──────┬────────────┬──────────┘
        delegate   │            │ escalate
                  ▼            ▼
            small model─┐  frontier model
                  └────┴────┐
            (EXPLORE_SHADOW │ runs BOTH; frontier serves)
                           ▼  _record()               [telemetry attached]
                   ┌───────────────┐
                   │   RingBuffer  │   ──▶ push(DecisionRecord)  per evaluation
                   └───────┬───────┘       UUIDv7 id, latency_us inside
                           │    pop_batch(1000) on ~1ms poll, background thread
                           ▼
                  ┌──────────────────────┐
                  │    BatchConsumer     │──▶ decisions/date=…/hour=…/*.jsonl
                  └───────┬──────────────┘
                          │   outcomes arrive later (off-path)
                          ▼
                 ┌────────────────┐
                 │   Calibrator   │──▶ ServingPolicy YAML (q_hat per context)
                 └───────┬────────┘
                         │  reload_policy(new_policy)  — atomic swap, off-path
                         ▼
                 Gatekeeper.policy  (new snapshot served from here on)
```

## Component roles

| Component | File | Role |
| --- | --- | --- |
| `Gatekeeper` | `gatekeeper.py` | The **only** object a request touches. Maps a 16-byte context hash + confidence to `DELEGATE` / `ESCALATE` / `EXPLORE_SHADOW`. Fail-closed: unknown context, inactive context, insufficient samples, or a missing `q_hat` all escalate. |
| `DecisionRecord` | `telemetry.py` | Frozen, `slots=True` dataclass: one immutable record per evaluation (UUIDv7 id, timestamp, context hash, confidence, action, measured latency). |
| `RingBuffer` | `telemetry.py` | Bounded single-producer buffer (`deque(maxlen=capacity)`). `push` never blocks or raises; `pop_batch` drains FIFO. Explicit backpressure tiers warn before overload; drops are **counted**, never a crash. |
| `BatchConsumer` | `consumer.py` | Background daemon thread that polls `pop_batch` and writes newline-delimited JSON. Keeps the producer off the disk. |
| `CalibrationContext` / `ServingPolicy` | `gatekeeper.py`, `policy.py` | Per-context operating envelope (`q_hat`, sample size, active flag) shipped as a versioned YAML artifact and swapped into the gatekeeper atomically. |
| `utils.context_hash` | `utils.py` | Distills prompt/model/weights/quantization/adapter/temperature into a 16-byte context key. Change any factor → brand-new context → gatekeeper fails closed until recalibrated. |

## Latency budget and how it is met

**Budget:** a single `evaluate()` is sub-microsecond amortized and must never
exceed 1 ms; draining 1000 records from the buffer must stay under 100 µs
(see [BENCHMARKS.md](BENCHMARKS.md)).

**Achieved on this machine (Ryzen 3 5300U, Python 3.14):**

| Path | Measured |
| --- | --- |
| `evaluate()` with no telemetry | ~1.05 µs mean |
| `evaluate()` + ring buffer (10k loop) | p50 7.7 µs, p99 26.6 µs |
| `pop_batch(1000)` drain | ~53–72 µs |
| + consumer-side teardown of a discarder | another ~110 µs |

**How the budget holds up:**

1. **The decision path allocates nothing.** `evaluate()` is dict lookups,
   integer increments, and float comparisons against pre-existing singletons
   (`gatekeeper.py:121-131` initializes all counters once in
   `__post_init__`).
2. **Telemetry is opt-in.** The ring buffer is off by default; only when
   attached does the call build a `DecisionRecord` (one UUIDv7, one frozen
   dataclass, one deque append).
3. **The buffer is O(1) per record.** `deque` appends and `popleft`s do not
   reallocate; `DecisionRecord` is a `slots=True` dataclass so teardown stays
   cheap (≈15 µs/1000 saved at drain time).
4. **The consumer is off the serving path.** Draining happens on a daemon
   thread; the producer only ever does an append.
5. **Measured budgets are loose against reality.** The 1 ms / 100 µs budgets
   are ~40× / ~1.4× the measured numbers, so regressions surface as test
   failures long before they are user-visible.

## Thread safety model

- **A single `threading.RLock` guards the gatekeeper state.** The policy
  dict reference, the call/exploration counters, and the per-type metrics all
  live under one lock (`gatekeeper.py:168`). Evaluations hold it only for
  their microsecond lifetime.
- **`reload_policy` takes the same lock** (`gatekeeper.py:245`) to swap in a
  new dict. Evaluations already in flight finish against the old snapshot;
  every subsequent evaluation observes the new one. No RCU or lock-free
  tricks are needed under the CPython GIL.
- **The counter is updated before the checks**, so the deterministic
  exploration cadence is exact even under concurrency — every eligible call
  is counted exactly once.
- **The ring buffer is lock-free by contract.** `deque` operations are atomic
  under the GIL, so a single producer can `push` while a consumer
  `pop_batch`es without a lock. The design assumes **one producer** (the
  gatekeeper, itself serialized by its RLock).
- **Metrics are read under the lock** and copied as plain ints; the returned
  dict is a fresh snapshot, safe to hold.

## Exploration strategy

Exploration is **deterministic stratified epsilon sampling** instead of a
PRNG (`gatekeeper.py:223-237`):

- The gatekeeper keeps an atomic call counter that advances **only for
  eligible calls** (active context, enough data, valid `q_hat`).
- Eligible call `n` explores when `n % int(1.0 / exploration_rate) == 0`. At
  `exploration_rate=0.02` exactly one call in fifty — 2% — is shadowed.
- Results are fully reproducible (no random state) and the measured
  exploration rate matches the parameter exactly.

**What happens on `EXPLORE_SHADOW`:** both the small model and the frontier
model run; the **frontier result serves the request** and the small model's
counterfactual is logged for calibration. Because shadow samples never affect
the live answer, exploration carries no user risk, and it removes the
selection bias that would otherwise hide failures once low-confidence
decisions stop being served. Calibration deliberately **excludes** shadow
records (`CalibrationRecord.is_exploratory`, `calibration.py:34,67`) — shadow
does not pollute the risk estimate.