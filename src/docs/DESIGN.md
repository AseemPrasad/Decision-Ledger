# Design: Decision Ledger (MVP)

## Problem

Small models (1–7B) increasingly handle *control-plane* decisions (route,
judge, speculate, mutate, summarize, abstain) to cut latency and cost. The
trust gap: **how do we know when a small model is safe to trust?**

Fixed-confidence heuristics fail because small models exhibit logit
compression (predictions cluster at [0.98, 1.0]), calibration drifts with
model/prompt changes, and selection bias hides failures once low-confidence
decisions are recycled out of measurement.

## Approach

Replace intuition with measurement, backed by a formal finite-sample
guarantee from **Split Conformal Risk Control (CRC)**:

```
P(Loss(Decision, GroundTruth) > 0) <= alpha
```

for a user-specified risk budget `alpha`, under exchangeably sampled
calibration data.

## Core loop

```
                 ┌─────────────────────────────┐
   request ────▶ │  Gatekeeper.evaluate()      │  hot path, fail-closed
                 │  S = 1 - model_confidence   │
                 │  S <= q_hat ?                │
                 └──────┬────────────┬─────────┘
                  delegate        escalate
                    │                │
                    ▼                ▼
              small model      frontier model
                    │                │
                    └───────┬────────┘
                            ▼
                ┌─────────────────────┐
                │   RingBuffer        │  every evaluation logged
                └──────────┬──────────┘
                           ▼  (async)
                ┌─────────────────────┐
                │  BatchConsumer      │  writes JSONL (MVP) / Parquet (prod)
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │  Outcome Collector  │  independent outcomes by decision_id
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │  ConformalCalibrator│  computes q_hat per context
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │  Policy Artifact    │  versioned YAML (q_hat + samples)
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │  gatekeeper reload  │  atomic policy swap
                └─────────────────────┘
```

## Components

### Gatekeeper (`gatekeeper.py`)

The only component on the serving path. A deterministic, allocation-free
function of the current context dict (swapped atomically).

```
evaluate(context_hash, model_confidence, decision_type) -> GateAction
```

Order of checks (each is a fail-closed accelerator):

1. Context not in policy  -> `ESCALATE`
2. Context inactive or below `min_sample_size` -> `ESCALATE`
3. `q_hat` missing         -> `ESCALATE`
4. Exploration lottery (deterministic, one in every `int(1/rate)` calls)
   -> `EXPLORE_SHADOW`
5. `1 - confidence <= q_hat` -> `DELEGATE`, else `ESCALATE`

A `threading.RLock` guards the policy reference: evaluations hold it briefly,
reloads take it to swap snapshots. When a ring buffer is attached, every
evaluation is appended (decision_id, context hash, confidence, action,
latency); without one the decision path allocates nothing.

### Telemetry (`telemetry.py`)

`DecisionRecord` (frozen): `decision_id` (UUIDv7 str), `timestamp_ns`,
`context_hash`, `decision_type`, `model_confidence`, `non_conformity`,
`action_taken`, `latency_us`. `RingBuffer` wraps a `deque(maxlen=capacity)`
(default 100k): non-blocking `push` that wraps at capacity and counts every
eviction/drop in `dropped_count`, batch `pop_batch`, and `size`/`fill_level`.
Backpressure follows the production spec: quiet below 50% fill, throttled
warnings through 50/75/90%, and above 95% (when truly full) drops --
exploratory records first, then the oldest, so escalation/delegation records
survive overload longer. No lock is needed under the CPython GIL for the
single-producer contract.

### Consumer (`consumer.py`)

Background thread that drains the ring buffer and flushes newline-delimited
JSON partitioned by date/hour under `output_dir/decisions/`. Replaces the
production Parquet/S3 writer with zero extra dependencies.

### Outcomes (`outcomes.py`)

`OutcomeRecord` links an independent result (human=0, task_metric=1,
model_verification=2, user_report=3) to a past decision by `decision_id`.
`DecisionOutcomeJoiner` produces `JoinedRecord`s (the calibration input).

### Calibration (`calibration.py`)

Split Conformal Risk Control per context:

- only **independent, non-exploratory** records calibrate the live threshold;
- sort by non-conformity score, take empirical risk over increasing prefixes;
- `q_hat` = largest score whose prefix risk `<= alpha`;
- Wilson lower bound reports finite-sample confidence in coverage;
- below `min_sample_size`, no threshold is emitted (fail-closed).

### Policy (`policy.py`)

`ServingPolicy` is an immutable, versioned snapshot of `CalibrationContext`s.
Contexts are keyed by a 128-bit hash of:

```
prompt_template + model weights + quantization + adapter + temperature
+ decision_type
```

Any change produces a *new* context -> no samples -> `DRAINING` ->
gatekeeper escalates until re-calibrated. YAML artifacts round-trip through
`save_policy` / `load_policy`.

## Guarantees and non-guarantees

Guaranteed:
- Probability of an incorrect *delegated* decision `<= alpha` (finite-sample,
  subject to exchangeability and an independent outcome source).
- Fail-closed behavior by default: unknown context / insufficient data always
  escalates.
- Decisions are auditable: every record has `decision_id`, context hash,
  model-valued confidence, action, and linked outcome.

Not guaranteed:
- Quality of the small model's *generated content* (this governs control-plane
  decisions only).
- Real-time drift detection (batch, configurable latency).
- Protection against adversarial attacks on the small model itself.

## Deliberate MVP trade-offs vs. production blueprint

| Blueprint (production)          | MVP                              |
| ------------------------------- | -------------------------------- |
| Rust gatekeeper + FFI           | Python dataclass implementation  |
| mmap shared-memory ring buffer  | in-process list-backed buffer    |
| Parquet + S3                    | JSONL on local disk              |
| RCU / hazard pointers           | atomic attribute swap            |
| e-vector exploration, drift     | fixed `exploration_rate` dice    |
| K8s/job monitoring              | pytest + examples                |

The interfaces (`evaluate`, `reload_policy`, `compute_threshold`,
`policy_from_results`) are shaped so the production pieces can be swapped in
without changing callers.