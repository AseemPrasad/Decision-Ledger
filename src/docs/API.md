# API Reference

Public surface comes from `decision_ledger/__init__.py`.

## Gatekeeper

### `class Gatekeeper(policy, exploration_rate=0.02, telemetry=None)`

Decides whether a small model may act for a given context.

- `policy: dict[bytes, CalibrationContext]` — calibrated context snapshot keyed
  by 16-byte context hash; swapped atomically. Build it from a
  `ServingPolicy` via `policy.contexts`.
- `exploration_rate: float` — epsilon for stratified counterfactual sampling
  (`0.0 .. 1.0`); must be within `[0.0, 1.0]`.
- `telemetry: RingBuffer | None` — optional ring buffer where each decision is
  recorded. Off by default (keeps the hot path allocation-free).

Exploration is **deterministic**: the call counter advances per eligible
evaluation and samples every `int(1/rate)`-th call, so the measured
exploration rate matches the parameter and results are reproducible.

#### `evaluate(context_hash: bytes, model_confidence: float, decision_type: str) -> GateAction`

Returns `DELEGATE`, `ESCALATE`, or `EXPLORE_SHADOW`. `decision_type` is a
name from `route`, `judge`, `speculate`, `mutate`, `summarize`, `abstain`
(unknown names warn and proceed as `route`). Confidences are clamped to
`[0.0, 1.0]`; negative/NaN values warn and clamp to `0.0`.

```python
action = gk.evaluate(ctx_hash, 0.92, "route")
if action == GateAction.DELEGATE:
    result = small_model.generate(query)
elif action == GateAction.ESCALATE:
    result = frontier_model.generate(query)
else:
    small = small_model.generate(query)      # log counterfactual
    result = frontier_model.generate(query)  # serve frontier
```

#### `reload_policy(new_policy: dict[bytes, CalibrationContext]) -> None`

Thread-safe policy swap (RLock). Evaluations in flight finish against the old
snapshot; subsequent calls observe the new one. E.g. after hourly
calibration: `gk.reload_policy(load_policy("policies/policy-v1.yaml").contexts)`.

#### `get_metrics() -> dict`

Snapshot: total/delegate/escalate/explore counts, `escalation_rate`,
`exploration_rate`, and a `per_decision_type` breakdown with `calls`,
`escalations`, and `escalation_rate` per type.

### `enum DecisionType`

`ROUTE = 0`, `JUDGE = 1`, `SPECULATE = 2`, `MUTATE = 3`, `SUMMARIZE = 4`,
`ABSTAIN = 5`. The enum mirrors the string names accepted by `evaluate`
(e.g. `DecisionType.ROUTE` corresponds to `"route"`).

### `enum GateAction`

`DELEGATE = 0`, `ESCALATE = 1`, `EXPLORE_SHADOW = 2`.

### `class CalibrationContext(context_hash, q_hat=None, min_sample_size=100, current_sample_size=0, is_active=False)`

> Defined in `decision_ledger.gatekeeper`; re-exported from the package root
> and from `decision_ledger.policy`.

- `context_hash: bytes` — 16-byte BLAKE3 (truncated) context reference.
- `q_hat: float | None` — conformal non-conformity threshold. `None` disables
  delegation for this context (gatekeeper escalates).
- `min_sample_size: int` — samples required before delegation is allowed.
- `current_sample_size: int` — sample count tracked by the calibration pipeline.
- `is_active: bool` — whether the context is ready for delegation.
- `has_enough_data: bool` — `current_sample_size >= min_sample_size`.

## Policy

### `class ServingPolicy(schema_version, version_id, contexts)`

Immutable, versioned snapshot. `contexts: dict[bytes, CalibrationContext]`
keyed by the 128-bit context hash — pass `.contexts` straight to a
`Gatekeeper`.

### `policy_from_results(results, *, version_id, min_sample_size=100) -> ServingPolicy`

Build a policy from `{context_hash: CalibrationResult}`. Contexts without a
`q_hat` stay inactive.

### `save_policy(policy, path) -> Path` / `load_policy(path) -> ServingPolicy`

YAML round-trip. Context hashes serialize as hex; context hashes are the YAML
keys on re-load.

## Calibration

### `class ConformalCalibrator(target_alpha=0.05, min_sample_size=500, confidence_level=0.95)`

#### `compute_threshold(records) -> CalibrationResult`

One context's calibration set. `records` are `CalibrationRecord`s; only
`is_independent and not is_exploratory` count.

#### `calibrate_by_context(records) -> dict[bytes, CalibrationResult]`

Groups by `context_hash` and calibrates each independently.

### `class CalibrationRecord(context_hash, non_conformity_score, loss, *, is_independent=True, is_exploratory=False)`

### `class CalibrationResult(q_hat, sample_size, coverage_lower_bound, achieved_empirical_risk)`

- `q_hat: float | None` — `None` means "do not activate".
- `coverage_lower_bound` — Wilson lower bound on P(correct | delegated).

## Telemetry

### `class RingBuffer(capacity=100_000)`

- `push(record: DecisionRecord) -> bool` — non-blocking, never raises or
  blocks; at capacity the oldest record is evicted (counted in
  `dropped_count`); returns `False` for invalid input or records dropped
  under overload.
- `pop_batch(max_records=1000) -> list[DecisionRecord]` — FIFO drain.
- `size() -> int`, `fill_level() -> float` — current occupancy (0.0-1.0).
- `total_pushed` / `dropped_count` — lifetime accounting properties.
- Backpressure: 50-75% warn once/minute, 75-90% every 10s, 90-95% critical;
  at >=95% (and full), drops — exploratory records first, then the oldest.
  No locks: the CPython GIL makes `deque` append/popleft safe for the
  single-producer contract.

### `class DecisionRecord(..., frozen=True)`

`decision_id` (UUIDv7, 36 chars), `timestamp_ns`, `context_hash` (16 bytes),
`decision_type` (str), `model_confidence`, `non_conformity`
(= `1 - model_confidence`), `action_taken` (`DELEGATE`/`ESCALATE`/
`EXPLORE_SHADOW`, as `str`), `latency_us`.

`from_evaluation(...)` builds one from a gatekeeper result; `to_dict()`
serializes to the JSONL record shape.

## Consumer

### `class BatchConsumer(ring_buffer, output_dir, flush_interval_ms=100, batch_size=10_000, poll_interval_ms=1)`

- `start()` / `stop()` — daemon thread over the ring buffer.
- `drain_now() -> int` — synchronous drain (tests/ops).

Writes JSONL under `output_dir/decisions/date=YYYY-MM-DD/hour=HH/`.

## Outcomes

### `class OutcomeCollector(paths=None)`

- `record(decision_id, outcome_value, *, outcome_source=0, metadata=None)`
- `iter_records()`, `export(path)`, `import_file(path)`

### `class DecisionOutcomeJoiner(outcomes=None)`

- `add_outcome(outcome)`
- `join(decisions) -> list[JoinedRecord]` — only decisions with an outcome.

### `class OutcomeSource`

`HUMAN = 0`, `TASK_METRIC = 1`, `MODEL_VERIFICATION = 2`, `USER_REPORT = 3`.

### `class JoinedRecord(...)`

`decision_id, context_hash, decision_type, model_confidence, non_conformity,
action_taken, outcome_source, outcome_value, decision_timestamp_ns,
outcome_timestamp_ns`. Properties: `latency_delta_ns`, `loss` (0.0/1.0).

## Helpers

### `context_hash(decision_type, *, prompt_template=None, model_id=None, model_weights_sha256=None, quantization_format=None, adapter_config_hash=None, temperature=None) -> bytes`

128-bit context reference; any parameter change yields a new context.

### `decision_id() -> str`

Time-ordered UUIDv7 identifier (36-char lowercase canonical form).

### `now_ns() -> int`

Wall-clock nanoseconds.