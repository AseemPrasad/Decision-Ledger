# API Reference

Public surface comes from `decision_ledger/__init__.py`.

## Gatekeeper

### `class Gatekeeper(policy=None, policy_file=None, exploration_rate=0.02, telemetry=None)`

Decides whether a small model may act for a given context.

- `policy: dict[bytes, CalibrationContext]` — calibrated context snapshot keyed
  by 16-byte context hash; swapped atomically. Build it from a
  `ServingPolicy` via `policy.contexts`. Omit to start fail-closed (every
  context escalates).
- `policy_file: str | None` — optional schema-1.0 artifact YAML to load at
  construction; takes precedence over `policy`.
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
calibration:
`gk.reload_policy(policy_from_dict(load_policy("policies/policy-v1.yaml")).contexts)`.

#### `from_policy_file(policy_file: str, exploration_rate=0.02) -> Gatekeeper` *(classmethod)*

Load a schema-1.0 artifact YAML and build a gatekeeper from it. Logs
`[Loaded policy: version=<version>, contexts=<n>]` and raises
`FileNotFoundError` / `PolicyValidationError` for missing or invalid files.

#### `reload_policy_from_file(policy_file: str) -> None`

Reload from a schema-1.0 artifact YAML, swapping atomically. Logs the version
transition: `[Reloaded policy: <old> -> <new>]`.

#### `convert_policy_dict_to_contexts(policy_dict: dict) -> dict[bytes, CalibrationContext]` *(staticmethod)*

Convert an artifact dict into gatekeeper contexts: `context_ref` hex is
decoded to the 16-byte key, `min_sample_size` comes from
`global.min_sample_size_default`, and `is_active` is `True` only when
`state == "ACTIVE"` (`DRAINING` / `REVOKED` contexts stay present but fail
closed).

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

Build a serving policy from `{context_hash: CalibrationResult}`. Contexts
without a `q_hat` stay inactive.

### `class PolicyGenerator(policies_dir="data/policies", *, default_alpha=0.05, fail_closed=True, exploration_rate=0.02, min_sample_size_default=100, revoked_contexts=frozenset(), logger=None)`

Produces the versioned, validated YAML artifacts shipped to gatekeepers.

- `generate_policy(calibration_results, *, policy_version=None, force=False) -> str`
  — writes `policy_<YYYYMMdd-HHMMSS>.yaml` (timestamp version default) and
  repoints `policy_latest.yaml` at it. Context state is `ACTIVE` (`q_hat` set
  and `sample_size >= min_sample_size`), `DRAINING` (`q_hat` `None` or below
  the minimum sample), or `REVOKED` (context present in `revoked_contexts`,
  set outside this function). On Windows without symlink privileges the
  `policy_latest.yaml` link falls back to a plain file copy.
- `load_latest_policy() -> dict` — load the current `policy_latest.yaml`.
- `get_policy_history() -> List[str]` — generated versions on disk, newest
  first.
- `rollback_policy(target_version) -> str` — republish an older artifact as
  latest; returns the published path.

### `validate_policy(artifact) -> bool`

Validates a schema-1.0 artifact dict. Always returns `True` or raises
`PolicyValidationError`, so invalid artifacts cannot pass unnoticed.

### `load_policy(path) -> dict` / `policy_from_dict(artifact) -> ServingPolicy`

`load_policy` parses and validates a schema-1.0 YAML artifact, returning the
raw dict. `policy_from_dict` converts it back into a `ServingPolicy`,
**dropping `REVOKED` contexts** (the gatekeeper then fails closed for them):

```python
gk.reload_policy(policy_from_dict(load_policy("policies/policy-v1.yaml")).contexts)
```

### `save_policy(policy, path) -> Path`

Writes a `ServingPolicy` as a validated schema-1.0 artifact (legacy
convenience; use `PolicyGenerator` when you want versioning, history, and
rollback).

### Artifact format (schema_version `"1.0"`)

```yaml
schema_version: "1.0"
policy_version: "20260830-153000"        # YYYYMMdd-HHMMSS
generated_at: "2026-08-30T15:30:00Z"
global:
  default_alpha: 0.05
  fail_closed: true
  exploration_rate: 0.02
  min_sample_size_default: 100
contexts:
  - context_ref: "<32-hex>"
    state: ACTIVE            # ACTIVE | DRAINING | REVOKED
    q_hat: 0.05              # present when set (required for ACTIVE)
    sample_size: 500
    min_sample_size: 100
```

## Calibration

### `class ConformalCalibrator(database=None, target_alpha=0.05, min_sample_size=100, confidence_level=0.95)`

`database: Database | None` — when a `Database` is provided, the live
(`calibrate_context`, `detect_drift`) engine can be used; with no database the
offline methods below still work on `CalibrationRecord` inputs. `z_score = 1.96`
(a 95% confidence interval).

`target_alpha` must be in `(0.0, 1.0)`, `min_sample_size >= 1`, and
`confidence_level` in `(0.0, 1.0)`.

#### `calibrate_context(context_hash: bytes) -> CalibrationResult`

Calibrate one live context from the durable `joined_records` table. Reads every
joined decision for the context that has an outcome, excludes `EXPLORE_SHADOW`
records and any decision whose outcomes are not independent
(`model_verification` only), then computes `q_hat`.

Raises `ValueError` for an invalid context hash or a calibrator without a
database; `DatabaseError` on store failure.

#### `detect_drift(context_hash: bytes) -> dict`

Compares accuracy on the full confidence range (`EXPLORE_SHADOW` records)
against the active delegation range (`DELEGATE` records). Returns
`{"drift_detected", "full_range_accuracy", "active_range_accuracy", "divergence"}`;
flags drift when the absolute divergence exceeds 5%
(`DRIFT_DIVERGENCE_THRESHOLD`). Accuracy on an absent range is `0.0`.

#### `compute_threshold(records) -> CalibrationResult`

One context's calibration set. `records` are `CalibrationRecord`s; only
`is_independent and not is_exploratory` count.

#### `calibrate_by_context(records) -> dict[bytes, CalibrationResult]`

Groups by `context_hash` and calibrates each independently.

#### `_wilson_interval_lower(p, n) -> float` / `wilson_lower_bound(p, n) -> float`

Wilson score interval lower bound for coverage `p` over `n` samples (clamped to
`>= 0.0`). `wilson_lower_bound` is the backward-compatible public alias.

### `class CalibrationRecord(context_hash, non_conformity_score, loss, *, is_independent=True, is_exploratory=False)`

### `class CalibrationResult(q_hat, sample_size, coverage_lower_bound, achieved_empirical_risk, min_observed_loss=0.0, max_observed_loss=0.0)`

- `q_hat: float | None` — `None` means "do not activate" (insufficient data or
  no threshold within the risk budget).
- `coverage_lower_bound` — Wilson lower bound on P(correct | delegated).
- `min_observed_loss` / `max_observed_loss` — span of losses in the calibration
  set (both `0.0` when every decision succeeded).

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

### `class BatchConsumer(ring_buffer, database, flush_interval=5.0, batch_size=5_000, daemon=True)`

A `threading.Thread` (daemon by default) that polls `RingBuffer.pop_batch`
(max 1000 per poll, 100ms cadence) and flushes the accumulated batch to the
SQLite `decisions` table via `Database.batch_insert` — the durable copy of
the ledger.

- Flush triggers: `batch_size` records accumulated, or `flush_interval`
  seconds since the last successful flush.
- Failure handling: a failed flush is retried twice with a short backoff,
  then buffered in memory (100k cap; the oldest are dropped and counted once
  the cap is hit; a critical alert fires above 50k).
- `start()` — inherited from `Thread` (single-shot; errors on restart).
- `stop(timeout=10.0)` — signals shutdown, joins the loop thread, then does a
  final flush of whatever remains.
- `get_metrics() -> dict` — `total_records_processed`, `total_records_flushed`,
  `total_records_dropped`, `total_flushes`, `last_flush_time`,
  `avg_flush_time_ms`, `backlog_records`.

`JsonlExport` (`JsonlExport(ring_buffer, output_dir, flush_interval_ms=100,
batch_size=10_000, poll_interval_ms=1)`) is the same poll/flush shape but
writes ad-hoc date/hour-partitioned JSONL:

- `start()` / `stop()` — daemon thread over the ring buffer.
- `drain_now() -> int` — synchronous drain (tests/ops).

Writes JSONL under `output_dir/decisions/date=YYYY-MM-DD/hour=HH/`.

## Outcomes

### `class OutcomeSource(Enum)`

String values, matched 1:1 to the schema TEXT column:

- `HUMAN = "human"`
- `TASK_METRIC = "task_metric"`
- `MODEL_VERIFICATION = "model_verification"`
- `USER_REPORT = "user_report"`

### `class OutcomeCollector(database: Database)`

Validates every outcome (decision exists, value in `[0.0, 1.0]`, known source,
JSON metadata) and persists it to the `outcomes` table via `batch_insert`;
each record is stamped with a UUIDv7 `outcome_id`.

- `log_outcome(decision_id, outcome_value, outcome_source, metadata="") -> str`
  — logs one outcome for an existing decision; returns the generated
  `outcome_id`. Raises `DecisionNotFoundError`, `InvalidOutcomeValueError`,
  `InvalidOutcomeSourceError` or `InvalidMetadataError` on bad input.
- `log_outcomes_batch(records) -> list[str]` — all-or-nothing batch; each
  dict needs `decision_id`, `outcome_value` and `source` (or `outcome_source`),
  plus optional `metadata`. Any invalid record rejects the whole batch.
- `get_outcome(outcome_id) -> dict | None`
- `get_outcomes_for_decision(decision_id) -> list[dict]` — oldest first.
- `get_metrics() -> dict` — `outcomes_logged`, `batches_logged`, `last_logged_at`.

Row dicts carry the TEXT `outcome_source` and `metadata` as a JSON string (or
`None`).

### CLI

```bash
python -m decision_ledger.outcomes --decision-id <uuid> \
    --outcome-value <0.0-1.0> --source <human|task_metric|model_verification|user_report> \
    [--metadata '<json>'] [--db ledger.db]
```

Exit codes: `0` success, `2` usage/validation error, `3` database error.

### `class InMemoryOutcomeCollector(paths=None)`

Offline collector for demos and tests (no database); validates with the same
rules as `OutcomeCollector`:

- `record(decision_id, outcome_value, *, outcome_source=..., metadata=None)`
- `iter_records()`, `export(path)`, `import_file(path)`

### `class DecisionOutcomeJoiner(outcomes=None)`

- `add_outcome(outcome)`
- `join(decisions) -> list[JoinedRecord]` — only decisions with an outcome.

### `class JoinedRecord(...)`

`decision_id, context_hash, decision_type, model_confidence, non_conformity,
action_taken, outcome_source, outcome_value, decision_timestamp_ns,
outcome_timestamp_ns`. Properties: `latency_delta_ns`, `loss` (0.0/1.0).

## Joining (`database.py`)

`Joiner` materializes the `joined_records` table with one `INSERT ... SELECT`
LEFT JOIN over `decisions` and `outcomes`. `joined_id` equals the source
`decision_id`, so `ON CONFLICT(joined_id) DO NOTHING` makes re-runs
idempotent (a decision is joined at most once).

### `class Joiner(database: Database)`

- `database.joiner` — property on `Database` returning a bound `Joiner`.
- `join_decisions_and_outcomes(start_time=None, end_time=None) -> int` —
  joins decisions to outcomes (LEFT JOIN; unmatched decisions get a row with
  `outcome_value = NULL`) and returns the number of rows actually inserted.
  Optional strict time bounds: `d.timestamp_ns > start_time`,
  `d.timestamp_ns < end_time`.
- `get_joined_records(...)` / `get_join_statistics()` — delegate to the
  `Database` methods below.

### `Database.get_joined_records(context_hash=None, include_unmatched=True) -> list[dict]`

Rows from `joined_records`, oldest decision first. `context_hash` filters by
raw 16-byte hash; `include_unmatched=False` drops rows with
`outcome_value IS NULL`.

### `Database.get_join_statistics() -> dict`

```
{"total_decisions": int, "total_outcomes": int,
 "joined_count": int, "match_rate": float}
```

`joined_count` is the number of distinct decisions with at least one outcome
(from the `outcomes` table, so it is accurate even before a join runs);
`match_rate = joined_count / total_decisions` (0.0 on an empty ledger).

## Helpers

### `context_hash(decision_type, *, prompt_template=None, model_id=None, model_weights_sha256=None, quantization_format=None, adapter_config_hash=None, temperature=None) -> bytes`

128-bit context reference; any parameter change yields a new context.

### `decision_id() -> str`

Time-ordered UUIDv7 identifier (36-char lowercase canonical form).

### `now_ns() -> int`

Wall-clock nanoseconds.