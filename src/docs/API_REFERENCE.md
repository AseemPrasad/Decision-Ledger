# API Reference — Decision Ledger

The Decision Ledger is a systems primitive for an old question: *when is a
small model safe to trust for a control-plane decision?* It records every
decision, links it to independent outcomes, derives per-context confidence
thresholds with Split Conformal Risk Control, and enforces them through a
fail-closed gatekeeper.

This guide documents the **entire public surface** from the newcomer's point
of view. Every component is also documented in depth in
[`API.md`](API.md), [`API_GATEKEEPER.md`](API_GATEKEEPER.md) and
[`API_TELEMETRY.md`](API_TELEMETRY.md), and worked, runnable programs live in
`src/examples/` (`python src/examples/basic_serving.py`).

**Contents**

- [Install & import](#install--import)
- [DecisionLedger (the orchestrator)](#decisionledger-the-orchestrator)
  - [Constructor](#constructor)
  - [`evaluate()` — the serving hot path](#evaluate--the-serving-hot-path)
  - [`log_outcome()` — attach a label](#log_outcome--attach-a-label)
  - [`calibrate()` — learn thresholds](#calibrate--learn-thresholds)
  - [`stats()` — operational snapshot](#stats--operational-snapshot)
  - [`shutdown()` and context management](#shutdown-and-context-management)
  - [Public attributes](#public-attributes)
- [Helper functions](#helper-functions)
- [Data structures](#data-structures)
- [Outcome sources and exceptions](#outcome-sources-and-exceptions)
- [CLI: logging an outcome](#cli-logging-an-outcome)
- [Performance notes](#performance-notes)
- [Common mistakes](#common-mistakes)

---

## Install & import

Install the package (it lives under `src/`; examples and docs assume you run
from the repo root, where `pyproject.toml` puts `src` on `PYTHONPATH`):

```bash
pip install -e .
python -c "import decision_ledger; print(decision_ledger.__version__)"   # 0.1.0
```

Everything in this document imports from the top-level package:

```python
from decision_ledger import (
    DecisionLedger,
    make_context_hash,
    generate_uuidv7,
    now_ns,
    now_us,
    GateAction,
    CalibrationContext,
    DecisionRecord,
    CalibrationResult,
    OutcomeSource,
)
```

---

## DecisionLedger (the orchestrator)

`DecisionLedger` wires every component — SQLite `Database`, telemetry
`RingBuffer`, fail-closed `Gatekeeper`, `OutcomeCollector`, `ConformalCalibrator`,
`PolicyGenerator` and a background `BatchConsumer` — into one serving API.
You normally interact **only** with this class.

```python
ledger = DecisionLedger("ledger.db")          # auto-starts the consumer

ctx = make_context_hash("qwen-7b", "routing") # 16-byte context reference
action = ledger.evaluate(ctx, confidence=0.85, decision_type="route")  # "ESCALATE"
outcome_id = ledger.log_outcome(decision_id, 1.0, "human")             # after flushing
ledger.calibrate()                            # learn + hot-reload a policy
ledger.stats()                                # live operational snapshot
ledger.shutdown()                             # flush, snapshot, close
```

### Constructor

```python
DecisionLedger(
    db_path: str = "ledger.db",
    policy_file: Optional[str] = None,
    exploration_rate: float = 0.02,
    ring_buffer_capacity: int = 100_000,
    flush_interval: float = 5.0,
    auto_start_consumer: bool = True,
) -> None
```

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `db_path` | `str` | `"ledger.db"` | Path to the SQLite store. Created (schema + indexes) on first touch. |
| `policy_file` | `str \| None` | `None` | Path to a schema-1.0 artifact (YAML) produced by `PolicyGenerator.generate_policy()`. Loaded into the gatekeeper at startup; new artifacts from `calibrate()` are written **next to it**. `None` (default) starts fully fail-closed, and artifacts go to `data/policies/`. |
| `exploration_rate` | `float` | `0.02` | Stratified shadow-sampling epsilon in `[0.0, 1.0]`. Must be in range or `ValueError`. |
| `ring_buffer_capacity` | `int` | `100_000` | Capacity of the in-memory telemetry ring buffer (>= 1). At capacity the oldest record is evicted and counted as a drop. |
| `flush_interval` | `float` | `5.0` | Maximum seconds between consumer flushes to SQLite. Smaller = more durable sooner, more write contention. |
| `auto_start_consumer` | `bool` | `True` | Start the background `BatchConsumer` thread immediately. When `False`, call `ledger.consumer.start()` or `ledger.calibrate()` (which drains) before depending on durability. |

**Raises.** `ValueError` if `exploration_rate` is outside `[0.0, 1.0]` or the
ring capacity is < 1. Note the constructor never raises on a *bad* `policy_file`
for an implicit reason: a missing file is `FileNotFoundError`; an invalid YAML
raises `PolicyValidationError`.

**Example.**

```python
from decision_ledger import DecisionLedger

# Development: no policy yet, don't spawn the background thread.
ledger = DecisionLedger("dev.db", auto_start_consumer=False)

# Production: start from the artifact the calibration pipeline published.
ledger = DecisionLedger("prod.db", policy_file="data/policies/policy_20260830-120046.yaml")
```

**Common mistakes.**

- Serving against a database with *no* policy and low `exploration_rate` looks
  like a bug (everything `ESCALATE`s) — it isn't; the gate is fail-closed until
  calibration activates a context.
- `auto_start_consumer=False` + forgetting to start or drain the consumer
  leads to `DecisionNotFoundError` on the very next `log_outcome()` (the
  decision is still only in the ring buffer, see below).

---

### `evaluate()` — the serving hot path

```python
evaluate(
    context_hash: bytes,
    model_confidence: Optional[float] = None,
    decision_type: str = "route",
    *,
    confidence: Optional[float] = None,
) -> str
```

**Purpose.** Decide whether the small model may act for a context, record the
decision to telemetry, and hand you the control signal. This is the only
synchronous, latency-critical call.

| Parameter | Type | Meaning |
| --- | --- | --- |
| `context_hash` | `bytes` | 16-byte context reference (from `make_context_hash()`, or `Gatekeeper`/`context_hash`). Anything that is not exactly 16 bytes escalates. |
| `model_confidence` | `float \| None` | Model self-confidence in `[0.0, 1.0]`. Out-of-range values are clamped (negative/NaN → 0.0, >1 → 1.0). |
| `decision_type` | `str` | One of `route`, `judge`, `speculate`, `mutate`, `summarize`, `abstain`; unknown names warn and are treated as `route`. |
| `confidence` | `float \| None` | Keyword-only alias for `model_confidence`. Pass **exactly one** of the two. |

**Returns.** `str` — one of `"ESCALATE"` (fail-closed: reroute to the
frontier model), `"DELEGATE"` (the small model may act), or
`"EXPLORE_SHADOW"` (counterfactual: both models run, the frontier serves).

**Raises.**

- `ValueError` — both `model_confidence` and `confidence` supplied, or neither.
- `RuntimeError` — the ledger has already been shut down.
- Unexpected errors inside the gatekeeper do **not** propagate: they are logged
  and the call fails closed with `"ESCALATE"` (safety over silence).

**Examples.**

```python
ctx = make_context_hash("qwen-7b", "routing")
ledger.evaluate(ctx, confidence=0.85, decision_type="route")      # alias
ledger.evaluate(ctx, model_confidence=0.85, decision_type="route")  # both mean the same
```

```python
action = ledger.evaluate(ctx, confidence=0.92, decision_type="route")
if action == "DELEGATE":
    result = small_model.generate(query)
elif action == "EXPLORE_SHADOW":
    small_model.generate(query)          # log the counterfactual only
    result = frontier_model.generate(query)
else:  # "ESCALATE"
    result = frontier_model.generate(query)
```

**Performance.** Microseconds, not milliseconds: the p50 of a 10k-request run
(with telemetry + ring-buffer append) is ≈ **7.7 µs**, p99 ≈ **27 µs**
(see [`BENCHMARKS.md`](BENCHMARKS.md)). The gatekeeper alone (no telemetry)
is ≈ **1.05 µs**. There is no network, disk, or allocation on this path; the
durable write is batched and deferred to the background consumer.

**Common mistakes.**

- Passing both confidence spellings, or neither — `ValueError` (this is the
  single most common newcomer error).
- Treating the *decisions recorded* count as immediately durable. `evaluate()`
  pushes to the ring buffer; until the consumer flushes, the row is not in
  SQLite (see `stats()`).

---

### `log_outcome()` — attach a label

```python
log_outcome(
    decision_id: str,
    outcome_value: float,
    outcome_source: str = "human",
    metadata: str = "",
) -> str
```

**Purpose.** Attach an *independent* observation (was the model right?) to a
previously recorded decision. Outcomes are the oxygen of calibration.

| Parameter | Type | Meaning |
| --- | --- | --- |
| `decision_id` | `str` | The decision's UUIDv7, as stored in SQLite. It must already be **durable** (flushed) or `DecisionNotFoundError` is raised. |
| `outcome_value` | `float` | Success score in `[0.0, 1.0]`; `0.0` = fail, `1.0` = success. Numeric strings are also accepted. Values < 0.5 are counted as failures by calibration. |
| `outcome_source` | `str` | One of `"human"`, `"task_metric"`, `"user_report"`, `"model_verification"` — or an `OutcomeSource` member. Default `"human"`. See [Outcome sources](#outcome-sources-and-exceptions). |
| `metadata` | `str` | Optional JSON (string or dict) with source-specific data, e.g. `'{"metric": "exact_match"}'`. |

**Returns.** `str` — the generated `outcome_id` (UUIDv7, 36 chars).

**Raises.** `DecisionNotFoundError`, `InvalidOutcomeValueError`,
`InvalidOutcomeSourceError`, `InvalidMetadataError`, `DatabaseError`, or
`RuntimeError` if the ledger is closed. (The `Error`-typed exceptions live in
`decision_ledger.outcomes`; import them from there.)

**Examples.**

```python
ledger.evaluate(ctx, confidence=0.9)
ledger.consumer.drain_now()                     # make the decision durable NOW
rows = ledger.database.execute_query("SELECT decision_id FROM decisions LIMIT 1")
decision_id = rows[0]["decision_id"]

ledger.log_outcome(decision_id, 1.0, "human", {"reviewer": "alice"})
ledger.log_outcome(decision_id, 0.0, OutcomeSource.TASK_METRIC, '{"metric": "accuracy"}')
```

**Common mistakes.**

- Not flushing before logging: with the default `flush_interval=5.0`, a fresh
  decision can still be ring-buffered when you call `log_outcome()` →
  `DecisionNotFoundError`. In production wait a poll cycle; in tests use
  `ledger.consumer.drain_now()`.
- `outcome_value` outside `[0.0, 1.0]` (e.g. between 0 and 100) →
  `InvalidOutcomeValueError`.

---

### `calibrate()` — learn thresholds

```python
calibrate(target_alpha: Optional[float] = None) -> str
```

**Purpose.** Run the full control loop and hot-reload the gate **with no
restart**: drain queued decisions → join decisions with outcomes → calibrate
every context that has outcomes (Split Conformal Risk Control) → generate a
schema-1.0 policy artifact → atomically swap it into the live gatekeeper.

| Parameter | Type | Meaning |
| --- | --- | --- |
| `target_alpha` | `float \| None` | Per-context risk budget, **strictly** in `(0.0, 1.0)`. Defaults to the calibrator's `0.05`. |

**Returns.** `str` — path of the newly generated and loaded policy artifact.

**Raises.** `ValueError` if `target_alpha` is not in `(0.0, 1.0)`;
`PolicyValidationError` / `DatabaseError` on artifact/store failures; `RuntimeError`
if the ledger is closed.

**Example.**

```python
path = ledger.calibrate(target_alpha=0.05)
print(f"active policy: {path}")

for ctx_hash, cfg in ledger.gatekeeper.policy.items():
    print(ctx_hash.hex(), "q_hat=", cfg.q_hat, "active=", cfg.is_active)
```

Requires durable **outcomes**: a context is only activated (`q_hat` set) once it
has at least `min_sample_size` (default 100) labeled decisions; otherwise it is
written to the policy as *draining* and the gate keeps escalating it.

---

### `stats()` — operational snapshot

```python
stats() -> Dict[str, Any]
```

**Purpose.** A live, read-only snapshot of the whole ledger. Cheap enough to
poll every N requests for dashboards.

**Returns v1 keys.**

| Key | Type | Meaning |
| --- | --- | --- |
| `total_decisions` | `int` | **Durable** decisions in SQLite (not ring-buffered ones). |
| `total_outcomes` | `int` | Outcomes logged. |
| `join_rate` | `float` | `matched / total_decisions`, `0.0`–`1.0`. |
| `decisions_by_action` | `dict[str, int]` | Persisted counts per action. |
| `contexts_active` | `int` | Contexts with `is_active` in the loaded policy. |
| `contexts_draining` | `int` | Policy contexts that are not yet active. |
| `ring_buffer_fill` | `float` | `size / capacity`, `0.0`–`1.0`. |
| `ring_buffer_size` | `int` | Records currently buffered, awaiting flush. |
| `dropped_records` | `int` | Records the buffer evicted/dropped (capacity wraparound, backpressure, rejected inputs). |
| `policy_version` | `str \| None` | Loaded policy version; `None` when fail-closed. |
| `gatekeeper` | `dict` | Gate metrics: `delegate`, `escalate`, `explore`, `total`, `escalation_rate`, `exploration_rate`, `per_decision_type`. |
| `consumer` | `dict` | Consumer metrics: `total_records_processed`, `total_records_flushed`, `total_records_dropped`, `total_flushes`, `last_flush_time`, `avg_flush_time_ms`, `backlog_records`. |

To monitor the *live serving rate* watch `gatekeeper["total"]`; to monitor
durability watch `ring_buffer_fill` and `consumer["backlog_records"]`.

---

### `shutdown()` and context management

```python
shutdown(timeout: float = 10.0) -> Dict[str, Any]
```

**Purpose.** Stop the consumer thread (joining it with `timeout`), flush
whatever remains, take a final `stats()` snapshot, and close the database.
Also writes the snapshot to `<db stem>_stats.json` next to the ledger file so
the last-known-good numbers survive a restart.

- `timeout: float` — seconds to wait for the consumer loop thread to exit;
  the final flush still runs afterwards, on the calling thread.

**Returns.** The final stats dict.

**Raises.** `RuntimeError` — a second `shutdown()` (the ledger is closed after
the first). Guarded: the consumer `stop()`/drain/snapshot steps each tolerate
and log their own failures rather than aborting shutdown.

```python
final = ledger.shutdown()
print(final["total_decisions"])          # snapshotted + written to ledger_stats.json
# ledger.evaluate(...)  # now raises RuntimeError
```

It is also a **context manager** — closing flushes and releases everything at
the end of the block:

```python
with DecisionLedger("app.db") as ledger:
    while serving:
        ledger.evaluate(ctx, confidence=score)
# ledger is shut down and flushed here, automatically
```

**Common mistakes.** Keeping a reference after `shutdown()` (all public methods
raise `RuntimeError`); forgetting that `shutdown()` is not free — it forces a
final flush and snapshot, so don't call it in a hot loop.

---

### Public attributes

| Attribute | Type | Notes |
| --- | --- | --- |
| `ledger.database` | `Database` | The SQLite store (`execute_query`, `execute_write`, `batch_insert`, `get_join_statistics`, `joiner`, …). |
| `ledger.ring_buffer` | `RingBuffer` | Telemetry buffer; also the gatekeeper's `telemetry`. |
| `ledger.gatekeeper` | `Gatekeeper` | The evaluator (`policy`, `get_metrics()`, …). |
| `ledger.outcome_collector` | `OutcomeCollector` | `log_outcome()`, `log_outcomes_batch()`, `get_outcome()`, `get_outcomes_for_decision()`. |
| `ledger.calibrator` | `ConformalCalibrator` | `calibrate_context()`, `detect_drift()`. |
| `ledger.policy_generator` | `PolicyGenerator` | `generate_policy()`, `load_latest_policy()`. |
| `ledger.consumer` | `BatchConsumer` | `start()`, `stop()`, `drain_now()`, `get_metrics()`. |

---

## Helper functions

All are pure and cheap; import them from `decision_ledger`.

### `make_context_hash(model_id, task_type, prompt_template_version="default", quantization="int8", adapter_config="") -> bytes`

Deterministic 128-bit (16-byte) **model + task** reference used for calibration
and cache keys. Same inputs → same bytes on any machine (BLAKE3); any changed
factor → a fresh context. Factor boundaries are serialized with `repr()`, so
`("a","bc")` can never collide with `("ab","c")`.

```python
ctx_routing = make_context_hash("qwen-7b", "routing")
ctx_judging = make_context_hash("qwen-7b", "judging")

# Same inputs, same hash:
assert ctx_routing == make_context_hash("qwen-7b", "routing")

# Different quantization is a DIFFERENT context (fresh, fail-closed):
ctx_fp16 = make_context_hash("qwen-7b", "routing", quantization="fp16")
assert ctx_fp16 != ctx_routing

print(ctx_routing.hex())   # '1f810209b58ae19f185af097ca9cf646'
```

| Argument | Type | Default | Meaning |
| --- | --- | --- | --- |
| `model_id` | `str` | — | Model name + version, e.g. `"qwen-7b"`. |
| `task_type` | `str` | — | Control-plane task, e.g. `"routing"`, `"judge"`. |
| `prompt_template_version` | `str` | `"default"` | Prompt template in use. |
| `quantization` | `str` | `"int8"` | e.g. `"int8"`, `"fp16"`. |
| `adapter_config` | `str` | `""` | LoRA/adapter id; `""` when none. |

**Common mistakes.** Passing a `str` hash instead of these factors (you *want*
the factoring); using a truncated/hex-encoded form where a `bytes` is required
(the gatekeeper validates exactly 16 bytes).

### `generate_uuidv7() -> str`

Time-ordered UUID **v7** as a canonical 36-char lowercase string. The leading
48 bits are a millisecond timestamp, so batched IDs preserve causal order —
the ledger relies on this for replaying decisions in arrival order. Uses
`uuid.uuid7` (Python 3.14+), falling back to the `uuid6` package, then to a
UUIDv4-compatible string (unique but no longer time-ordered).

```python
id1, id2 = generate_uuidv7(), generate_uuidv7()
assert id1 < id2          # time-ordered
assert len(id1) == 36
```

### `now_ns() -> int` and `now_us() -> int`

Wall-clock time since the epoch:

- `now_ns()` — **nanoseconds** via `time.time_ns()`, used for
  `DecisionRecord.timestamp_ns`.
- `now_us()` — **microseconds** (`int(time.time() * 1e6)`), handy for
  latency budgets in the 1–100 µs range.

```python
start = now_us()
for _ in range(1000):
    ledger.evaluate(ctx, confidence=0.5)
print(f"1000 evaluates in {now_us() - start} µs")
```

> Also exported: `context_hash(decision_type, ...)` (the *serving-context*
> hash used by the production gatekeeper — factors include model weights,
> temperature, adapter), `decision_id()` (alias of `generate_uuidv7`),
> `validate_confidence(x)` (clamps to `[0,1]`; raises `TypeError` for
> non-numbers), `validate_context_hash(x)` (`True` iff exactly 16 bytes), and
> `setup_logging(level, log_file=None)`.

---

## Data structures

### `GateAction(IntEnum)`

Outcome of a gatekeeper evaluation.

| Member | Value | Meaning |
| --- | --- | --- |
| `GateAction.DELEGATE` | `0` | The small model may act. |
| `GateAction.ESCALATE` | `1` | Reroute to the frontier model (fail-closed default). |
| `GateAction.EXPLORE_SHADOW` | `2` | Counterfactual: both models run, frontier serves. |

`DecisionLedger.evaluate()` returns the **name string** (`"DELEGATE"`); the raw
gatekeeper returns the enum. Convert with `.name` / `GateAction[value]`.

```python
from decision_ledger import GateAction

assert GateAction(0).name == "DELEGATE"
assert GateAction["ESCALATE"].value == 1
assert GateAction(2).name == "EXPLORE_SHADOW"
```

### `CalibrationContext` *(dataclass)*

The calibrated operating envelope for one context, as held in
`Gatekeeper.policy`.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `context_hash` | `bytes` | — | 16-byte context reference. |
| `q_hat` | `float \| None` | `None` | Conformal non-conformity threshold. `None` = not yet calibrated (escalate). |
| `min_sample_size` | `int` | `100` | Outcomes needed before activation. |
| `current_sample_size` | `int` | `0` | Outcomes seen so far (maintained by the pipeline). |
| `is_active` | `bool` | `False` | Whether the context may delegate. |

Property: `has_enough_data` → `current_sample_size >= min_sample_size`.

```python
cfg = ledger.gatekeeper.policy[ctx]
if cfg.is_active and cfg.q_hat is not None:
    if 1.0 - confidence <= cfg.q_hat:
        action = "DELEGATE"
```

### `DecisionRecord` *(frozen dataclass, `slots=True`)*

Immutable record of a single evaluation, pushed to the ring buffer and later
serialized to the `decisions` table by the consumer.

| Field | Type | Meaning |
| --- | --- | --- |
| `decision_id` | `str` | UUIDv7, 36 chars. |
| `timestamp_ns` | `int` | Nanoseconds since epoch (`now_ns()`). |
| `context_hash` | `bytes` | 16-byte context reference (raw bytes, not hex — the schema column is a BLOB). |
| `decision_type` | `str` | e.g. `"route"`. |
| `model_confidence` | `float` | The confidence passed to the gate. |
| `non_conformity` | `float` | `1.0 - confidence` (the calibration score `S`). |
| `action_taken` | `str` | `"DELEGATE"`, `"ESCALATE"` or `"EXPLORE_SHADOW"`. |
| `latency_us` | `int` | Evaluation latency in µs (measured by the gatekeeper). |

Classmethods/methods: `DecisionRecord.from_evaluation(**kwargs)` builds one
(deriving `non_conformity`); `.to_dict()` returns a plain dict with
`context_hash` **hex-encoded** (for JSONL/logging — note the column stores raw
bytes).

### `CalibrationResult` *(frozen dataclass)*

Output of calibration for a single context (also the per-context input the
pipeline feeds to policy generation).

| Field | Type | Meaning |
| --- | --- | --- |
| `q_hat` | `float \| None` | Empirically chosen non-conformity threshold; `None` = not enough data / risk never in budget. |
| `sample_size` | `int` | Number of labeled decisions used. |
| `coverage_lower_bound` | `float \| None` | Conformal coverage bound, if computed. |
| `achieved_empirical_risk` | `float \| None` | Actual risk at the chosen threshold. |
| `min_observed_loss` | `float` | Smallest loss observed (0.0 = all correct). |
| `max_observed_loss` | `float` | Largest loss observed (1.0 = all failed). |

`CalibrationResult.sample_size >= min_sample_size` and a finite `q_hat` are
the two conditions for a context to become `ACTIVE` in the policy.

---

## Outcome sources and exceptions

### `OutcomeSource(Enum)`

| Member | Value | Used for calibration? |
| --- | --- | --- |
| `OutcomeSource.HUMAN` | `"human"` | Yes |
| `OutcomeSource.TASK_METRIC` | `"task_metric"` | Yes |
| `OutcomeSource.USER_REPORT` | `"user_report"` | Yes |
| `OutcomeSource.MODEL_VERIFICATION` | `"model_verification"` | **No** — the verifier is itself a model, not an *independent* observer. |

Pass the string value (`"human"`) or the enum everywhere an outcome source is
expected.

### Exceptions

All are raised by `log_outcome()` / the outcome collector and are importable
from `decision_ledger.outcomes`:

| Exception | Meaning |
| --- | --- |
| `DecisionNotFoundError` | Outcome references an unknown decision (usually: not yet flushed). |
| `InvalidOutcomeValueError` | `outcome_value` not in `[0.0, 1.0]`. |
| `InvalidOutcomeSourceError` | Unknown source. |
| `InvalidMetadataError` | `metadata` is not valid JSON. |
| `DatabaseError` / `DatabaseIntegrityError` | Store failures / constraint violations. |
| `PolicyError` / `PolicyValidationError` | Policy artifact generation/validation failures (from `calibrate()`). |

`DecisionLedger.evaluate()` never raises these; it fails closed to
`"ESCALATE"`.

---

## CLI: logging an outcome

Record a single outcome from the shell — the same call as
`log_outcome()`, minus the `_require_open()` check (it opens its own
`Database`):

```bash
python -m decision_ledger.outcomes \
  --decision-id 018d5c13-2b55-7120-a2c5-8d5c13b2f000 \
  --outcome-value 1.0 \
  --source human \
  --metadata '{"reviewer": "alice"}' \
  --db ledger.db
```

| Argument | Required | Type/default | Meaning |
| --- | --- | --- | --- |
| `--decision-id` | yes | `str` | UUIDv7 of a durable decision. |
| `--outcome-value` | yes | `float` | `[0.0, 1.0]`; `0.0` = fail, `1.0` = success. |
| `--source` | yes | `{human, task_metric, model_verification, user_report}` | `OutcomeSource` string value. |
| `--metadata` | no | `str`, default `""` | Optional JSON string. |
| `--db` | no | `str`, default `"ledger.db"` | Path to the SQLite ledger file. |

**Exit codes.** `0` success · `2` validation error (bad UUID, value, source, or
metadata — also argparse usage errors) · `3` database error.

```bash
echo $?   # 0
```

Scriptable equivalent in Python:

```python
from decision_ledger.outcomes import OutcomeCollector
from decision_ledger import DecisionLedger

with DecisionLedger("ledger.db") as ledger:
    outcome_id = ledger.outcome_collector.log_outcome(
        decision_id, 1.0, "human"
    )
```

---

## Performance notes

- **Hot path** — `DecisionLedger.evaluate()` (telemetry on): p50 ≈ **7.7 µs**,
  p99 ≈ **27 µs** over 10k calls; the bare `Gatekeeper.evaluate()` ≈ **1 µs**.
  Budgets are asserted in `test_gatekeeper.py` (mean < 1 ms).
- **Writes are batched, not synchronous.** `evaluate()` pushes to the ring
  buffer; the `BatchConsumer` drains (default `batch_size=5_000`,
  `flush_interval=5.0 s`) into SQLite. Durability is intentionally deferred;
  `drain_now()` forces a synchronous flush for tests and for
  `calibrate()`.
- **Ring buffer backpressure.** At `fill >= 0.95` and full, the buffer drops
  exploratory records first, then the oldest; all drops count in
  `dropped_records`. Tune `ring_buffer_capacity` / `flush_interval` so steady
  state stays below that.
- **Exploration is deterministic.** Call `n` explores when
  `n % int(1/rate) == 0`: the measured rate matches `exploration_rate`
  exactly and runs are reproducible (no PRNG).
- **DDos-proofing the DB.** Each writer thread owns its SQLite connection;
  `check_same_thread=False` allows the consumer thread to write and the main
  thread to close at `shutdown()`.

## Common mistakes

1. **Both (or neither) confidence kwargs** → `ValueError`. Pass `confidence=`
   **or** `model_confidence=`, never both.
2. **Outcome before durability** → `DecisionNotFoundError`. Flush first
   (`ledger.consumer.drain_now()` in tests; a poll cycle in prod).
3. **Context hash not bytes / not 16 bytes** → the ledger escalates
   (fail-closed), never crashes. Build with `make_context_hash()`.
4. **`outcome_value` on a 0–100 scale** → `InvalidOutcomeValueError`; use
   `[0.0, 1.0]`.
5. **Using the ledger after `shutdown()`** → `RuntimeError`.
6. **Treating `stats()["total_decisions"]` as the live request count during a
   burst** — it counts durable rows; add `ring_buffer_size` for the buffer.
7. **Expecting `model_verification` outcomes to calibrate** — they won't;
   they are excluded by design.

---

For step-by-step onboarding see [`QUICKSTART.md`](QUICKSTART.md), worked
programs in `src/examples/`, and the deep dives:
[`API.md`](API.md) · [`API_GATEKEEPER.md`](API_GATEKEEPER.md) ·
[`API_TELEMETRY.md`](API_TELEMETRY.md) · [`BENCHMARKS.md`](BENCHMARKS.md).