# 13 — All Features & Functionalities: Deep-Dive

A complete, reverse-engineered understanding of **everything the Decision
Ledger repository does** as one integrated system. Not a file summary — a
functional analysis grounded in the exact code paths. Every claim carries a
`path:line` anchor you can open.

> Orientation: this "feature" is the entire closed loop
> **serve → record → verify → calibrate → republish**. Understanding that loop
> is understanding the library. Section 2 lists every entry point; section 3
> traces each.

---

## 1. Feature Overview

### What the feature does

Decision Ledger is an embeddable Python library that makes a **statistical
promise operational**: *only let a small model act on a decision when the
historical, outcome-verified risk of acting stays within a budget `alpha`.*

The complete feature is the **learned serving loop**:

1. **Serve** — an application asks the gatekeeper whether to act
   (`evaluate`); it answers `DELEGATE | EXPLORE_SHADOW | ESCALATE` in
   microseconds.
2. **Record** — every evaluation becomes an immutable `DecisionRecord`
   (off-path ring buffer → background consumer → durable SQLite).
3. **Verify** — ground truth arrives later as an outcome attached to the
   original decision (`log_outcome`, API or CLI).
4. **Learn** — `calibrate()` reconciles decisions with outcomes, computes a
   per-context non-conformity threshold `q_hat` via conformal risk control,
   detects confidence drift, publishes a versioned policy artifact, and
   **hot-reloads** it into the live gatekeeper.
5. **Report** — `stats()`, `get_metrics()`, `_stats.json` snapshots and a
   summary line make the loop auditable.

### Who uses it

| User | How | Entry point |
| --- | --- | --- |
| Embedding application (a service hosting a small model) | Decisions + outcomes + calibration on the hot path | `DecisionLedger` API |
| The model/scorer itself | Supplies `confidence ∈ [0,1]` only — no code from this repo | (external producer) |
| Operators / data teams | Log outcomes via script, inspect policy artifacts, rollback | CLI + YAML files |
| Test/CI harnesses, demos | Exercise the loop on synthetic data | `src/examples/*`, pytest |

### What problem it solves

Raw confidence is not a safety argument. A model can be confident and wrong,
and an over-confident small model making control-plane decisions causes real
damage. The ledger replaces *"trust the number"* with *"trust the measured
record"*:

* decisions are **paired with independent outcomes** (not left unverified);
* the delegation threshold comes from **real risk on real data** via
  non-conformity scores (`S = 1 - confidence`), not a hardcoded cutoff;
* trust **degrades to escalation** whenever evidence is missing, stale, or
  drifting — fail closed, never fail open.

### Important business rules (encoded in code)

1. **Fail closed.** Unknown context, inactive context, too few samples, no
   threshold, or any unexpected exception ⇒ `ESCALATE`
   (`gatekeeper.py:193-214`, `__init__.py:225-233`).
2. **Deterministic exploration.** Call `n` explores when
   `n % int(1/rate) == 0` — exactly one in fifty at rate 0.02; no PRNG
   (`gatekeeper.py:231-245`).
3. **Only outcome-verified, independent, non-exploratory decisions calibrate.**
   `model_verification` outcomes are excluded (self-confirmation);
   `EXPLORE_SHADOW` records are excluded (`calibration.py:151-152`,
   `_INDEPENDENT_OUTCOME_SOURCES`).
4. **Minimum evidence before trust.** `q_hat` is `None` (⇒ escalate) unless a
   context has ≥ 100 (default) joined samples and a prefix with empirical risk
   ≤ alpha exists (`calibration.py:143-169`).
5. **One outcome per decision.** No double-labeling; enforced by the UNIQUE
   decision linkage and validation (`outcomes.py`).
6. **Immutable, versioned policy artifacts.** Every republish creates a new
   `policy_<YYYYMMDD-HHMMSS>.yaml` and repoints `policy_latest.yaml`
   (symlink or copy, Windows-aware) (`policy.py:310-361,421-427`).
7. **Producer never blocks on disk.** The hot path pushes to a bounded ring
   buffer that drops low-priority (exploratory) records rather than blocking
   (`telemetry.py:146-204`).
8. **One writer at a time.** Only the consumer thread may batch-write
   decisions; everything else reads with thread-local connections.

---

## 2. Entry Point(s)

The system has **seven** execution entry points. There is no UI and no HTTP
endpoint.

| # | Entry point | Trigger type | Exact location |
| --- | --- | --- | --- |
| 1 | `DecisionLedger.evaluate()` | Library call (application hot path) | `src/decision_ledger/__init__.py:173` |
| 2 | `DecisionLedger.log_outcome()` | Library call (applications) | `src/decision_ledger/__init__.py:246` |
| 3 | `DecisionLedger.calibrate()` | Library call/scheduled job (ops) | `src/decision_ledger/__init__.py:293` |
| 4 | `DecisionLedger.stats()` | Library call (ops/monitoring) | `src/decision_ledger/__init__.py:336` |
| 5 | `DecisionLedger.shutdown()` | Library call (lifecycle) | `src/decision_ledger/__init__.py:377` |
| 6 | **CLI** `python -m decision_ledger.outcomes ...` | CLI command (operators/data teams) | `src/decision_ledger/outcomes.py:570` (`main`), parser at `531` |
| 7 | `BatchConsumer` background thread | Background job (starts inside `DecisionLedger.__init__`) | `src/decision_ledger/consumer.py:201` (`run`), started at `consumer.py:134` |

The **master entry point** is `DecisionLedger.__init__`
(`__init__.py:104-157`): it builds the entire object graph (Database, RingBuffer,
Gatekeeper, OutcomeCollector, ConformalCalibrator, PolicyGenerator,
BatchConsumer) and immediately starts the consumer unless disabled — so executing
*any* ledger method means the background job is already running.

Example programs (`src/examples/basic_serving.py`, `outcome_logging.py`,
`calibration_demo.py`, `stress_test.py`) are themselves entry points that call
#1–#5.

---

## 3. Complete Execution Trace

Format per step: **file — class/module — function — responsibility — input →
output — side effects.**

### Trace 1 — Serve: `evaluate()`

| Step | Location | Function | Responsibility | Input → Output | Side effects |
| --- | --- | --- | --- | --- | --- |
| 1 | `__init__.py:173` | `DecisionLedger.evaluate` | Public serving API: normalize + delegate + fail-closed catch | `(context_hash: bytes, model_confidence) → str action` | — |
| 2 | `__init__.py:212` | `_require_open` | Refuse work after shutdown | — → raises `RuntimeError` | — |
| 3 | `__init__.py:213-223` | (inline) | Confidence alias guard: accept `confidence` or `model_confidence`, never both | → normalized `float` | raises `ValueError` on misuse |
| 4 | `__init__.py:226` | `gatekeeper.evaluate` (call) | The actual decision | → `GateAction` | see below |
| 5 | `__init__.py:227-233` | (inline) | Fail-closed catch-all | any exception → `ESCALATE` | logs `exception` |
| 6 | `__init__.py:240` | (inline) | Row logging | — | logs INFO |
| 7 | `gatekeeper.py:152` | `Gatekeeper.evaluate` | Decide with no allocations on the path | `(hash, confidence, type) → GateAction` | — |
| 8 | `gatekeeper.py:160-167` | (inline) | Map decision_type string → int code; unknown → default | `str → int` | logs warning for unknown type |
| 9 | `gatekeeper.py:169-184` | (inline) | Clamp confidence to [0,1]; treat NaN as 0.0 | `float → float` | logs warning |
| 10 | `gatekeeper.py:186,188` | (inline) | Start latency timer; acquire RLock | — | mutual exclusion with reloads |
| 11 | `gatekeeper.py:191` | (inline) | Per-type call counter | — | `_calls[code] += 1` |
| 12 | `gatekeeper.py:193-214` | (inline) | **Fail-closed checks**: unknown context / not active / < min samples / no q_hat | → `ESCALATE` | escalations counters |
| 13 | `gatekeeper.py:216-220` | `_should_explore` | Deterministic slot test `call % period == 0` | → `bool` | advances `_call_count` |
| 14 | `gatekeeper.py:222-229` | (inline) | Rule: `1 - confidence <= q_hat` → `DELEGATE`, else `ESCALATE` | → `GateAction` | delegate/escalate counters |
| 15 | `gatekeeper.py:354` | `_record` | Build + push telemetry record | — | pushes `DecisionRecord` to ring buffer (unless telemetry None) |
| 16 | `telemetry.py:43` | `DecisionRecord.from_evaluation` | Frozen record: id (UUIDv7), confidence, `non_conformity = 1 - c`, action, latency_us | → `DecisionRecord` | mints `decision_id` |
| 17 | `telemetry.py:146` | `RingBuffer.push` | Non-blocking enqueue | → `bool` accepted | may drop / evict exploratory under overload; increments counters |
| 18 | `telemetry.py:71` | `DecisionRecord.to_dict` (used at flush by consumer) | Row dict for the DB | → `dict` | — |

*(After the return, the record continues asynchronously through Trace 4.)*

### Trace 2 — Verify: `log_outcome()`

| Step | Location | Function | Responsibility | Input → Output | Side effects |
| --- | --- | --- | --- | --- | --- |
| 1 | `__init__.py:246` | `DecisionLedger.log_outcome` | Public API: validate + attach ground truth | `(decision_id, value, source, metadata) → outcome_id` | — |
| 2 | `__init__.py:275` | `_require_open` | Closed-state guard | — → raises | — |
| 3 | `__init__.py:280` | `_coerce_outcome_source` | String → `OutcomeSource` | `str → Enum` | raises on unknown |
| 4 | `outcomes.py:350` | `OutcomeCollector.log_outcome` | Validation funnel + insert | → `outcome_id` | counters |
| 5 | `outcomes.py:378` | `_ensure_decision_exists` | FK existence check | — → raises `DecisionNotFoundError` if absent | SELECT |
| 6 | `outcomes.py:497` | `_build_row` | Normalize value/JSON metadata; mint `outcome_id` UUIDv7 | → `_OutcomeRow` | `_coerce_outcome_value` (bool→reject, strings→float) |
| 7 | `outcomes.py:514` | `_insert_rows` | Persist row(s) | → none | INSERT into `outcomes` via `Database` |
| 8 | `database.py` | `execute_write` → SQLite | Commit | → rowcount | — |

Exact same path is used by the **CLI** (Trace 5) and by `log_outcomes_batch`
(`outcomes.py:415`) which pre-validates all rows then inserts in one
transaction (all-or-nothing).

### Trace 3 — Learn: `calibrate()`

| Step | Location | Function | Responsibility | Input → Output | Side effects |
| --- | --- | --- | --- | --- | --- |
| 1 | `__init__.py:293` | `DecisionLedger.calibrate` | Orchestrate full loop | `target_alpha? → artifact path` | — |
| 2 | `__init__.py:314-315` | (inline) | Closed guard; resolve alpha (default 0.05) | − | — |
| 3 | `__init__.py:317` | `consumer.drain_now` | Make pending decisions durable before learning | → int drained | flush to `decisions` |
| 4 | `__init__.py:318` | `joiner.join_decisions_and_outcomes` | Materialize LEFT JOIN into `joined_records` | → int joined | `INSERT … ON CONFLICT DO NOTHING`, watermark-based |
| 5 | `__init__.py:319-324` | `CalibrationPipeline(...)` | Wire DB + gatekeeper + generator | → pipeline | — |
| 6 | `pipeline.py:61` | `run_calibration` | Calibrate every context + republish + reload | → artifact path | see below |
| 7 | `pipeline.py:145` | `_context_hashes` | Distinct contexts with ≥1 outcome | → `List[bytes]` | read `joined_records` |
| 8 | `pipeline.py:71-82` | (loop) | Per context = `calibrate_context` + `detect_drift` | → results + drift map | logs per-context |
| 9 | `calibration.py:241` | `calibrate_context` | Query independent joined rows; call core | → `CalibrationResult` | SELECT filter: not `EXPLORE_SHADOW`, source in `_INDEPENDENT_OUTCOME_SOURCES` |
| 10 | `calibration.py:143` | `compute_threshold` | **The math**: process valid records, compute `q_hat` | `records → CalibrationResult` | numpy sort/cumsum; no DB writes |
| 11 | `calibration.py:146-169` | (inline) | Only `is_independent and not is_exploratory`; score-sort; empirical risk prefix ≤ alpha; `None` if n<100 or none qualify | → `q_hat`/coverage/Wilson bound | — |
| 12 | `calibration.py:296` | `detect_drift` | Compare full-range vs active-range accuracy; flag divergence > 5% | → `{drift_detected, …}` | read only |
| 13 | `pipeline.py:84` | `policy_generator.generate_policy` | Publish artifact | → path | writes YAML + repoints `policy_latest.yaml` |
| 14 | `policy.py:310` | `generate_policy` | Build schema-1.0 dict + `yaml.safe_dump` + `_refresh_latest_link` | → path | mkdir, write file (non-atomic — LOW-6) |
| 15 | `policy.py:401` | `_entry_for` | Per-context entry: ACTIVE/DRAINING/REVOKED + q_hat | → dict | — |
| 16 | `pipeline.py:85` | `gatekeeper.reload_policy_from_file` | Load + validate + hot swap | — | → `reload_policy` |
| 17 | `gatekeeper.py:272` | `reload_policy_from_file` | `load_policy` + `convert_policy_dict_to_contexts` + `reload_policy` | — | reads YAML, validates |
| 18 | `gatekeeper.py:247` | `reload_policy` | Swap snapshot dict under RLock | — | concurrent readers finish on old snapshot |
| 19 | `pipeline.py:90-95` | (inline) | Summary `print` | — | note: `print`, not logger (LOW-4) |

### Trace 4 — Durable drain: `BatchConsumer` (background job)

| Step | Location | Function | Responsibility | Input → Output | Side effects |
| --- | --- | --- | --- | --- | --- |
| 1 | `consumer.py:134` | `start` | Spawn daemon thread | — | thread begins `run` |
| 2 | `consumer.py:201` | `run` | Loop forever until stopped | — | — |
| 3 | `consumer.py:212` | `ring_buffer.pop_batch(1000)` | Pull batch | `→ List[DecisionRecord]` | deque atomic pop |
| 4 | `consumer.py:214` | `_append_to_batch` | Accumulate in-memory batch | — | `_batch.extend` |
| 5 | `consumer.py:216-225` | (inline) | Flush when `batch >= 5000` OR `flush_interval` elapsed; backoff on failure (1s) | → bool | — |
| 6 | `consumer.py:273` | `_flush_to_db` | 3 retry attempts; `batch_insert` | → bool | on persistent failure: hold in memory |
| 7 | `consumer.py:247` | `_apply_backlog_locked` | Enforce 100k cap + 50k alert level | — | **drops oldest** records past cap; CRITICAL logs |
| 8 | `database.py:493` | `batch_insert` | One `BEGIN…COMMIT` tx; rollback on error | → rowcount | single writer |
| 9 | `consumer.py:340` | `_record_to_row` | `DecisionRecord → dict` row | → dict | — |

*Sleeps `POLL_INTERVAL_S = 0.1`; never lets one bad poll kill the loop
(`consumer.py:230-233`).*

### Trace 5 — CLI: record an outcome

| Step | Location | Function | Responsibility | Input → Output | Side effects |
| --- | --- | --- | --- | --- | --- |
| 1 | `outcomes.py:570` | `main` | CLI entry + exit-code contract | `argv → int (0/2/3)` | — |
| 2 | `outcomes.py:531` | `_build_parser` | Args: `--decision-id`, `--outcome-value`, `--source`, `--metadata`, `--db` | → parser | choices = `OutcomeSource` values |
| 3 | `outcomes.py:575-577` | (inline) | UUID format check | — | `parser.error` on bad UUID |
| 4 | `outcomes.py:579` | `Database(args.db)` | Open store | — | — |
| 5 | `outcomes.py:581` | `OutcomeCollector.log_outcome` | Same funnel as Trace 2 | → outcome_id | INSERT |
| 6 | `outcomes.py:587-595` | (inline) | Map validation errors → exit 2 | — | logs |
| 7 | `outcomes.py:596-599` | (inline) | Map DB errors → exit 3 | — | logs |
| 8 | `outcomes.py:601` | closed & printed | `database.close()` | — | exit 0 |

### Trace 6 — Shutdown

`DecisionLedger.shutdown` (`__init__.py:377`): idempotency guard →
`consumer.stop(timeout=10)` (final drain + final flush) → `_write_final_stats`
(`*_stats.json`) → `database.close()`. After this, `_require_open` fails every
API call (fail-closed at the system level).

---

## 4. Data Flow

### 4.1 Decision data — the "offer"

```
model_confidence (float, app-supplied)
  → clamp to [0,1], NaN→0.0                      [gatekeeper.py:169-184]
  → non_conformity S = 1 - confidence             [gatekeeper.py:222]
  → action (ESCALATE | DELEGATE | EXPLORE_SHADOW) [gatekeeper.py:193-229]
  → DecisionRecord (frozen dataclass, UUIDv7 id)  [telemetry.py:43]
  → ring buffer deque (bounded 100k)              [telemetry.py:146]
  → pop_batch(1000)                               [consumer.py:212]
  → to_dict / _record_to_row (dict row)           [consumer.py:340, telemetry.py:71]
  → batch_insert (BEGIN…COMMIT transaction)       [database.py:493]
  → decisions table row
  → LEFT JOIN outcome (if/when present)           [database.py:687-701]
  → joined_records row (denormalized copy)
  → CalibrationRecord (context_hash, S, loss)     [calibration.py:63]
  → CalibrationResult (q_hat, coverage, risk)     [calibration.py:74]
  → policy artifact YAML entry (context_ref, state, q_hat) [policy.py:401]
  → CalibrationContext dataclass in gatekeeper snapshot [gatekeeper.py:291]
```

### 4.2 Outcome data — the "verdict"

```
outcome_value (float|numeric str) + source (human|task_metric|user_report|model_verification)
  → _coerce_outcome_value [0,1]                   [outcomes.py:87]
  → _coerce_outcome_source → OutcomeSource        [outcomes.py helpers]
  → metadata JSON (validated string or dict)
  → _OutcomeRow {outcome_id UUIDv7, decision_id, timestamp_ns, …} [outcomes.py:46]
  → INSERT outcomes                               [database.py]
  → JOINED via joiner into joined_records         [database.py:687-701]
  → loss = 0.0/1.0 from value                     [calibration.py:280 / loss_from_outcome_value]
  → calibration + drift computation               [calibration.py:241-345]
  → policy state (ACTIVE / DRAINING / REVOKED)    [policy.py:401-419]
```

### 4.3 Policy data — the "contract"

```
CalibrationResult map (context_hash → result)
  → artifact dict {schema_version, policy_version, generated_at,
                   global{alpha, fail_closed, exploration_rate, min_sample_size},
                   contexts[]}                    [policy.py:342-356]
  → yaml.safe_dump + write policy_<ts>.yaml       [policy.py:358]
  → _refresh_latest_link → policy_latest.yaml     [policy.py:421]
  → load_policy → validated dict                  [policy.py validate]
  → convert_policy_dict_to_contexts               [gatekeeper.py:291]
  → Dict[bytes, CalibrationContext] → reload_policy (atomic swap) [gatekeeper.py:247-254]
```

**Transformation style:** every boundary converts a *representation of the same
fact* (an offer / a verdict / a threshold). Nothing is derived twice — the
denormalized `joined_records` is the calibration stage's single input, and the
YAML artifact is the serving stage's single input.

---

## 5. Architecture (layers in this feature)

| Layer | Components | Responsibility | Evidence |
| --- | --- | --- | --- |
| **Controller/handler** | `DecisionLedger` facade (`evaluate`, `log_outcome`, `calibrate`, `stats`, `shutdown`) | Entry points; arg normalization; guard rails; fail-closed catch | `__init__.py:173-430` |
| **Service / use case** | `CalibrationPipeline.run_calibration`; `OutcomeCollector.log_outcome` (batch + single) | Orchestrate multi-step workflows | `pipeline.py:61`; `outcomes.py:350,415` |
| **Domain logic** | `Gatekeeper.evaluate` (decision rule + exploration); `ConformalCalibrator.compute_threshold` / `detect_drift` (the statistics); `PolicyGenerator._entry_for` (trust states) | The actual "thinking" — no I/O plumbing | `gatekeeper.py:152`; `calibration.py:143,296`; `policy.py:401` |
| **Repository** | `Database` domain queries (`batch_insert`, `get_joined_records`, `get_join_statistics`, `get_outcomes`), `Joiner`, `RingBuffer` (in-memory capture) | Opaque typed surface over state | `database.py` (718 lines) |
| **Database** | SQLite 4 tables + 8 indexes (DDL `database.py:94-158`) | Durable truth | schema §8 |
| **Infrastructure / async** | `BatchConsumer` thread + `JsonlExport` strategy; `threading.local` connections; RLock snapshot swap | Batching, backpressure, concurrency | `consumer.py` |
| **External services** | *None at runtime.* Filesystem is the only "external" boundary (SQLite file, YAML, JSONL, stats) | — | verified: no net/HTTP imports |
| **CLI** | `python -m decision_ledger.outcomes` | Human/scripted out-of-band writes | `outcomes.py:531-603` |

**Why it crosses these layers:** the feature is fundamentally a **feedback
loop**. The serving layer needs sub-microsecond latency; the learning layer
needs correct statistics over accumulation; the persistence layer needs
durability independent of both. So the loop *must* be split — the hot path
cannot wait on a join, and the calibrator cannot care about deque mechanics.
The layered split is what lets each part be independently tested and swapped
(e.g. `JsonlExport` for the consumer, in-memory collector for tests).

---

## 6. Design Decisions

For every non-trivial choice: why, alternatives, why-worse-here, tradeoff,
assumption. Tagged `[E]` (evidence) / `[I]` (inference).

### D1. Ring buffer + background consumer instead of synchronous DB writes
* **Why:** hot path must be microseconds; SQLite sync writes are ~orders
  slower and jittery. Producer never blocks (`telemetry.py:7-10`) `[E]`.
* **Alternatives:** (a) synchronous commit per decision — kills latency and
  batching; (b) queue library (Celery/Redis) — heavyweight, new infrastructure
  for an embedded lib `[I]`.
* **Tradeoff:** bounded staleness (≤ ~one batch/100ms) and a small hard-kill
  loss window; loss becomes *possible under pathological overload* —
  compensated by drop-prioritization + counters + CRITICAL logs
  (`consumer.py:247-269`).
* **Assumptions:** the app tolerates ≤100 ms outcome-durability lag; a small
  loss tail on hard kill is acceptable; single-process deployment.
* **Known weakness:** MED-3 (drain_now/flush race) touches exactly this seam.

### D2. Deterministic exploration (`n % int(1/rate) == 0`) instead of PRNG
* **Why:** reproducibility; exact, measurable rates; test-friendly;
  no real randomness needed (`gatekeeper.py:231-245`) `[E]`.
* **Alternatives:** `random.random() < rate` — rate correct only statistically,
  unrepeatable, harder to test/audit.
* **Tradeoff:** exploration is *clocking* — a malicious/buggy caller could
  schedule around it; acceptable for an internal control plane `[I]`.
* **Assumption:** call ordering is the only fairness property needed.

### D3. Non-conformity & risk-control (`S = 1 - confidence`, empirical-risk prefix) instead of a raw confidence cutoff
* **Why:** raw "conf > 0.7" encodes no ground truth. Pairing decisions with
  outcomes and thresholding on *sorted empirical risk* binds the threshold to
  measured failure rates (`calibration.py:143-204`) `[E]`.
* **Alternatives:** logistic re-calibration, isotonic regression, plain
  conformal quantile. All valid; prefix-risk is simple, monotone, and needs no
  recalibration model `[I]`.
* **Tradeoff + weakness:** the *implemented* quantity is an empirical-risk
  plug-in; the docs' finite-sample claim is stronger than the estimator alone
  delivers (**MED-2**). Also excludes exploratory data, which biases sample
  size down until exploration lands outcomes.
* **Assumption:** calibration records are exchangeable/independent and
  collected under the same conditions as serving.

### D4. `min_sample_size` floor (default 100) with `q_hat = None` below it
* **Why:** a context with 3 samples can't support any honest threshold; the
  floor turns "no data" into "no delegation" (fail-closed) `[E]`.
* **Alternatives:** calibrate anyway, penalize, or hardcode — all worse on
  trust or complexity.
* **Tradeoff:** cold-start delay — every context must collect 100 *independent,
  non-exploratory* outcomes before it can delegate at all.
* **Assumption:** 100 is a sensible default for these decision types (a global
  constant in `policy.py:70`, overridable).

### D5. Materialized `joined_records` + `ON CONFLICT DO NOTHING`
* **Why:** calibration reads one flat table (O(read)), and re-runs are
  crash-idempotent via watermark + conflict rule (`database.py:687-701`) `[E]`.
* **Alternatives:** join at calibration-time (O(join) every pass);
  view-based (no fast index); event-store replay.
* **Tradeoff + weakness:** **MED-1** — late-arriving outcomes never overwrite a
  previously-joined NULL row, so they never calibrate.
* **Assumption:** outcomes mostly arrive before the next calibration run; fixed
  ordering in ops (calibrate-after-outcomes-batch).

### D6. SQLite, single file, WAL *off*
* **Why:** embedded durability with zero ops; single portable file is
  deliberately kept for sync/backup (`database.py:20-22`) `[E]`.
* **Tradeoff:** writer/reader contention in default journal mode → mitigated
  by single-writer discipline + 3× lock retry (`database.py:358-377`).
* **Assumption:** one process is the only writer (see §7 concurrency).

### D7. Policy as immutable YAML artifacts + atomic in-memory snapshot swap
* **Why:** auditable history, rollback, human-readable trust contracts;
  serving reads only the swapped dict so readers never see partial state
  (`policy.py:310-361`, `gatekeeper.py:247-254`) `[E]`.
* **Alternatives:** DB-only rows (no human/ops ergonomics); live objects
  (mutable, unsafe).
* **Tradeoff:** disk write per republish (fine at ops cadence); `policy_latest`
  needs a symlink/copy dance on Windows (`policy.py:421-427`); artifact write
  itself is non-atomic (**LOW-6**).

### D8. Strict validation funnels everywhere
* **Why:** bad input must fail *before* it reaches the store, and validation is
  centralized so every caller (API/CLI/batch) shares it
  (`outcomes.py:87-163`, `policy.py` `validate_policy`) `[E]`.

### D9. One outcome per decision (UNIQUE linkage)
* **Why:** the calibration math needs exactly one label per decision; double
  labeling would corrupt risk `[E]`.

### D10. Numpy only in calibration (not on the hot path)
* **Why:** import cost + vectorized sort/cumsum belong to the slow, batched
  stage, not the µs-critical one `[E]`.

---

## 7. Failure Scenarios

What the implementation *actually* does for each:

| Scenario | Behavior | Anchor | Assessment |
| --- | --- | --- | --- |
| **Invalid input** (bad confidence both-or-neither) | `ValueError` raised | `__init__.py:213-223` | loud, correct |
| **Invalid outcome value/source/metadata** | `InvalidOutcome*Error`; exit 2 in CLI | `outcomes.py:378-385,587-595` | loud, correct |
| **NaN / out-of-range confidence** | clamped to 0.0 (safe: escalates) | `gatekeeper.py:169-184` | graceful |
| **Missing data (unknown context / no q_hat / < min samples)** | `ESCALATE` | `gatekeeper.py:193-214` | fail-closed ✔ |
| **Decision not in ledger (log_outcome)** | `DecisionNotFoundError` → CLI exit 2 | `outcomes.py:478` | loud, correct |
| **Duplicate outcome for a decision** | blocked; single-outcome invariant | UNIQUE linkage | enforced (but see join staleness MED-1) |
| **Database failure (locked/busy)** | consumer retries ×3 then memory-backlog + cap + CRITICAL; ops queries retry 3× | `consumer.py:273-337`, `database.py:358-377` | graceful, eventually alerting |
| **Hard kill mid-batch** | up to ~1000 records/100ms lost; everything after commit survives; joins idempotent | `consumer.py`, `database.py:687-701` | bounded, documented (LOW-9) |
| **Concurrency (reload during evaluate)** | snapshot swap under RLock; in-flight calls finish on old snapshot | `gatekeeper.py:247-254` | safe |
| **Concurrent flush vs drain_now** | **race exists** (MED-3) | `consumer.py` | ❌ known |
| **Unexpected exception in gatekeeper** | caught → `ESCALATE` + exception log | `__init__.py:227-233` | fail-closed ✔ |
| **Policy artifact invalid on load** | `load_policy` raises before swap; old snapshot stays active | `gatekeeper.py:272` | roll-forward safe ✔ |
| **Duplicate artifact version** | `PolicyError` unless `force=True` | `policy.py:337-340` | guard |
| **Shutdown during work** | `_require_open` refuses; double shutdown guarded | `__init__.py:377,424` | safe |
| **External/network** | n/a (no network surface) | — | non-applicable |

---

## 8. Security

* **Authentication:** none — no users, no credentials, no sessions. The only
  "identity" is a 16-byte `context_hash` derived from the caller's context via
  `make_context_hash`; it is validated to be 16 bytes
  (`utils.validate_context_hash`).
* **Authorization:** statistical, not identity-based — the gate decides
  *whether this context may act* via risk budget; `ESCALATE` is the denial.
  Anyone who can write `policy_latest.yaml` can effectively rewrite
  authorization (trust boundary!).
* **Validation:** positive whitelists everywhere — outcome value coerced to
  `[0,1]` (`outcomes.py:87`), source must be an `OutcomeSource`, metadata must
  be JSON, policy files validated against schema-1.0 before load
  (`policy.py:175`).
* **Sensitive data:** none stored intentionally — only IDs, numeric
  confidence, action strings, and caller-supplied `metadata` (which *could*
  carry sensitive items; document that metadata is stored verbatim as JSON).
* **Trust boundaries:**
  1. App → ledger (trusted embedding; the app is the "user").
  2. Filesystem artifacts (attack surface: tampered YAML ⇒ changed serving
     behavior). Files are *validated*, not signed.
  3. Model/scorer → confidence (out-of-range handled defensively).
* **Attack surfaces:** (a) malformed policy YAML (rejected by validation);
   (b) malformed metadata JSON (rejected); (c) a malicious process sharing the
   SQLite file (no encryption, trusted-local assumption); (d) path choice of
   `db_path`/`policy_file` (operator-controlled, no traversal checks — operator
   is trusted).

---

## 9. Performance

* **Hot path (`evaluate`):** O(1), no object allocation on the decision path
  (`gatekeeper.py:157-158`); one RLock acquisition; measured ~1.05 µs without
  telemetry; p50 7.7 µs / p99 26.6 µs end-to-end with telemetry (benchmarks
  in `src/tests/test_gatekeeper.py:332`, `test_integration_week1.py:346,365`).
* **DB queries:** calibration reads one indexed flat table
  (`joined_records(context_hash)`); joins are materialized (no per-run join
  cost); all hot queries are parameterized + indexed.
* **Network calls:** none.
* **Computational complexity:** calibration per context is `O(n log n)`
  (argsort) in numpy (vectorized); 100k records × 100 contexts < 120 s.
* **Caching:** policy snapshot in-memory (hot path); ring buffer as the
  write-back cache; no external cache. `_maybe_warn` throttling protects logs.
* **Unnecessary work guards:** decision-type mapping uses an int code dict;
  `latency_us` timing skipped when telemetry is None
  (`gatekeeper.py:186`); numpy kept off the hot path.
* **Scalability constraints:** single SQLite writer (batch_insert) sets an
  upper bound (~118k rows/s single tx); ring buffer drop-first-exploratory is
  the overload relief valve; multi-process/replica requires externalizing this
  design (see section 11 alt B).
* **Verification in repo:** consumer flush ~60–62k records/s; batch drain
  53–72 µs; all budgets asserted in tests using pytest-benchmark.

---

## 10. Testing

* **Unit:** `src/tests/test_calibration.py` (threshold math incl. Wilson bound,
  edge cases), `test_gatekeeper.py` (five branches, exploration rate, reload
  thread-safety, latency benchmark), `test_consumer.py`, `test_telemetry.py`
  (ring buffer tiers/drops), `test_utils.py`, `test_policy.py` (validation),
  `test_outcomes.py` (validation + CLI + batch).
* **Integration:** `test_database.py` (schema, locking, thread-local conns),
  `test_joiner.py` (join semantics/idempotency), `test_pipeline.py`,
  `test_ledger.py` (facade), `test_storage_pipeline.py` (producer+consumer
  concurrency, shutdown, no-data-loss assertions).
* **End-to-end:** `tests/test_end_to_end.py` + `test_integration_week1.py`
  (full loop incl. exploration-rate, policy files exist+validate, drift).
* **Benchmarks:** pytest-benchmark in `test_gatekeeper` /
  `test_integration_week1` assert latency budgets.
* **Suite:** 308 tests, coverage 97% (1543 stmts / 53 miss), mypy --strict +
  flake8 + black gates clean.
* **Missing test cases (gaps):** MED-1 (late outcome re-join) and MED-3
  (concurrent drain/flush race) have no regression tests; stdlib `hashlib.
  blake3` path is untested (only PyPI fallback exercised on 3.14); no test
  drives the `policies` table history vs YAML dual-write.

---

## 11. Alternative Designs

### Alternative A — Synchronous write, no ring buffer/consumer
*Evaluate → DB write* inline, each call its own transaction, or with a
`threading` lock.
* **Complexity:** lowest (remove `telemetry.py`+`consumer.py`).
* **Maintainability:** fewer moving parts.
* **Performance:** every decision pays SQLite latency (~µs→ms with fsync);
  the 1 µs hot-path contract dies; write amplification ×N.
* **Testability:** simpler, but latency budgets vanish.
* **Scalability:** worse — the hot path becomes disk-bound.
* **Failure modes:** an I/O stall blocks the *producer* (bad); no drop
  questions (good).
* **Verdict:** the ring buffer + consumer *is* the way to keep the hot path
  fast; A is only reasonable if decisions are rare (an ops tool, not a gate).

### Alternative B — External durable queue (Kafka/Redis/Pulsar) feeding SQLite (or a real DB)
*Producer → broker → consumer → Postgres.*
* **Complexity:** high — new infra, connectors, retry/offset management.
* **Performance:** hot path pushes over the network (µs→~ms); broker adds
  hard ops dependency but kills the local-loss window and MED-3.
* **Scalability:** genuinely multi-process/auto-scalable — solves replica +
  writers.
* **Testability:** harder (broker in tests).
* **Maintainability/DevExp:** significant ops burden; contradicts "embedded
  single-file" pitch.
* **Verdict:** the right *next* step at multi-process scale; over-engineering
  for the single-process embedded goal the repo targets `[I]`.

### Alternative C — Probabilistic exploration + live-join calibration (no materialized table)
`random() < rate` for shadowing; calibration runs a real JOIN over
`decisions ⋈ outcomes` per context.
* **Complexity:** removes joiner + `joined_records` table (simpler schema).
* **Performance:** calibration scans+joins the corpus every run (O(total));
  exploration rate becomes statistical (unmeasurable exactly).
* **Testability:** rate assertions become flaky.
* **Failure modes:** pathological PRNG seeds; reservations about
  reproducibility.
* **Verdict:** the materialized-join + deterministic-exploration choices are
  deliberate reproducibility/performance wins; C trades both away.

**Overall:** the current design is reasonable for its stated goal — embedded,
single-process, auditable, µs-hot-path — and each alternative trades away one
of those properties. The weakest points are the *known* seams (MED-1, MED-3)
rather than the overall shape.

---

## 12. Learning Questions

> Answers are in **§ 12.4 Answer Key** at the end of this section.

### 12.1 Beginner (10)

1. What three values can `evaluate()` return, and what does `ESCALATE` mean?
2. Where does the `decision_id` for a decision come from?
3. What is the non-conformity score formula, and where is it computed?
4. How long is a decision's data "safe" in memory before it is durable?
5. What does `q_hat` represent, in one sentence?
6. What must be true before a context can ever `DELEGATE`?
7. How does `EXPLORE_SHADOW` differ from `DELEGATE`?
8. What is `policy_latest.yaml` and why does it exist?
9. Which two things does `log_outcome` verify before inserting?
10. What happens to `evaluate()` calls after `shutdown()`?

### 12.2 Intermediate (10)

11. Explain the steps of `calibrate()` in order, naming the four collaborators
    it touches.
12. What exactly does `Range of empirical risk prefix` mean — how is the
    "largest prefix with risk ≤ alpha" computed?
13. Why are `EXPLORE_SHADOW` records excluded from calibration? What does that
    imply about cold-start?
14. What does the joiner's `ON CONFLICT(joined_id) DO NOTHING` do when an
    outcome arrives *after* the join? (Name the consequence.)
15. How does the ring buffer decide *what* to drop under overload, and why?
16. Describe the consumer's flush conditions (both), the failure backoff, and
    the backlog cap behavior.
17. What interplay does `drain_now()` (10k cap) have with the consumer loop’s
    `pop_batch(1000)` — and where is the race?
18. When the pipeline detects drift, what happens to the *active* threshold?
19. How are `model_verification` outcomes treated differently, and why?
20. How does `reload_policy` make concurrent readers safe without blocking
    them?

### 12.3 Advanced (10)

21. Derive why, with `exploration_rate = 0.02`, exactly one in fifty calls
    explores — and why that makes the rate *exactly* measurable.
22. Critically assess: does `compute_threshold` implement a finite-sample
    guarantee, or an empirical-risk plug-in? Where does the documented claim
    overreach? (MED-2)
23. Design the fix for MED-1 so late outcomes eventually calibrate, and name
    the idempotency property you must preserve.
24. Explain the consumer's crash-loss window precisely (how many records can be
    lost, under what conditions) and what invariants survive it.
25. Why does `calibration.py` use numpy only here, and what would the
    import-weight cost be if it were imported by `gatekeeper`?
26. Trace the exact data dependency that makes `joined_records` a
    denormalization, and justify why it is updated via watermark instead of a
    view.
27. What are the trust boundaries, and which one does an attacker with write
    access to `data/policies/` cross?
28. Why is a 3-attempt `_run_with_lock_retry` used on reads but a memory-backed
    backlog on writes? Contrast the two philosophies.
29. What assumptions must hold for the empirical-risk threshold to be
    calibrated under *serving conditions* — and which are unverifiable from the
    repo?
30. If you were told "the ledger must now run in two processes sharing one
    SQLite file," which three invariants break first?

### 12.4 Answer Key (short answers)

1. `DELEGATE`, `EXPLORE_SHADOW`, `ESCALATE`. `ESCALATE` = the model may *not*
   act (fail-closed).
2. `utils.decision_id()` → a UUIDv7, minted inside
   `DecisionRecord.from_evaluation` (`telemetry.py:43`, `gatekeeper.py:367`).
3. `S = 1 - confidence`; computed at `gatekeeper.py:222` and normalized into
   `DecisionRecord.non_conformity` (`telemetry.py`).
4. Until the consumer flushes it; flush happens when the in-memory batch
   reaches `batch_size` (5000) or `flush_interval` (default 5.0s) elapses —
   so roughly ≤ 5s worst case, far less at rate.
5. The largest non-conformity score whose cumulative empirical risk stays
   ≤ `alpha` — the "highest risk you may take and still delegate".
6. The context must be: in the snapshot (known), `ACTIVE`, `sample_size ≥
   min_sample_size`, and have a non-None `q_hat` (`gatekeeper.py:193-214`).
7. It is a *measuring* path: same records flow out (tagged
   `EXPLORE_SHADOW`), but exotic behavior never delegates and its records are
   excluded from threshold calibration.
8. The pointer file for "the latest valid policy" in the artifact directory —
   what `load_latest_policy` and gatekeeper reloads read
   (`policy.py:363-368`).
9. That the decision exists in `decisions` (`DecisionNotFoundError`), and that
   value/source/metadata are valid (coerced/whitelisted).
10. They raise — `_require_open` fails after `shutdown()` (`__init__.py:424`).
11. `drain_now()` → `Joiner.join_decisions_and_outcomes()` →
    `CalibrationPipeline.run_calibration()` where each context gets
    `calibrate_context` + `detect_drift`, then `PolicyGenerator.generate_policy`
    → `Gatekeeper.reload_policy_from_file` → returns artifact path.
12. Sort scores; compute cumulative sum of losses; divide by prefix length to
    get prefix empirical risk; take the largest index whose value ≤ alpha;
    `q_hat` = that score (`calibration.py:174-199`).
13. They did not actually *decide* (no trust exercised); including them would
    make thresholds overly optimistic (exploratory data is typically the honest
    low-confidence tail). Implication: a context needs 100 *non-exploratory,
    independent* outcomes before trusting.
14. `DO NOTHING` keeps whatever was joined first — so a late outcome never
    overwrites a previously-joined NULL `outcome_value` ⇒ never calibrates
    (MED-1).
15. At ≥95% fill, evict the **exploratory** records first (they carry the least
    trust info), else the oldest; all evictions counted
    (`telemetry.py:162-204`).
16. Flush if `len(_batch) >= batch_size` **or** `now - last_flush >=
    flush_interval`; on failure back off ~`FAILURE_BACKOFF_S` (1s); if the
    backlog ever exceeds cap 100k, oldest records are dropped with CRITICAL
    logs (`consumer.py:216-269`).
17. Both call `ring_buffer.pop_batch` (1000 vs 10000) and both may invoke
    `_flush_to_db`; because the loop pops *by count* of its head batch while
    `drain_now` flushes concurrently, records appended between pop and flush
    can be popped-but-never-written — MED-3.
18. Nothing automatically: the pipeline still writes a new artifact (using the
    current q_hat); drift only emits a warning/summary flag
    (`pipeline.py:70-95`, `calibration.py:296-345`). (Arguably a design
    question — see §6.)
19. Excluded from calibration entirely: `_INDEPENDENT_OUTCOME_SOURCES`
    drops `MODEL_VERIFICATION` as self-confirming (`calibration.py:58-60`).
20. It replaces the whole dict reference under one short RLock; in-flight
    evaluations finish against the old snapshot, every later one sees the new —
    copy-on-write. (`gatekeeper.py:247-254`)
21. `_should_explore` advances a counter only for *eligible* calls and tests
    `n % (1/rate) == 0` → exactly call numbers 0,50,100… explore; rate is an
    exact ratio, and unit tests assert it precisely.
22. It computes an **empirical-risk prefix** (plug-in) and reports a Wilson
    coverage lower bound; the README/DESIGN wording "P(Loss>0) ≤ alpha" claims
    a stronger finite-sample property than this estimator alone delivers — fix
    by implementing a monotone-risk construction or rewording (MED-2).
23. Add a re-join pass (`WHERE outcome_value IS NULL AND EXISTS (outcome)`),
    or `INSERT … ON CONFLICT(joined_id) DO UPDATE`; preserve idempotency
    (re-runs safe) and update the tests that enshrine first-inserted outcomes.
24. Records live in the ring buffer + unflushed in-memory batch; on hard kill
    they are lost. Worst case ≈ batch capacity (up to a few thousand) within
    the flush window; anything committed is safe and joins are idempotent
    (LOW-9, MED-3).
25. Numpy is imported only by `calibration.py` for vectorized argsort/cumsum;
    importing it on the hot path adds ~tens-of-ms import + heavier memory,
    killing the µs startup/latency contract — the isolation is deliberate.
26. `joined_records` denormalizes decisions ⋈ outcomes so calibration reads one
    table; the watermark (`> last_joined`) + `DO NOTHING` gives incremental,
    crash-safe materialization cheaper than a per-run live join.
27. The three boundaries: app→ledger, ledger→filesystem artifacts, model→
    confidence. An attacker who can write `data/policies/` crosses boundary 2
    and can set `q_hat=1.0` ⇒ authorize everything (validation ≠ trust).
28. Reads retry a fixed number of times because a transient BUSY resolves; the
    write path instead holds records in memory with a cap+alert so *produced*
    decisions aren't dropped while still never blocking the producer — one
    philosophy is "avoid losing reads," the other "avoid losing writes without
    stalling."
29. Exchangeability/independence of calibration records, collecting under
    serving conditions, `model_confidence` being comparable across the
    explored vs delegated ranges — the last is only *assumed* and is exactly
    what `detect_drift` tries to check.
30. (1) single-writer assumption of `batch_insert`; (2) the in-process ring
    buffer / `threading.local` conns (no shared buffer); (3) the in-process
    policy snapshot reload — the other process would not see new policies. That
    is why a distributed design needs an external queue + externalized policy
    store (Alternative B).

---

## 13. Implementation Exercise

### Goal
Re-implement the **serving core** of the feature — the part that turns
`evaluate()` inputs into an action — from a written spec *without looking at*
`gatekeeper.py:152-245`. This tests architecture understanding: the fail-closed
rules, deterministic exploration, the non-conformity rule, and the snapshot
concurrency model.

### Scenario
You are given a concrete context type and a policy snapshot, and you must
decide for a caller exactly what `evaluate()` would decide.

```python
# Provided datum — do NOT edit
class Context:
    def __init__(self, is_active: bool, q_hat: float | None, sample_size: int, min_sample_size: int):
        self.is_active = is_active
        self.q_hat = q_hat
        self.sample_size = sample_size
        self.min_sample_size = min_sample_size
```

### Requirements (derive them from the architecture, don’t check the source)

Implement `Evaluator.evaluate(context_hash, confidence)` where:

1. **Clamping:** any `confidence` must be normalized to `[0.0, 1.0]` first
   (NaN and negatives → `0.0`; >1 → `1.0`).
2. **Fail-closed order** (must be checked in this order, all → `"ESCALATE"`):
   a. context hash not in the snapshot;
   b. context not active;
   c. `sample_size < min_sample_size`;
   d. `q_hat is None`.
3. **Deterministic exploration:** eligible calls (those that passed step 2)
   explore when `n % int(1 / exploration_rate) == 0`, where `n` counts only
   *eligible* calls. With `rate = 0.02`, calls 0, 50, 100, … return
   `"EXPLORE_SHADOW"`.
4. **Decision rule:** otherwise `"DELEGATE"` if `(1 - confidence) <= q_hat`,
   else `"ESCALATE"`.
5. **Snapshot concurrency:** your `reload_policy(new_snapshot)` must let in-flight
   `evaluate()` calls finish against the old snapshot and must be safe to call
   while `evaluate()` calls run concurrently (choose a mechanism — think
   "swap-the-reference atomically", not "mutate-in-place").
6. **Telemetry (stub):** return the action string; keep a side-channel note of
   `(context_hash, action)` you could later push to a queue.

### Inputs for your test run

* Snapshot: `h_a → Context(True, 0.10, 120, 100)`; `h_b → Context(False, 0.10,
  120, 100)`; `h_c → Context(True, None, 120, 100)`;
  `h_d → Context(True, 0.10, 10, 100)`.
* Rate `0.02`; over a fresh evaluator, make 60 calls alternating contexts in
  fixed order: `[h_a, h_b, h_c, h_d, h_a]` repeated 12 times, confidence 0.95,
  and note the per-context results and the exploration positions.

### Expected behavior (verify yourself, then write tests)

* `h_b`, `h_c`, `h_d` → always `"ESCALATE"`.
* `h_a` → `"DELEGATE"` for 1−0.95 = 0.05 ≤ 0.10, except at exploration slots.
* With 60 calls total and eligible = those at `h_a` positions (12 calls), the
  exploration slots occur at eligible-call numbers 0 and 50 → expect 2
  `"EXPLORE_SHADOW"` on `h_a`.
* After calling `reload_policy` with `h_a → Context(True, 0.005, 120, 100)` and
  a confidence of 0.80 (S = 0.20), `h_a` now → `"ESCALATE"`, while an
  `evaluate()` that started *before* the swap would still have seen `0.10`
  if it were already inside the locked section (you can test the "in-flight
  completes against old snapshot" property with threads + a slow path).

### Evaluation checklist (what understanding is really being tested)

- [ ] The five fail-closed branches and their *order* are reproduced exactly.
- [ ] `_should_explore` counts only **eligible** calls (not every call).
- [ ] The decision rule uses `non_conformity <= q_hat` (not `confidence >=`).
- [ ] Clamping happens *before* statistics. NaN handled.
- [ ] Snapshot swap is atomic reference replacement; no partial mutation.
- [ ] You can *explain why* an unknown context escalating is correct even
      though a new context is indistinguishable from a typo'd hash (trust-vs-
      convenience tradeoff from §6).

### Bonus (stretch)
Extend your Evaluator with a `metrics()` that reports per-decision-type
`calls` and `escalation_rate`, and a thread-safety test that runs 50 concurrent
`evaluate()` + 20 `reload_policy` calls without exception and with a consistent
snapshot story — then run it under a stress test like
`src/tests/test_storage_pipeline.py` does.

*Don’t open `gatekeeper.py` until your tests pass; then diff your branch
structure against `gatekeeper.py:152-245` and write down the one rule you got
different.*