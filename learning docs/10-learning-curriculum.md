# 10 — Learning Curriculum

A path from "never seen the repo" to "can redesign it". Each level lists
**learn-by-reading**, **learn-by-doing**, and **prove-it** (a task you should
be able to do cold at that level).

## Level 1 — Beginner (orientation)

> Goal: explain the system to a colleague in 5 minutes; run it end-to-end.

* Learn by reading: `01` → `02` → `05`. Then `src/docs/QUICKSTART.md`,
  `src/docs/DESIGN.md`, `README.md`.
* Learn by doing:
  * `python -m venv venv`, activate, `pip install -e ".[dev]"` (or `make install`).
  * Run the examples: `basic_serving`, `outcome_logging`, `stress_test`,
    `calibration_demo` (exit codes must be 0).
  * `python -m pytest` — see 308 passing; `make cov` to see 97%.
  * `make check` to run the full gate (format→lint→typecheck→test).
* Prove it:
  * "What in the system guarantees a bad decision never silently auto-runs?"
  * "Trace one decision from `evaluate()` to `joined_records`."
  * "Where do policy artifacts live, and how does one go live?"

## Level 2 — Intermediate (components)

> Goal: modify a single module correctly and explain the invariants that
> must not break.

* Learn by reading: `03`, `04`, `06`. Then each module in dependency order:
  `utils` → `telemetry` → `database` → `consumer` → `gatekeeper` → `outcomes`
  → `calibration` → `policy` → `pipeline` → `__init__`.
* Learn by doing:
  * Add a field to `DecisionRecord` at all four boundaries (record → buffer →
    batch → DB) — follow the plumbing to see every touch-point.
  * Write a JSONL-only reproducer using `JsonlExport` (no SQLite).
  * Simulate a `database is locked` and watch `_run_with_lock_retry` recover.
* Prove it:
  * "Change `batch_size` and predict the writes-per-second curve."
  * "Why is the gatekeeper's `q_hat` not the same as a confidence threshold?"
  * "What is non-conformity in this code, and which module owns its meaning?"

## Level 3 — Advanced (ownership)

> Goal: own a subsystem; reason about its sharp edges and concurrency.

* Learn by reading: `07`, `08`, `09`. Then the tests as documentation
  (`test_gatekeeper`, `test_storage_pipeline`, `test_integration_week1`).
* Learn by doing:
  * Fix one MED issue from `09` (start MED-1), with a regression test.
  * Add a new outcome source to `OutcomeSource` and thread it through.
  * Reproduce and fix LOW-3 (docstring/cap mismatch).
  * Add a drift-detection test where the context drifts and assert the
    pipeline's reaction.
* Prove it:
  * "Describe MED-3’s interleaving, then fix it without breaking `drain_now`."
  * "Explain why WAL-off + single-file + retry is coherent; when would you flip it?"
  * "Give the statistical argument for why `min_sample_size` exists."

## Level 4 — Senior (redesign)

> Goal: challenge the architecture; design the v2.

* Learn by reading: `09` + `11` (the gap list), then the CHANGELOG for the
  "why". Benchmarks as budgets: `src/docs/BENCHMARKS.md`.
* Learn by doing:
  * Redesign `DecisionLedger` to scale to multi-process serving with the
    least change (which seams break? which survive?).
  * Design the migration framework that MED-schema-change needs, without
    touching behavior.
  * Write the monotone-risk implementation to close MED-2, and the honest
    docs claim.
  * Design a Kafka/Durable-queue sink that keeps the same `RingBuffer` API.
* Prove it:
  * "Where exactly does the current guarantee overreach, and what’s the
    minimal honest claim?" 
  * "Which three changes would you make before taking this to 10x load?"
  * "Is the joiner better materialized or view-based at 10⁶ rows/day? Defend it."

## Suggested schedule (self-paced)

| Week | Focus | Exit task |
| --- | --- | --- |
| 1 | Orientation + run everything | 5-min tour to a peer |
| 2 | Hot path + telemetry + consumer | diagram the buffer→SQL pipeline |
| 3 | DB + join + outcomes | fix LOW-3; write a JSONL exporter test |
| 4 | Calibration math + policy | add a context-drift test |
| 5 | Facade + pipeline + orchestration | wire a custom sink via DI |
| 6 | Design review + `09` sweep | fix MED-1 with regression test |

## Key concepts you must be able to define from memory (flashcard list)

1. Non-conformity score and why it beats raw confidence.
2. `min_sample_size` and fail-closed escalation.
3. Deterministic exploration (`n % N == 0`) vs PRNG.
4. Ring-buffer tiering + drop-first-exploratory.
5. Batch tx (BEGIN/COMMIT) vs autocommit; why both exist.
6. `JOIN ... ON CONFLICT DO NOTHING` idempotency and its limitation.
7. Wilson coverage vs empirical risk — the MED-2 boundary.
8. Snapshot/copy-on-write concurrency for hot reload.
9. Immutable policy artifacts + `policy_latest` pointer.
10. `threading.local` connections with single dedicated writer.