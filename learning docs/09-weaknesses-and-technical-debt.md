# 09 — Weaknesses & Technical Debt

Ranked `[HIGH]` / `[MED]` / `[LOW]` for the purposes of a senior reviewer.
Findings from the final code review are reproduced here as the canonical debt
list; each item has code anchor(s) and a fix sketch.

## MED-1 — Late outcomes never feed calibration (joiner staleness)

* **Where:** `database.py:687-701` (`materialize_join`, `ON CONFLICT(joined_id)
  DO NOTHING`), `pipeline.py:145-148`.
* **Behavior:** a decision joined while its outcome is still pending gets
  `outcome_value = NULL` permanently. When the outcome arrives later, the
  conflict handler skips the row, so the late outcome never reaches
  `joined_records` → never calibrates.
* **Impact:** for slow-label pipelines (human review, real ground truth), a
  meaningful fraction of data is silently excluded from threshold updates.
* **Fix sketch:** re-join pass on rows where `outcome_value IS NULL` but the
  linked outcome now exists; or `INSERT ... ON CONFLICT(joined_id) DO UPDATE`;
  or a calibration-time live join. Tests currently enshrine the first-inserted
  outcome — update those as well.

## MED-2 — The documented guarantee is stronger than the estimator

* **Where:** `calibration.py:143-204` (`compute_threshold`), README + DESIGN.
* **Behavior:** docs claim a finite‑sample "P(Loss > 0) ≤ alpha"; the code
  computes the largest empirical‑risk prefix with Wilson coverage bound
  reported alongside. The raw empirical‑risk prefix is a plug‑in estimate, not
  by itself the CRC‑style monotone‑risk quantile.
* **Impact:** a reader could guarantee a property the estimator does not
  mechanically provide; on small n the Wilson bound is the honest statement;
  on large n the difference is small in practice.
* **Fix sketch:** either (a) implement the monotone‑risk / tightened
  construction and state that, or (b) reword docs/docstrings to the precise
  empirical‑risk + Wilson property.

## MED-3 — Narrow data-loss race in the consumer flush path

* **Where:** `consumer.py:273-337` (`drain_now` + `_flush` interplay).
* **Behavior:** the flush loop pops the head batch *by count* (`len(rows)`),
  while `drain_now` can flush concurrently; a record appended between the pop
  and the drain could be popped-then-never-written.
* **Impact:** narrow, but real; violates the "never silently lose what we
  hold" invariant in exactly one interleaving.
* **Fix sketch:** flush under a single lock (or pop-by-identity), so count-based
  batching and synchronous drains serialize.

## LOW list

| # | Finding | Anchor | Note |
| --- | --- | --- | --- |
| LOW-1 | `z_score` hardcoded 1.96 (95%) vs `confidence_level` | `calibration.py:136-137` | confidence_level param exists; z not derived |
| LOW-2 | README says "SQLite (WAL)" but code is WAL‑less by design | README vs `database.py:20-22` | doc/code mismatch; MED‑2-adjacent |
| LOW-3 | `drain_now` docstring implies unbounded; capped at 10k | `consumer.py` drain_now | doc vs cap mismatch |
| LOW-4 | pipeline uses bare `print` | `pipeline.py:90` | logging discipline: use `logger` |
| LOW-5 | placeholder URLs in pyproject (homepage/repo) | `pyproject.toml` | polish before public release |
| LOW-6 | policy artifact write not atomic (no temp+rename) | `policy.py:358` | torn-file window for `policy_<ts>.yaml` |
| LOW-7 | stdlib `hashlib.blake3` path untested | `utils.py` | only PyPI-blake3 path exercised (3.14 lacks it) |
| LOW-8 | `*_stats.json` not gitignored | repo state after runs | noise in status/diffs |
| LOW-9 | crash durability window: poll→commit means ≤1 batch lost on hard kill | `consumer.py` | documented as acceptable, should be in README |
| LOW-10 | single-file DB + external sync (OneDrive) mid-tx risk | `database.py:20-22` | torn-state window; idempotent recovery mitigates |
| LOW-11 | `sqlite3` `:memory:` + WAL/thread interactions in tests | tests using `:memory:` | correctness with default journal depends on single-writer discipline |
| LOW-12 | no migration framework; schema pro-version constant | `database.py` DDL | first schema change = manual migration |

## Structural debt

* **`__init__.py` is 429 lines** — the facade mixes wiring, API, and
  normalization; fine for a v1, but growing responsibilities will outgrow it.
  `[I]`
* **`database.py` is 718 lines** with lifecycle+schema+lock+queries+joiner —
  cohesive but thick; candidates to split: `schema.py`, `joiner.py`.
  `[I]`
* **Two identities for contexts**: human-readable `context` string and hashed
  `bytes` key (black-box in gatekeeper dict) — clear in code, easy to confuse
  in tests/logs. `[E]`
* **Guarantee surface mismatched in 2 docs places** (README WAL claim, DESIGN
  guarantee claim) — one doc sweep closes both. `[E]`

## What is *not* a concern (checked)

* Hot-path lock contention — single RLock measured ~1 µs; benchmarks vetted.
* Concurrency — stress tests green (`test_storage_pipeline`,
  `test_concurrent_policy_reload_without_crash_or_data_loss`,
  `test_thread_safety_concurrent_reads_during_reload`).
* Data loss on *normal* operation — batch tx + retry + in-memory backlog with
  critical logging.
* Calibration correctness at scale — 100k×100 ctx < 120 s and vectorized.

## Recommended fixes, ordered

1. **MED-1** (real-world correctness): two-hour fix, highest value.
2. **MED-2** (honesty of the guarantee): choose implement-vs-reword.
3. **MED-3** (flush race): small lock/identity fix, test + assert.
4. LOW sweep (2–3): doc mismatches + gitignore + print→logger cost minutes.