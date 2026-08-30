# 07 — Architecture Decisions & Tradeoffs

Every important decision here is tagged:
`[E]` = directly evidenced by the repository · `[I]` = engineering inference ·
`[A]` = unverifiable assumption.

## 7.1 Decisions inventory (decision → chosen → rejected-alternative)

| # | Decision | Chosen | Rejected alternative | Anchor |
| --- | --- | --- | --- | --- |
| 1 | Packaging | src‑layout + `py.typed` | flat `decision_ledger/` at root | `pyproject.toml:72-78` |
| 2 | Storage | embedded SQLite, single file | Postgres / external DB | `database.py:7-23` |
| 3 | Journal | WAL **off** | WAL journal mode | `database.py:20-22` |
| 4 | Writing | per‑thread connections, autocommit + batched tx | single global connection / full WAL | `database.py:252-281`, `493-502` |
| 5 | Locking | RLock + snapshot swap on the hot path | fine-grained per-context locks | `gatekeeper.py:124,188` |
| 6 | Exploration | deterministic call‑count | PRNG sampling | `gatekeeper.py:18-21` |
| 7 | Telemetry hot path | lock‑free bounded ring buffer | direct DB write per decision | `telemetry.py` |
| 8 | Consumer | background poll+flush | blocking synchronous write | `consumer.py` |
| 9 | Calibration | conformal non‑conformity prefix‑risk | plain "confidence ≥ 0.7" heuristic | `calibration.py:143-204` |
| 10 | Sample floor | `min_sample_size` → escalate | calibrate with any n | `calibration.py` |
| 11 | Guarantee wording | "P(Loss > 0) ≤ alpha" in docs | "empirical risk ≤ alpha" only | README / DESIGN / `calibration.py` |
| 12 | Policy rollout | immutable YAML artifacts + `policy_latest` | database-only state | `policy.py` |
| 13 | Policy states | ACTIVE/DRAINING/REVOKED | single boolean | `policy.py` |
| 14 | Failure mode | fail‑closed (ESCALATE) | fail‑open (DELEGATE) | `gatekeeper.py:193-214` |
| 15 | Outcome cardinality | one outcome per decision (UNIQUE FK) | many‑to‑many | `outcomes.py` |
| 16 | Join | materialized LEFT OUTER + `DO NOTHING` | view / on‑the‑fly join | `database.py:687-701` |
| 17 | Versioning | dynamic from `__version__.py` | static in two places | `pyproject.toml:69-70` |
| 18 | Error handling | exception + retry + never‑silently‑drop | swallow/log only | `consumer.py:273-337` |
| 19 | API noise | small facade (evaluate/log_outcome/calibrate/shutdown) | expose raw modules | `__init__.py` |
| 20 | CLI surface | single outcome CLI | full management CLI | `outcomes.py` |

## 7.2 Dossier on the four most consequential

### D-2/D-3: SQLite, single file, WAL **off**

* Chosen: embedded SQLite; synchronous durability with autocommit; journal WAL
  off because **the DB must remain a single portable file** (OneDrive sync,
  backups, ship-it-around portability). `[E]` (comment at `database.py:20-22`
  states "WAL is disabled so the DB stays a single file").
* Rejected: external DB (operations burden, license not applicable, destroys
  "embedded" pitch) `[I]`; WAL (creates `-wal`/`-shm` companion files, breaks
  the single-file promise) `[E]`.
* Tradeoff paid: writer/reader contention in default journal mode — mitigated by
  single-writer design + `_run_with_lock_retry` (3 attempts) `[E]`
  (`database.py:358-377`). Throughput still measured ~118k rows/s single tx.
* Risks: sync-based replication of the single file while a batch is mid-commit
  can catch a torn state; mitigated because batches are small and idempotent
  (`DO NOTHING` + watermark). `[I]`

### D-5/16: Snapshot concurrency + materialized join

* The gatekeeper swaps immutable dicts under one lock ⇒ readers never block
  each other and never observe partial reloads. The Joiner materializes
  decision↔outcome pairs into a flat table so calibration is O(read), not O(join).
* Tradeoff: **stale outcomes are frozen forever** (MED-1 in `09`): a decision
  joined before its outcome arrives keeps `NULL` outcome. Because this is
  idempotent-on-purpose, the fix (re-join pass for NULL-outcome rows) is
  straightforward but is **not** implemented. `[E]`
* Alternative considered: calibration queries join live at read time — rejected
  because every calibrate pass would scan+join the whole corpus; the materialized
  approach wins on latency and is still correct for monotonic input. `[I]`

### D-9/D-11: the statistical core - what it actually computes

Two facts that matter for anyone reading the docs:

1. **Implemented:** (filter to independent non‑exploratory records →
   `min_sample_size` gate → sort by non‑conformity → cumulative-loss prefix →
   largest prefix whose *empirical* risk ≤ `alpha`) = `q_hat`
   (`calibration.py:143-204`). It reports a Wilson coverage lower bound.
2. **Documented:** "P(Loss > 0) ≤ alpha" as a finite‑sample guarantee
   (README + DESIGN).

The tension (MED‑2 in `09`): a raw empirical‑risk prefix is a *plug‑in*
estimate, not by itself a distribution‑free finite‑sample bound; a monotone‑risk
construction (e.g., adding a margin/tightening, or a CRC‑style quantile over
the empirical CDF) or an honest "empirical risk with Wilson bound" claim is
needed to make the docs match the math. The *implementation* is
reasonable; the *claim* is stronger than the estimator alone delivers. `[I]`

### D-10: `min_sample_size` — the operational safety valve

Escalating when a context has too few samples is a deliberate, cheap,
fail‑closed decision: a context with 3 paired outcomes is simply not yet
trustworthy enough to delegate — regardless of what q_hat would say. This is
where the system shows it is engineering the *decision*, not just the math. `[E]`

## 7.3 The tradeoff matrix (design tensions, by design)

| Tension | Resolution in code |
| --- | --- |
| Hot path latency vs. durability | ring buffer (fast) → consumer (durable) |
| Single‑file DB vs. WAL concurrency | WAL off + retry + single writer |
| Idempotent joins vs. late outcomes | `DO NOTHING` (idempotent) — late outcomes lost to calibration (MED-1) |
| Exploration vs. purity of decisions | exploratory records tagged & excluded from calibration |
| Determinism vs. unpredictability | call-count exploration — measurable, fakeable |
| Batch staleness vs. write amplification | flush every 100ms / 1000 rows |
| Docs vs. math | guarantee wording overreaches (MED-2) |
| Sync portability vs. write concurrency | single file (OneDrive) — contention priced in |

## 7.4 Decisions that look odd until you understand the constraint

* **WAL off, against SQLite folklore** — explained by the single-file sync
  requirement (D-3). Flipping this requires also revisiting D-2 (external DB).
* **RLock held during record()** — the lock is short; the hot-path benchmark
  (~1µs) is the argument. `lock` vs `RLock` choice is documented in
  `gatekeeper.py`.
* **Deterministic exploration even for "shadow"** — because EXPLORE_SHADOW
  outcomes still feed calibration (as exploratory records) and must be
  reproducible. `[I]`
* **Two policy stores** (DB table + YAML) — DB = journal; YAML = serving
  interchange. Both exist because the artifact is the source of trust. `[E]`

## 7.5 If you were redesigning today

* Replace the docs’ finite-sample claim with the estimator’s true property, or
  implement the monotone-risk form. (MED-2 fix.)
* Add a re-join pass / upsert for late outcomes. (MED-1 fix.)
* Consider optional WAL (opt-in) for write-heavy single-host use; keep single
  file mode as the default. 
* Add an explicit migration framework before any schema change ships — today a
  schema change is a manual migration. `[I]`