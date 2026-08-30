# 08 — Crosscutting Concerns

## 8.1 Security

* **Internal data only.** No network surface: no HTTP, no RPC, no sockets.
  Attack surface = the filesystem (DB + policy YAML) and the embedding process.
  `[E]`
* **Path handling.** DB path and artifact paths are caller-supplied; validation
  is on the *content* (policy schema), not on path traversal. An operator that
  can write policy YAML can influence serving behavior — treat artifact
  directories as trusted, restricted, versioned. `[I]`
* **Secrets.** The package never stores credentials/secrets; risk is
  downstream. There is no logging of decision *content* — only ids, numeric
  confidence, and metadata validated as JSON. `[E]`
* **Validation choke-points**: `validate_policy` (schema/type/value), outcome
  `metadata` JSON parsing, `normalize` on confidence. All are positive
  whitelists. `[E]`

## 8.2 Reliability

* **Composition-root resilience.** `DecisionLedger.evaluate` wraps the gatekeeper
  in a catch-all that returns `ESCALATE` on unexpected exceptions
  (`__init__.py:225-233`) — the system degrades to *human/stronger model*, never
  to *blind passenger*. A knob to *disable* evaluation after repeated failures
  is a TODO in the design docs.
* **Consumer durability.** Retry (3×) then hold-in-memory with a 100k cap and
  `CRITICAL` logging — explicit, observed, non-silent degradation
  (`consumer.py:273-337`).
* **Crash windows.** Between poll and commit, up to one batch (~1000 rows /
  100 ms) may be lost on hard kill — documented in LOW‑8 of `09`. Calibration,
  joins and artifacts are crash-idempotent (watermark + `DO NOTHING`).
* **Thread hygiene.** Single dedicated consumer thread writes alone; all other
  threads use their own connection; `threading.local` prevents cross-thread
  cursor sharing.
* **Single-file DB + OneDrive** — flips the fragility upside: the whole state is
  one file that can be copied/synced; no companion files (WAL off). Tradeoff:
  torn-state risk on mid-write sync (see D-2/D-3 and 09-LOW-10).

## 8.3 Performance (evidence chain)

Measured during the review pass (Python 3.14, dev box) and cross-checked
against `src/docs/BENCHMARKS.md`:

| Path | Measurement | Reference |
| --- | --- | --- |
| `evaluate()` no telemetry | 1.05 µs mean | `src/tests/test_gatekeeper.py:332` bench |
| `evaluate()` e2e w/ telemetry | p50 7.7 µs, p99 26.6 µs | `test_integration_week1.py:346,365` |
| `pop_batch(1000)` | 53–72 µs | ring buffer bench |
| SQLite batch insert | ~118k rows/s (single tx) | consumer bench |
| Consumer durable flush | ~60–62k records/s | stress runner / readme |
| calibrate 100k / 100 ctx | < 120 s | week-1 integration |

Performance invariants visible in code:
* Hot path is allocation-light by design (frozen `DecisionRecord`, no JSON on
  the path unless telemetry enabled). `[I]`
* Numpy is used for the calibrate quantile computation (vectorized
  argsort/cumsum). `[E]`
* The consumer *never* blocks the producer — bounded ring buffer with
  documented drop-first behavior only on overload.

## 8.4 Testability & quality gates

| Gate | Command (Makefile) | Evidence of use |
| --- | --- | --- |
| Format | `make check-format` (black --check) | clean |
| Lint | `make lint` (flake8) | clean (E203/W503 ignored) |
| Types | `make typecheck` (mypy --strict on pkg+examples) | 0 issues, 3.14 |
| Tests | `make test` (pytest -k "not benchmark") | 308 collected, exit 0 |
| Coverage | `make cov` | 97% (1543 stmts, 53 miss) |
| Docs | docstring audit script | 0 missing |
| Build | `make sdist` (python -m build) | 1.0.0rc1 sdist+wheel OK |

Test-suite layering:
* unit/integration under `src/tests/` (incl. concurrency stress, latency
  benchmarks, storage-pipeline).
* full e2e under `tests/test_end_to_end.py`.

Notable stress coverage: `test_storage_pipeline` (producer+consumer
concurrency, shutdown, no data loss), `test_concurrent_policy_reload_without
_crash_or_data_loss`, `test_thread_safety_concurrent_reads_during_reload`.

## 8.5 Infrastructure & operations

* **No hosted CI** in the repo — the makefile + verification-checklist doc serve
  as the gate; can be ported to any CI that runs Python 3.8–3.14. `[E]`
* **Distribution**: `pyproject.toml` (PEP 621) + `setup.py` mirror; sdist
  contains `src/docs` + examples via `MANIFEST.in`; `py.typed` advertises type
  info.  `[E]`
* **Python versions**: requires ≥3.8 (`from __future__ import annotations`),
  developed/verified 3.14; blake3 falls back to the PyPI wheel on <3.14;
  UUIDv7 falls back to `uuid6` on <3.14. `[E]` (`utils.py`)
* **Observability**: decision stats (counts, escalation rates via
  `get_metrics`), `*_stats.json` snapshots on shutdown, telemetry ring buffer
  tier warnings, consumer backlog CRITICAL logs.
* **Runtime files** in `data/policies/` and beside test DBs are gitignored
  (`_stats.json` NOT gitignored — see 09-LOW-4).
* **Runbook**: `docs/RUNBOOK.md` covers CLI + lifecycle; `src/docs/QUICKSTART.md`
  + examples are the onboarding path.

## 8.6 Compliance-adjacent properties (design intent)

* Auditability: every decision + outcome is append-only and id-represented
  (UUIDv7), policy artifacts are immutable + versioned → the ledger is a real
  ledger. `[E]`
* Data minimisation: only numbers/ids/metadata; no model prompts/outputs stored
  unless the operator logs them via outcome metadata. `[E]`
* Explainability: `joined_records` lets you reconstruct why a decision was taken
  (confidence → non-conformity → q_hat → action). `[I]`