# Changelog

All notable changes to Decision Ledger are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- Nothing yet.

## [0.1.0] - 2026-08-30

Initial production-ready release.

### Added

- **Gatekeeper** (`gatekeeper.py`): thread-safe hot-path `evaluate()` over a
  per-context policy; returns `DELEGATE` / `ESCALATE` / `EXPLORE_SHADOW` and
  fails closed for unknown or under-calibrated contexts.
- **Split Conformal Risk Control** (`calibration.py`): finite-sample
  per-context thresholds (`q_hat`) with a Wilson lower bound, plus
  `detect_drift()` over active-vs-full accuracy.
- **Telemetry ring buffer** (`telemetry.py`): bounded, lock-free-ish
  ingestion with drop accounting and backpressure tier metrics.
- **Batch consumer** (`consumer.py`): background drain loop writing to SQLite
  (and optional JSONL), 10k-record `drain_now()` cap, live flush/backlog
  metrics.
- **SQLite persistence** (`database.py`): `decisions`, `outcomes`,
  `joined_records`, `policies` tables with FK constraints, WAL, lock-retry
  with graceful `DatabaseError` degradation, and maintenance helpers.
- **Outcome collection** (`outcomes.py`): validated single and batched
  outcome logging, decision-outcome joining, plus a
  `python -m decision_ledger.outcomes` CLI.
- **Versioned policies** (`policy.py`): schema-1.0 YAML artifacts, atomic
  writes, latest-pointer resolution, rollback, and context-hash invalidation.
- **Orchestrator** (`pipeline.py`, `__init__.py`): `DecisionLedger` façade
  wiring evaluate -> telemetry -> consumer -> outcomes -> calibration ->
  policy reload, with `stats()` and `shutdown()`.
- **Docs**: `docs/RUNBOOK.md` (ops), `src/docs/API_REFERENCE.md` (verified
  reference), plus design, architecture, examples, benchmarks, and code-style
  guides.
- **Tests**: unit + integration suites in `src/tests/` and end-to-end
  integration/performance/stress tests in `tests/` (>= 96% line coverage).

### Performance (measured, see `src/examples/stress_test.py`)

- evaluate() hot path: p50 ~8 us, p99 ~20 us (budget 1 ms), ~90-110k evals/s
  per process; 1000 concurrent evaluations in ~11 ms with zero drops.
- Calibration of 100k joined records: ~1.3 s (~76k records/s, 100 contexts).

### Quality gates

- `black` (line length 100), `isort`, `flake8` clean.
- `mypy --strict` clean on the package and examples (no `type: ignore`).
- `mypy` intentionally scoped to the package and examples; the test suite is
  verified at runtime by `pytest` and excluded by design.

[0.1.0]: https://github.com/example/decision-ledger/releases/tag/v0.1.0