# Changelog

All notable changes to Decision Ledger are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0rc1] - 2026-08-30

Distribution + public-release preparation.

### Added

- **Packaging** (`src/setup.py` + `MANIFEST.in`): complete package metadata
  (name, version, author, MIT license, Long description from `README.md`,
  Python 3.8+ classifiers, project URLs), runtime dependencies (numpy, PyYAML,
  blake3, uuid6) and `[dev]` / `[test]` extras (pytest, pytest-cov and friends).
- **Version management** (`decision_ledger/__version__.py`): single source of
  truth (`"1.0.0rc1"`); both `pyproject.toml`-free `setup.py` metadata and
  `decision_ledger.__version__` read from it instead of hardcoding. (PEP 440
  requires a real pre-release label, so the "1.0.0-mvp" release ships as
  `1.0.0rc1`.)
- **CLI entry point**: `decision-ledger-outcome` console script wrapping
  `decision_ledger.outcomes:main` (same interface as
  `python -m decision_ledger.outcomes`).
- **`pyproject.toml`** trimmed to build-system + tool configs (black, isort,
  flake8, mypy, pytest); metadata no longer duplicated, so `pip install .`,
  `python setup.py` and `python -m build` all agree.
- **`Makefile`** (optional, GNU make): `install`, `test`, `cov`, `format`,
  `lint`, `typecheck`, `check`, `sdist`, `clean` targets.
- Python classifiers now advertise 3.8+ (`from __future__ import annotations`
  is present in every package module); 3.9-3.14 remain the tested floor.

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