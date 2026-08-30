# Decision Ledger

A systems primitive that answers one question with formal statistical
guarantees: **when is a small model safe to trust for control-plane
decisions?**

Small models (1–7B) routinely handle control-plane work — routing, judging,
speculating, mutating, summarizing, abstaining — to cut cost and latency.
"Confidence > 0.7" is not a safety argument. The Decision Ledger records every
decision, links it to independent outcomes, computes delegation thresholds
with **Split Conformal Risk Control**, and enforces them through a fail-closed
gatekeeper.

**Guarantee.** For a user-set risk budget `alpha`:

```
P(Loss(Decision, GroundTruth) > 0) <= alpha
```

finite-sample, no distributional assumptions, computed over exchangeable
calibration data.

## Final feature set

| Area          | What it does                                                              |
| ------------- | ------------------------------------------------------------------------- |
| Gatekeeper    | Hot-path `evaluate()` -> `DELEGATE` / `ESCALATE` / `EXPLORE_SHADOW`        |
| Fail-closed   | Unknown context or insufficient data always escalates                     |
| Telemetry     | Every evaluation captured in a bounded, lossy-if-overrun ring buffer      |
| Storage       | Background `BatchConsumer` flushes to durable SQLite (WAL) + optional JSONL|
| Outcomes      | Independent labels (human / task_metric / user_report) linked by `decision_id` |
| Calibration   | Split Conformal Risk Control: per-context `q_hat` + Wilson lower bound     |
| Drift         | `detect_drift()` compares active-vs-full-range accuracy post-recalibration|
| Policy        | Versioned schema-1.0 YAML artifacts, atomic writes, reload, rollback      |
| Exploration   | Epsilon shadow sampling removes selection bias (no user risk)             |
| Orchestrator  | `DecisionLedger` façade wires evaluate -> store -> outcomes -> calibrate -> reload |
| Ops           | `stats()`, final-stats snapshot on `shutdown()`, 100k-record CLI, runbook  |

## Performance (measured)

| Metric                           | Value                                  |
| -------------------------------- | -------------------------------------- |
| `evaluate()` p50                  | ~8 µs (budget 1 ms)                    |
| `evaluate()` p99                  | ~20 µs                                 |
| Sequential throughput             | ~109,000 evals/s per process           |
| Concurrent (8 workers, 1000 evals)| ~90,000 evals/s, **0 dropped**         |
| Calibration, 100k joined records  | ~1.3 s (~76,000 records/s, 100 contexts)|

Measured on a dev workstation with the end-to-end suite; reproduce with
`python src/examples/stress_test.py` and `pytest tests/test_end_to_end.py -m "benchmark or slow" -s`.

## Quick start (3 minutes)

```bash
# 1. Install
python -m venv venv
venv\Scripts\activate              # Windows   (source venv/bin/activate on macOS/Linux)
pip install -e ".[dev]"            # editable package + dev/test tools

# 2. Smoke test
python src/examples/basic_serving.py      # delegation enforcement, 1000 requests
```

That's a working ledger in two commands. To build one from scratch:

```python
import tempfile
from decision_ledger import DecisionLedger, make_context_hash

with tempfile.TemporaryDirectory() as tmp:
    ledger = DecisionLedger(f"{tmp}/ledger.db", auto_start_consumer=False)
    ctx = make_context_hash("qwen-7b", "routing")     # 16-byte context id
    print(ledger.evaluate(ctx, confidence=0.95, decision_type="route"))
    # 'ESCALATE'  -- empty policy: fail-closed until calibrated
    ledger.shutdown()
```

Full flows — calibrate, serve, log outcomes, reload, keep trust in check:

```bash
python src/examples/calibration_demo.py    # learn -> serve -> learn again
python src/examples/outcome_logging.py     # outcomes -> join -> match rate
python src/examples/stress_test.py         # 10k decisions, durability check
```

## Architecture

```
 request ──▶ Gatekeeper.evaluate()  S=1-confidence vs q_hat
                │ delegate ──▶ small model ──┐
                │ escalate ─▶ frontier model ─┤
                └─▶ RingBuffer (every decision logged)
                       │
                       ▼  async
                 BatchConsumer ──▶ SQLite (durable) · JsonlExport ─▶ JSONL
                       │
                       ▼
                 Outcome Collector ──▶ Joiner (by decision_id)
                       │
                       ▼
                 Conformal Calibrator ──▶ Policy YAML (q_hat per context)
                       │
                       └─▶ Gatekeeper.reload_policy()  (~atomic swap)
```

## Project layout

```
decision_ledger/
├── src/
│   ├── decision_ledger/        # the package
│   │   ├── gatekeeper.py       # hot-path enforcement (fail-closed)
│   │   ├── telemetry.py        # ring buffer + decision records
│   │   ├── consumer.py         # batch drain: SQLite consumer + JSONL export
│   │   ├── database.py         # SQLite persistence (decisions/outcomes/joins)
│   │   ├── outcomes.py         # outcome collection + joining + CLI
│   │   ├── calibration.py      # Split Conformal Risk Control + drift
│   │   ├── policy.py           # versioned policy artifacts (YAML)
│   │   ├── pipeline.py         # calibrate -> publish -> hot reload
│   │   └── __init__.py         # DecisionLedger orchestrator
│   ├── tests/                  # unit + integration suite
│   ├── examples/               # runnable demos
│   └── docs/                   # design, API, runbook, benchmarks, style
├── tests/test_end_to_end.py    # integration + performance + stress suite
├── docs/RUNBOOK.md             # operations runbook
├── pyproject.toml              # metadata + black/isort/flake8/mypy/pytest config
├── requirements.txt
├── setup.py                    # legacy shim (metadata lives in pyproject)
├── LICENSE                     # MIT
└── CHANGELOG.md
```

## Tutorial

The full control loop in five steps:

```python
from decision_ledger import (
    CalibrationRecord, ConformalCalibrator, DecisionLedger,
    Gatekeeper, PolicyGenerator, make_context_hash, policy_from_results,
)

# 1. Your serving context -- changing any factor creates a NEW context.
ctx = make_context_hash("qwen-7b", "routing", prompt_template_version="v3")

# 2. Calibrate with labeled decisions (independent, non-exploratory).
records = [CalibrationRecord(ctx, 0.05, 0.0), CalibrationRecord(ctx, 0.45, 1.0)]
result = ConformalCalibrator(target_alpha=0.05, min_sample_size=500)\
    .compute_threshold(records)
serving = policy_from_results({ctx: result}, version_id=1)
# Below min_sample_size the context stays INACTIVE and escalates
# (fail-closed); delegate only once enough exchangeable labels exist.

# 3. Enforce: delegate only when S = 1 - confidence <= q_hat.
gk = Gatekeeper(serving.contexts, exploration_rate=0.02)
action = gk.evaluate(ctx, 0.95, "route")
#   GateAction.DELEGATE        -> use the small model
#   GateAction.ESCALATE        -> use the frontier model (fail-closed)
#   GateAction.EXPLORE_SHADOW  -> run both, serve frontier, log counterfactual

# 4. Or use the orchestrator, which also persists and reloads automatically.
ledger = DecisionLedger("ledger.db", auto_start_consumer=False)
ledger.evaluate(ctx, confidence=0.95, decision_type="route")
ledger.consumer.drain_now()          # make the decision durable
decision_id = ledger.database.execute_query(
    "SELECT decision_id FROM decisions LIMIT 1"
)[0]["decision_id"]
ledger.log_outcome(decision_id, 1.0, outcome_source="human")
ledger.calibrate()                   # drain, join, recalibrate, publish, reload
stats = ledger.stats()               # operational snapshot
ledger.shutdown()

# 5. Publish a policy artifact and hot-swap the gate.
artifact_path = PolicyGenerator("policies").generate_policy(
    {ctx: result}, policy_version="20260830-100000"
)
gk.reload_policy_from_file(artifact_path)   # atomic swap
```

## Docs

| Doc                                                                 | What's in it                                        |
| -------------------------------------------------------------------- | --------------------------------------------------- |
| [API Reference](src/docs/API_REFERENCE.md)                            | Verified, runnable reference for the whole package  |
| [Runbook](docs/RUNBOOK.md)                                            | Deployment, monitoring, troubleshooting, incidents  |
| [Design](src/docs/DESIGN.md)                                          | Problem, guarantees, architecture, trade-offs       |
| [Architecture (Week 1)](src/docs/ARCHITECTURE_WEEK1.md)               | Gatekeeper + ring buffer, latency, exploration      |
| [Quickstart](src/docs/QUICKSTART.md)                                  | 5-minute setup with runnable examples               |
| [API](src/docs/API.md)                                                | Full reference                                      |
| [Gatekeeper API](src/docs/API_GATEKEEPER.md)                          | Reference + common mistakes                         |
| [Telemetry API](src/docs/API_TELEMETRY.md)                            | Ring buffer + records, performance                  |
| [Examples](src/docs/EXAMPLES.md)                                      | End-to-end patterns                                 |
| [Benchmarks](src/docs/BENCHMARKS.md)                                  | Measured budgets and methodology                    |
| [Code Style](src/docs/CODE_STYLE.md)                                  | Formatting, typing, docstrings, logging             |
| [License](LICENSE), [Changelog](CHANGELOG.md)                         | —                                                  |

## Development

```bash
black src tests examples        # format (line length 100)
isort src tests examples        # sort imports
flake8 src tests                # lint (see .flake8)
mypy                            # strict typecheck, package
mypy src/examples src/setup.py  # strict typecheck, examples
pytest                          # run the full suite (incl. benches)
coverage erase
coverage run --branch -m pytest -k "not benchmark"    # measure coverage
coverage report -m -i                                  # view report
```

Status: **black + isort + flake8 clean; mypy --strict clean (no `type: ignore`);**
**308 tests pass; >= 96% line coverage.**

> **mypy scope.** The test suite is deliberately outside mypy's strict scope
> (`packages = ["decision_ledger"]`); it is verified at runtime by pytest.
> This keeps the type-check gate tight on shipped code (package + examples).
>
> **Coverage method.** Use `coverage run` rather than `pytest --cov=<module>`
> on Python 3.14 — Coverage's `--source` rebinding can double-import numpy's
> C extension (`cannot load module more than once per process`). Benchmark
> tests (single-eval < 1 ms) are skipped for coverage via `-k "not benchmark"`
> because per-line tracing inflates sub-microsecond hot paths.
>
> **Intentional coverage gaps** (defensive code left deliberately untested):
> - fail-closed `except Exception` handlers on `evaluate()` / `calibrate()`
>   and the final-drain / stats-write failures in `shutdown()`,
> - missing-dependency fallbacks (`no blake3`, `no uuid6`), exercised only
>   when a dependency is absent,
> - consumer lifecycle edges (`stop()` before start, threads alive past the
>   join timeout, in-memory cap drop branch),
> - database maintenance helpers (`backup`, checkpoint, retention) partial
>   branches, and the outcomes CLI's argument-validation edge cases.

### License

MIT — see [LICENSE](LICENSE).