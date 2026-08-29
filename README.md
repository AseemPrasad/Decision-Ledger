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

## Feature overview

| Area          | What it does                                                          |
| ------------- | --------------------------------------------------------------------- |
| Gatekeeper    | Hot-path `evaluate()` -> `delegate` / `escalate` / `explore_shadow`    |
| Fail-closed   | Unknown context or insufficient data always escalates                 |
| Telemetry     | Every evaluation captured in a bounded ring buffer                    |
| Outcomes      | Independent results linked back to decisions by `decision_id`         |
| Calibration   | Split Conformal Risk Control -> per-context `q_hat` + Wilson bound    |
| Policy        | Versioned YAML artifacts, atomic reload, context-hash invalidation    |
| Exploration   | Epsilon shadow sampling removes selection bias (no user risk)         |

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

## Quick start

```bash
python -m venv venv
venv\Scripts\activate            # Windows   (source venv/bin/activate on macOS/Linux)
pip install -r requirements.txt  # runtime + dev tools
pip install -e .                 # imports the decision_ledger package
pytest                           # run the test suite
```

## 5-minute tutorial

The full flow, in five steps:

```python
from decision_ledger import (
    GateAction, Gatekeeper, RingBuffer, context_hash, policy_from_results,
)
from decision_ledger.calibration import CalibrationRecord, ConformalCalibrator

# 1. Your serving context — changing any input creates a NEW context.
ctx = context_hash(
    "route",
    prompt_template="SELECT * FROM {table} LIMIT {n};",
    model_id="qwen-2.5-coder-7b-instruct",
    temperature=0.2,
)

# 2. Calibrate with labeled decisions (independent, non-exploratory).
records = [CalibrationRecord(ctx, 0.05, 0.0), CalibrationRecord(ctx, 0.45, 1.0), ...]
result = ConformalCalibrator(target_alpha=0.05, min_sample_size=500)\
    .compute_threshold(records)
policy = policy_from_results({ctx: result}, version_id=1)

# 3. Enforce: delegate only when S = 1 - confidence <= q_hat.
gk = Gatekeeper(policy.contexts, exploration_rate=0.02, telemetry=RingBuffer())
action = gk.evaluate(ctx, 0.95, "route")
#   GateAction.DELEGATE      -> use the small model
#   GateAction.ESCALATE      -> use the frontier model (fail-closed)
#   GateAction.EXPLORE_SHADOW-> run both, serve frontier, log counterfactual

# 4. Outcomes arrive later; link and re-calibrate (see outcome_logging.py).

# 5. Publish the new policy and reload.
from decision_ledger.policy import save_policy, load_policy
save_policy(policy, "policies/policy-v1.yaml")
gk.reload_policy(load_policy("policies/policy-v1.yaml").contexts)
```

Run the ready-made demos:

```bash
python src/examples/basic_serving.py      # delegation enforcement
python src/examples/calibration_demo.py   # conformal calibration + policy YAML
python src/examples/outcome_logging.py    # outcomes -> join -> match rate
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
│   │   ├── outcomes.py         # outcome collection + joining
│   │   ├── calibration.py      # Split Conformal Risk Control
│   │   ├── policy.py           # versioned policy artifacts (YAML)
│   │   └── utils.py            # context hashing, UUIDv7 ids
│   ├── tests/                  # pytest suite
│   ├── examples/               # runnable demos
│   └── docs/                   # DESIGN.md, API.md, RUNBOOK.md, EXAMPLES.md
├── pyproject.toml              # metadata + black/mypy/pytest config
├── requirements.txt
└── setup.py                    # legacy shim
```

## Docs

- [Architecture (Week 1)](src/docs/ARCHITECTURE_WEEK1.md) — gatekeeper + ring buffer, latency budget, thread safety, exploration
- [Quickstart](src/docs/QUICKSTART.md) — 5-minute setup with runnable examples
- [Gatekeeper API](src/docs/API_GATEKEEPER.md) — full reference + common mistakes
- [Telemetry API](src/docs/API_TELEMETRY.md) — ring buffer + decision records, performance
- [Design](src/docs/DESIGN.md) — problem, guarantees, architecture, trade-offs
- [API](src/docs/API.md) — full reference
- [Runbook](src/docs/RUNBOOK.md) — operations, monitoring, failure modes
- [Examples](src/docs/EXAMPLES.md) — end-to-end patterns
- [Benchmarks](src/docs/BENCHMARKS.md) — measured budgets and methodology
- [Code Style](src/docs/CODE_STYLE.md) — formatting, typing, docstrings, logging

## Development

```bash
black .                                    # format
mypy src/                                  # typecheck the package
pytest                                     # fast test run (incl. benchmarks)
coverage run --branch -m pytest -k "not benchmark"   # measure coverage
coverage report -m -i                      # view report
```

> Note: use `coverage run` directly rather than `pytest --cov=<module>` on
> Python 3.14 — Coverage's `--source` rebinding can double-import numpy's C
> extension (`cannot load module more than once per process`).
>
> Benchmark tests (single-eval < 1ms, `pop_batch(1000)` < 100µs) run under
> plain `pytest`; they are skipped for coverage runs via `-k "not benchmark"`
> because Coverage's per-line tracing adds overhead to sub-microsecond hot
> paths. See [Benchmarks](src/docs/BENCHMARKS.md) for the measured numbers.
>
> `pyproject.toml` pins tool configs for black, mypy, and pytest; add
> `[tool.coverage.run]` there if you want coverage settings versioned.

### License

MIT.