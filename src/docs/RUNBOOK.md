# Runbook

Operational procedures for the Decision Ledger MVP.

## Setup

```bash
python -m venv venv
source venv/bin/activate          # or venv\Scripts\activate on Windows
pip install -r requirements.txt
pip install -e .
```

## Daily loop

1. **Serve.** Applications call `Gatekeeper.evaluate(...)` inline; every call
   is recorded in a ring buffer.
2. **Drain.** Run a `BatchConsumer` (sidecar thread or process) to flush the
   buffer to SQLite (`ledger.db`, the durable `decisions` table).
   `JsonlExport` writes ad-hoc date/hour JSONL when you need a human-readable
   export.
3. **Collect outcomes.** Whenever an independent result is known (auto task
   metric, human review), record it with the original `decision_id`:
   `python -m decision_ledger.outcomes --decision-id <uuid> --outcome-value 1.0 --source task_metric`
   (or via `OutcomeCollector.log_outcome` in-process).
4. **Calibrate.** Run the calibration job (cron/hourly):
   join decisions + outcomes -> `compute_threshold` -> `policy_from_results`
   -> `save_policy` -> reload in each gatekeeper.
5. **Monitor.** Watch escalation rate, join match rate, per-context
   `q_hat`, and coverage lower bound.

## Status checks

```bash
# Count decisions and outcomes in the ledger database
python - <<'PY'
from decision_ledger import Database
db = Database("ledger.db")
print("decisions", len(db.get_decisions()))
print("outcomes", len(db.get_outcomes()))
db.close()
PY
```

## Policies

- Policies live under `policies/` (gitignored; version numbers are time-ordered).
- `save_policy` at `policies/policy-v<N>.yaml`; keep the last few for rollback.
- Rollback = `Gatekeeper.reload_policy(load_policy("policies/policy-v<N-1>.yaml").contexts)`.

## Failure modes

| Symptom                          | Likely cause             | Action                                          |
| -------------------------------- | ------------------------ | ------------------------------------------------ |
| Escalation rate jumps to 100%    | Context changed/revoked  | Expected: new context is DRAINING; re-collect.   |
| Escalation rate slowly climbing  | Model drift              | Increase exploration rate, re-calibrate, retrain.|
| Ring buffer drops (`push=False`) | Consumer lagging         | `stop()`/`start()` consumer or enlarge capacity. |
| Match rate < 30%                 | Outcomes lagging         | Expedite outcome pipeline; delegation degrades.  |
| Unknown-context escalations      | Hash mismatch in config  | Recompute `context_hash` with the same inputs.   |
| q_hat = None                     | Below min_sample_size    | Keep escalating until threshold exists.          |

## FAQ

**Why is everything failing closed right after a model/prompt change?**
By design. The context hash encodes prompt, weights, quantization, adapter,
temperature and decision type. A new context has zero samples and cannot be
delegated until `min_sample_size` independent outcomes are collected.

**Why does exploration matter?**
Without epsilon-exploration, low-confidence decisions are always escalated and
never measured — selection bias makes the calibration look better than it is.
Shadow exploration samples the full confidence range with zero user risk.

**Why only independent, non-exploratory records in calibration?**
`is_exploratory` data is used for drift detection, not threshold computation;
records whose outcome came from the small model itself would self-confirm.