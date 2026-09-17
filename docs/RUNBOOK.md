# Decision Ledger — Production Runbook

Operational guide for running Decision Ledger (`decision_ledger` v0.1.0, policy
schema `1.0`) in production. Covers deployment, monitoring, troubleshooting,
incidents, maintenance, and scaling.

> This is the comprehensive runbook. The MVP quick loops are in
> [`src/docs/RUNBOOK.md`](../src/docs/RUNBOOK.md) and the full API reference is
> in [`src/docs/API_REFERENCE.md`](../src/docs/API_REFERENCE.md).

---

## 0. Data model in one page

You must understand the durability model before you operate this system.

```
 evaluate() ──► RingBuffer (in-memory) ──► BatchConsumer / gRPC Stream ──► SQLite / Postgres / ClickHouse
      │                                                                                │
      └─ DecisionRecord{action, confidence, non_conformity, latency_us}                │
                                                                                       ▼
 policy artifacts (◄─ PolicyGenerator) ◄─ Conformal/IPW Calib ◄─ join decisions ◄── outcomes.log_outcome()
      ▲                                       (joined_records)
      └── Ed25519 Verified Hot-Reload into Gatekeeper (Multi-Objective q_hat per context)
```

- `evaluate()` returns **before** the decision is durable. Durability happens
  when `BatchConsumer` flushes to SQLite (batch or time window).
- `log_outcome(decision_id, ...)` requires the decision to already be flushed;
  otherwise it raises `DecisionNotFoundError`. Force a flush with
  `ledger.consumer.drain_now()`.
- **Fail-closed invariant:** an unknown, inactive, under-sampled, or
  `q_hat = None` context always returns `ESCALATE`. An empty/missing policy
  means *everything* escalates.
- The ring buffer drops records (oldest, or exploratory first) when fill
  reaches `>= 0.95`. Dropped records are **permanently lost** from the audit
  trail — this is the one failure mode you can never repair retroactively.
- Configuration is **entirely programmatic** (constructor kwargs). The package
  has no `config.py`, reads **no** environment variables, and loads **no**
  YAML at runtime. Sections 1.2 and 1.3 define the convention this runbook
  recommends so operators get a real, versioned config surface.

---

## 1. Deployment

### 1.1 Prerequisites and setup

| Requirement     | Value                                                            |
| --------------- | ---------------------------------------------------------------- |
| Python          | `>= 3.9`; **3.14 recommended** (built-in `uuid.uuid7`)           |
| Runtime deps    | `numpy>=1.24`, `PyYAML>=6`, `blake3>=1`, `uuid6>=2024`           |
| DB              | SQLite, single file, rollback journal (WAL deliberately off)     |
| Storage         | **Local SSD only.** Avoid network filesystems and synced folders |

Installation on each node:

```bash
python -m venv venv
venv\Scripts\activate          # macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
pip install -e .
pytest                         # sanity: full suite green
pytest src/tests -k benchmark  # latency budgets: single-eval <1ms, pop_batch <100us
```

You need **one** serving process per service replica (the gate runs in-process)
plus optional dedicated workers for the outcome pipeline and calibration job.
Only **one process at a time** should own writing `decisions` — SQLite has a
single-writer model; multiple writer processes contend on the file lock
(see §3.2).

### 1.2 Configuration file template `decision_ledger.yaml`

The library does not auto-load this file. Deploy it as a convention, and load
it with the helper in §1.3 (or your own). Keep it in the same directory as
your app config; check it into a private repo (it contains no secrets; keep
the DB and policy paths absolute).

```yaml
# decision_ledger.yaml
version: 1
database:
  path: /var/opt/decision-ledger/ledger.db
policy:
  # Startup artifact. Omitting it boots the gate EMPTY => fail-closed
  # (every context escalates until the first calibration runs).
  file: /var/opt/decision-ledger/policies/policy_latest.yaml
  default_alpha: 0.05
  fail_closed: true
gate:
  exploration_rate: 0.02      # deterministic: 1 in (1/rate) eligible calls explores
  min_sample_size: 100
buffering:
  ring_buffer_capacity: 100000
  flush_interval_s: 5.0
consumer:
  # Honored only when components are wired directly (see 6.3). The
  # DecisionLedger facade hard-codes 5000.
  batch_size: 5000
  backlog_alert_level: 50000   # log warning; counts toward alerts (§2.2)
  backlog_capacity: 100000
monitoring:
  alert_on_drop: true
  alert_on_backlog: true
retention:
  decisions_days: 90           # enforced by the retention job (§5.4)
  policies_keep: 10            # keep N newest artifacts (§5.3)
```

### 1.3 Environment variables

Decision Ledger itself reads **no** environment variables. The variables below
are the deployment contract this runbook recommends: they are read by your
loader and passed into the constructor. Keeping them as env vars lets the same
container image run dev/staging/prod without code changes.

| Variable                          | Maps to                          | Default         |
| --------------------------------- | -------------------------------- | --------------- |
| `LEDGER_DB_PATH`                  | `DecisionLedger(db_path=...)`    | `ledger.db`     |
| `LEDGER_POLICY_FILE`              | `DecisionLedger(policy_file=...)`| *(none → empty)*|
| `LEDGER_EXPLORATION_RATE`         | `exploration_rate`               | `0.02`          |
| `LEDGER_RING_BUFFER_CAPACITY`     | `ring_buffer_capacity`           | `100000`        |
| `LEDGER_FLUSH_INTERVAL_S`         | `flush_interval`                 | `5.0`           |
| `LEDGER_LOG_LEVEL`                | logger level                     | `INFO`          |

Reference loader (reads YAML, then env overrides, then builds the ledger):

```python
import os
import yaml
from decision_ledger import DecisionLedger

def load_config(path: str = "decision_ledger.yaml") -> dict:
    cfg = yaml.safe_load(open(path, encoding="utf-8")) if os.path.exists(path) else {}
    env = os.environ
    return {
        "db_path": env.get("LEDGER_DB_PATH", cfg.get("database", {}).get("path", "ledger.db")),
        "policy_file": env.get("LEDGER_POLICY_FILE", cfg.get("policy", {}).get("file")),
        "exploration_rate": float(env.get(
            "LEDGER_EXPLORATION_RATE",
            cfg.get("gate", {}).get("exploration_rate", 0.02))),
        "ring_buffer_capacity": int(env.get(
            "LEDGER_RING_BUFFER_CAPACITY",
            cfg.get("buffering", {}).get("ring_buffer_capacity", 100_000))),
        "flush_interval": float(env.get(
            "LEDGER_FLUSH_INTERVAL_S",
            cfg.get("buffering", {}).get("flush_interval_s", 5.0))),
    }

ledger = DecisionLedger(**load_config())
```

### 1.4 Database setup

```bash
mkdir -p /var/opt/decision-ledger
# Ownership: the single writer process. Never run the DB on a synced folder
# (OneDrive/Dropbox) or a network mount.
chown -R svc-ledger:svc-ledger /var/opt/decision-ledger
```

1. The schema is created automatically and idempotently on first
   `DecisionLedger(...)` / `Database(db)` (see `/var/opt/decision-ledger/ledger.db`).
2. Verify on first boot:

```python
from decision_ledger import Database
db = Database("/var/opt/decision-ledger/ledger.db")
db.verify_schema()   # PRAGMA quick_check + tables/indexes; raises on problems
print(db.execute_query("PRAGMA journal_mode")[0]["journal_mode"])  # "delete" (rollback)
print([r["name"] for r in db.execute_query(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")])
# ['decisions','joined_records','outcomes','policies']
db.close()
```

Notes:

- **One writer at a time.** Holding the DB on a network share breaks the
  rollback-journal model and produces `database is locked` storms (§3.2).
- Connection settings are baked in: `busy_timeout=2000ms`, render timeout
  `2.0s`, `check_same_thread=False` (per-thread connections), autocommit.
- Every `shutdown()` writes `<db_stem>_stats.json` next to the DB. Keep it as
  the canonical end-of-process report.

### 1.5 Policy initialization (bootstrapping)

Fresh installs start **empty → fully fail-closed**. There is nothing to gate
yet, which is correct. Bring a context online as follows:

1. **Serve for a while.** `evaluate()` calls stream into the ring buffer and,
   once the consumer flushes, into `decisions`. All early traffic escalates.
2. **Collect outcomes.** Every decision that surfaced an independent result
   (task metric, human review, user report) gets `log_outcome(decision_id, ...)`
   or the CLI:

   ```bash
   python -m decision_ledger.outcomes \
     --decision-id <uuid> --outcome-value 1.0 \
     --source task_metric --db ledger.db
   ```

3. **First calibration** (once a context has >= `min_sample_size` outcomes):

   ```python
   from decision_ledger import DecisionLedger, make_context_hash
   ledger = DecisionLedger(policy_file="policies/policy_latest.yaml")
   path = ledger.calibrate()                    # drains, joins, calibrates, publishes, reloads
   print("published", path)
   ledger.consumer.drain_now()
   ```

4. **Confirm the gate moved:** a context is `ACTIVE` and delegable only when
   `q_hat is not None` **and** `sample_size >= min_sample_size`. Verify:

   ```python
   ctx = make_context_hash("qwen-7b", "routing")
   print(ledger.gatekeeper.evaluate(ctx, 0.95, "route").name)   # expect DELEGATE
   ```

Prefer pointing `policy_file` at an existing artifact so every replica boots
with the same already-calibrated policy instead of an empty gate.

---

## 2. Monitoring

### 2.1 Key metrics

All live counters come from `ledger.stats()` (plus telemetry/calibration
helpers). Pull `stats()` on a 10–60 s cadence into your metrics collector.

`DecisionLedger.stats()`:

| Key                  | Type                | Meaning                                            |
| -------------------- | ------------------- | -------------------------------------------------- |
| `total_decisions`    | int                 | Durable decisions written to SQLite                |
| `total_outcomes`     | int                 | Outcomes logged                                    |
| `join_rate`          | float (0–1)         | Fraction of decisions with an outcome              |
| `decisions_by_action`| dict[str,int]       | Counts of DELEGATE / ESCALATE / EXPLORE_SHADOW     |
| `contexts_active`    | int                 | Contexts currently delegable                       |
| `contexts_draining`  | int                 | Contexts still below `min_sample_size`             |
| `ring_buffer_fill`   | float (0–1)         | In-memory fill level                               |
| `ring_buffer_size`   | int                 | Decisions in the ring buffer right now             |
| `dropped_records`    | int                 | **Cumulative lost audit records** (must be 0)      |
| `policy_version`     | str\|None           | Serving `YYYYMMDD-HHMMSS` artifact version         |
| `gatekeeper`         | dict                | See below                                          |
| `consumer`           | dict                | See below                                          |

`stats()["gatekeeper"]` (`Gatekeeper.get_metrics()`):
`delegate`, `escalate`, `explore`, `total`, `escalation_rate`,
`exploration_rate`, `per_decision_type` (`route/judge/speculate/mutate/
summarize/abstain` → `{calls, escalations, escalation_rate}`).

`stats()["consumer"]` (`BatchConsumer.get_metrics()`):
`total_records_processed`, `total_records_flushed`, `total_records_dropped`,
`total_flushes`, `last_flush_time` (monotonic s), `avg_flush_time_ms`,
`backlog_records`.

Other readers: `OutcomeCollector.get_metrics()`
(`outcomes_logged`, `batches_logged`, `last_logged_at`), telemetry
`RingBuffer.dropped_count`/`fill_level()`, and
`CalibrationPipeline.get_calibration_stats()`:
`total_decisions`, `total_outcomes`, `join_rate`, `contexts_calibrated`,
`samples_per_context`, `q_hat_distribution`
(`activated_count/draining_count/activated_contexts/draining_contexts/q_hats`),
`drift` (`drift_detected_count`, `drifted_contexts`).

### 2.2 Alert thresholds

| Metric                                   | Watch            | Warn                | CRITICAL            |
| ---------------------------------------- | ---------------- | ------------------- | ------------------- |
| `ring_buffer_fill`                       | `>= 0.50`        | `>= 0.75` (10s)     | `>= 0.90` (5s)      |
| `dropped_records` / `*_records_dropped`  | —                | —                   | **> 0 (any)**       |
| `consumer.backlog_records`               | `>= 25_000`      | `>= 50_000` (alert) | `>= 90_000`         |
| `escalation_rate`                        | +10 pp vs 24 h   | +20 pp vs 24 h      | ~100% sustained     |
| `join_rate`                              | `<= 0.5`         | `<= 0.3`            | `<= 0.15`           |
| `drift.drift_detected_count`             | —                | `>= 1`              | `>= 1` persisted 1h |
| `q_hat` per context                      | None after 1 day | —                   | regression vs prev  |
| `p99 latency_us` (DB + live)             | `> 300`          | `> 700`             | `>= 900` (budget 1ms)|
| `avg_flush_time_ms`                      | `> 10`           | `> 50`              | `> 200`             |
| `policy_version` missing/stale           | —                | not `YYYYMMDD-HHMMSS` | older than last calibration |

The hard line: **any drop is a page.** Drops are unrecoverable audit loss.
Everything else degrades service, drops destroy data.

### 2.3 How to check gatekeeper latency

Latency for every decision is captured in `DecisionRecord.latency_us` and the
durable `decisions.latency_us` column. Two ways to read it:

Live, from the most recent durable records:

```python
from decision_ledger import Database
import statistics

db = Database("ledger.db")
rows = db.execute_query(
    "SELECT latency_us FROM decisions "
    "WHERE latency_us IS NOT NULL ORDER BY timestamp_ns DESC LIMIT 1000")
vals = sorted(r["latency_us"] for r in rows)
n = len(vals)
p99 = vals[min(int(n * 0.99) - 1, n - 1)]
print(f"n={n} p50={vals[n//2]}us p99={p99}us max={vals[-1]}us")
db.close()
```

In-process (hot quantiles, gates telemetry on — this is what the benchmark
asserts, p99 < 1 ms budget):

```python
import numpy as np
from decision_ledger import Gatekeeper, RingBuffer, context_hash

buffer = RingBuffer(capacity=100_000)
gk = Gatekeeper(policy={}, exploration_rate=0.0, telemetry=buffer)  # or real policy
lats = []
for i in range(5000):
    t0 = __import__("time").perf_counter_ns()
    gk.evaluate(context_hash("route"), 0.8 + (i % 100) / 1000, "route")
    lats.append((__import__("time").perf_counter_ns() - t0) / 1000)
print("p50 %.1fus  p99 %.1fus" % (np.percentile(lats, 50), np.percentile(lats, 99)))
```

Reference on Ryzen 3 5300U / Python 3.14: p50 ≈ 7.7 µs, p99 ≈ 26.6 µs with
telemetry; ~1.05 µs without. Budgets: single `evaluate()` < 1 ms,
`pop_batch(1000)` < 100 µs. CI check: `pytest src/tests -k benchmark`.

### 2.4 How to check join rate

Join rate = decisions that have at least one outcome ÷ total decisions. Three
equivalents:

```python
ledger.stats()["join_rate"]                                   # live end-to-end
db.get_join_statistics()["match_rate"]                        # total_decisions/total_outcomes/joined_count/match_rate
CalibrationPipeline(db, gk, gen).get_calibration_stats()["join_rate"]  # calibration view
```

Drill into what is failing to join (outcomes pipelining lag):

```python
joined = db.get_joined_records(include_unmatched=True)
matched = sum(1 for r in joined if r["outcome_timestamp_ns"] is not None)
print(f"{len(joined)} decisions, {matched} matched, {len(joined) - matched} unmatched")
```

Also watch `stats()["decisions_by_action"]["EXPLORE_SHADOW"]`: shadow
exploration is *excluded* from calibration thresholds by design, so it should
be ~`exploration_rate` of traffic — not a signal of join problems.

### 2.5 How to check for drift

Drift is measured per context as divergence between full-range and active-range
accuracy (`ConformalCalibrator.DRIFT_DIVERGENCE_THRESHOLD = 0.05`).

Aggregate (best for dashboards):

```python
cfg = CalibrationPipeline(db, gk, gen).get_calibration_stats()
d = cfg["drift"]
print(f"drifted contexts: {d['drift_detected_count']} {d['drifted_contexts']}")
```

Per context (investigation/ad-hoc):

```python
from decision_ledger import make_context_hash
from decision_ledger.calibration import ConformalCalibrator

cal = ConformalCalibrator(Database("ledger.db"))
ctx = make_context_hash("qwen-7b", "routing")
d = cal.detect_drift(ctx)
# -> {drift_detected, full_range_accuracy, active_range_accuracy, divergence}
if d["drift_detected"]:
    print("DRIFT", f"{d['divergence']:.3f} divergence on", ctx.hex())
```

A `drift_detected` alert means the serving small model's confidence ranking
diverged from its measured accuracy — recalibrate and/or retrain (§4.1).

---

## 3. Troubleshooting

### 3.1 Troubleshooting matrix

| # | Symptom | Diagnose | Most likely cause | Resolution |
| - | ------- | -------- | ----------------- | ---------- |
| 1 | `escalation_rate` jumps to ~100% | `stats()["contexts_active"]`→0 | Context changed/revoked; new context is DRAINING | Expected. Re-collect outcomes, recalibrate. Verify `context_hash` inputs unchanged |
| 2 | `escalation_rate` slowly climbing | `detect_drift` divergence | Model drift | Raise exploration, recalibrate (§4.1) |
| 3 | `dropped_records > 0` | `ring_buffer_fill`, `backlog_records` | Consumer lag | §3.3 recovery; never silent |
| 4 | `backlog_records` high but fill OK | `avg_flush_time_ms` high | Slow disk / locks | §3.3, §3.2, §6.1 |
| 5 | `DecisionNotFoundError` on `log_outcome` | Was decision flushed? | Decision still in ring buffer / consumer | `ledger.consumer.drain_now()` first (also §0) |
| 6 | High p99 latency | §2.3 | Saturated buffer (drop-tier overhead), lock contention, slow disk | §3.4 |
| 7 | `database is locked ... after 3 attempts` | Lock errors in logs; who else writes? | Multiple writers / long txns / network FS | §3.2 |
| 8 | No context ever activates | `samples_per_context` stuck low | Outcomes not arriving (join < min) or calibration excludes records | Check `model_verification` source is NOT used; ensure independent, non-exploratory records |
| 9 | Calibration excludes things unexpectedly | Inspect policy | `EXPLORE_SHADOW`/`model_verification` excluded by design | Expected; drive via independent `human`/`task_metric` outcomes |
| 10 | `FileNotFoundError: policy_file` | Path check | Artifact gone / renamed | Restore from archive (§5.3) or boot empty (fail-closed) |
| 11 | `PolicyError` on `generate_policy` | Message | Same-second version collision | Pass explicit `policy_version` or `force=True` |
| 12 | `stats.json` missing | End-of-process | Crash (no clean `shutdown`) | Trigger clean shutdown in SIGTERM handler; report still in DB |
| 13 | Join rate < 30% | §2.4 unmatched count | Outcome pipeline lagging | Expedite outcome collection; delegation degrades meanwhile |
| 14 | `latency_us` NULL on rows | Row written pre-telemetry path | Gate without telemetry attached | Attach `RingBuffer` to Gatekeeper for full audit |
| 15 | Everything escalates after deploy | Policy booted empty or wrong `policy_file` | Config missing `policy.file` | Point at artifact (§1.5) |

### 3.2 Database locking errors

Symptoms: `DatabaseError: database is locked after 3 attempts (...)`; SQLite
`OperationalError: database is locked`; `avg_flush_time_ms` rises.

Built-in mitigations (already active): `busy_timeout = 2000 ms`, write
connection timeout `2.0 s`, 3 retries with 50–150 ms backoff, per-thread
connections via `check_same_thread=False`.

Causes and fixes, in order of likelihood:

1. **Two processes writing the same DB.** SQLite allows one writer. Ensure
   exactly one process owns the write path (the `BatchConsumer`). Other
   replicas should run fail-closed read-only gates with empty/static policies
   or point at a replicated artifact.
2. **Network / synced filesystems.** Rollback journaling on NFS/SMB/OneDrive
   remounts metadata and livelocks. Move the DB to a local SSD.
3. **Long transactions** (e.g., a large `log_outcomes_batch`, a `VACUUM`, a
   retention DELETE while the consumer flushes). Batch sizes are already 5k
   for consumer flushes; keep `log_outcomes_batch` batches modest and run
   maintenance off-hours (§5).
4. **Contended maintenance.** Never run `VACUUM`/big DELETEs while the
   consumer is mid-flush; stop the writer first (§5.2).

Quick check that contention is the active problem:

```python
from decision_ledger import Database
db = Database("ledger.db")
print(db.execute_query("PRAGMA busy_timeout")[0])   # busy_timeout=2000
# List live writers: n/a for SQLite; instead inspect for other processes:
print(db.execute_query("SELECT COUNT(*) AS n FROM decisions")[0])
db.close()
```

### 3.3 Consumer lag and how to recover

Symptoms: `ring_buffer_fill` climbs past 0.75, `dropped_records > 0`,
`backlog_records` near 50k, `avg_flush_time_ms` high, `last_flush_time` stale.

Diagnose:

```python
m = ledger.consumer.get_metrics()
print(f"processed={m['total_records_processed']} flushed={m['total_records_flushed']} "
      f"dropped={m['total_records_dropped']} backlog={m['backlog_records']} "
      f"avg_flush_ms={m['avg_flush_time_ms']}")
```

Recovery (fastest first):

1. **Force a drain** — clears the ring buffer synchronously:
   `ledger.consumer.drain_now()`.
2. **Reduce other DB load.** If `avg_flush_time_ms` is high, the flush itself
   is the bottleneck (disk, lock contention — see §3.2, §6.1).
3. **Restart the drain loop.** The consumer `Thread` cannot be restarted
   (`start()` twice raises). `stop()` and build a **fresh** consumer on the
   same buffer:

   ```python
   ledger.consumer.stop(timeout=10)      # final flush attempt
   from decision_ledger import BatchConsumer
   ledger.consumer = BatchConsumer(
       ledger.ring_buffer, ledger.database,  # same buffer+db; ownership passes
       flush_interval=ledger.consumer.flush_interval,
   )
   ledger.consumer.start()
   ```

   Prefer restarting the whole process in production (cleaner ownership).
4. **Raise capacity / batch** (§6.2, §6.3) if lag is structural.
5. Confirm recovery: `fill_level` back < 0.5, `dropped` stopped growing, and
   `SELECT COUNT(*) FROM decisions` matches `stats()["total_decisions"]`.

**No records are lost on flush failure** — the consumer keeps them in the
in-memory backlog (cap 100k) and retries. Records **are** lost only when the
ring buffer itself drops at fill `>= 0.95`. A DB outage costs latency, not
completeness; a hard-crash (process kill) loses whatever was in the ring
buffer at that instant. Budget capacity so this is rare (§6.2).

### 3.4 High latency

Diagnose with §2.3 quantiles. Known contributors, in order of cost:

| Cause | Signature | Fix |
| ----- | --------- | --- |
| Saturated ring buffer paying drop-tier overhead | `ring_buffer_fill >= 0.90`, p99 skews | Raise capacity / fix consumer lag (§6.2, §3.3) |
| Disk contention (spin/network) on flush | `avg_flush_time_ms > 50`, lock retries | Local SSD; one writer (§3.2) |
| Telemetry cost | no telemetry ≈ 1.05 µs vs 7.7 µs with it | Accept (audit trail) or raise throughput via bigger batches |
| Inline `log_outcome`/`drain_now` on the hot path | spikes coincide with calls | Move outcome logging off hot path; defer `drain_now` |
| Python GC/allocations under burst | transient p99 spikes | Warm up; size the buffer; load test with `stress_test.py` |
| `calibrate()` running during serving | spike during cron | Schedule calibration in a maintenance window or separate process |

Budget is **p99 < 1 ms** for `evaluate()`. If p99 > ~300–700 µs, act before
you approach the budget.

### 3.5 Data consistency checks

Weekly (or after any incident):

```python
from decision_ledger import Database
db = Database("ledger.db")

db.verify_schema()                       # PRAGMA quick_check + schema; raises on any error
assert db.execute_query("PRAGMA quick_check")[0]["quick_check"] == "ok"

s = db.get_join_statistics()
print("decisions", s["total_decisions"], "outcomes", s["total_outcomes"],
      "joined", s["joined_count"], "match", s["match_rate"])

# No orphan decisions in joined_records beyond decisions count implies consistency.
# Foreign keys are ON: outcomes.decision_id -> decisions.decision_id enforced.
# Expect: policy_latest.yaml exists, is YAML-schema 1.0, version matches stats()["policy_version"].
db.close()
```

Check the durable-vs-memory story after a restart: on boot, with an empty
ring buffer, `stats()["total_decisions"]` should equal
`SELECT COUNT(*) FROM decisions` (until new traffic flows). The `<db_stem>_stats.json`
from a clean `shutdown()` is your offline consistency snapshot — diff its
`total_decisions` against the DB after downtime.

---

## 4. Incidents

### 4.1 Quality degradation response

Signs: escalation climbing, drift alert, join rate falling, q_hat moving.

**Triage (0–10 min)**

1. Read the drift panel: `drift.drifted_contexts` and per-context
   `detect_drift` (must be the same hash the serving code uses — check params).
2. Read quality over the last 24 h: p99 latency, escalation rate, per-context
   q_hat from `CalibrationPipeline.get_calibration_stats()`.
3. Classify severity:

| Severity | Criteria | Action |
| -------- | -------- | ------ |
| SEV-1 | Any `dropped_records` | §3.3 immediately (data loss) |
| SEV-2 | escalation ~100% or drift persisted >1 h | Contain (below), then restore |
| SEV-3 | escalation +10–20 pp, join < 30% | Monitor, raise exploration, expedite outcomes |

**Contain (10–20 min)**

- **Raise exploration** to sample the failing confidence band (adds shadow
  traffic, zero user risk):
  `ledger.gatekeeper.exploration_rate = 0.1`.
- **If containment is insufficient, force global fail-closed** (roll all
  traffic to ESCALATE for the affected context) — see §4.3 soft reset, or
  §4.2 for a full policy rollback.

**Restore (1–4 h)**

1. Ensure outcomes continue to be collected for the degraded traffic (fix the
   outcome pipeline before recalibrating — recalibration over bad joins bakes
   bias in).
2. Re-run calibration and publish:
   `ledger.calibrate(target_alpha=...)` (default 0.05).
3. Verify `q_hat` per context moved in the safe direction, then confirm
   `DELEGATE` on a known-good sample (§1.5 step 4).
4. Ramp exploration back to `0.02`. If the drift returns, the fix is a retrain
   or a context change (new hash) — schedule it.

Two hours after every incident, diff `<db_stem>_stats.json` snapshots and
write a brief report: cause, decisions touched, whether any records dropped.

### 4.2 How to roll back a policy (EMERGENCY)

Every calibration publishes `policy_<YYYYMMDD-HHMMSS>.yaml` and repoints
`policy_latest.yaml`. Roll back to any previous version:

```python
from decision_ledger.policy import PolicyGenerator

gen = PolicyGenerator("data/policies")       # same dir the artifacts live in
print(gen.get_policy_history())              # newest first, e.g. ['20260830-130000', ...]
target = "20260830-120000"                   # pick the last known-good version
gen.rollback_policy(target)                  # repoints policy_latest.yaml; returns artifact path
# hot-reload into the running gate:
ledger.gatekeeper.reload_policy_from_file("data/policies/policy_latest.yaml")
print("now serving", ledger.stats()["policy_version"])
```

CLI-equivalent using a fresh process (good for multi-replica rollouts):

```python
from decision_ledger.policy import PolicyGenerator
gen = PolicyGenerator("data/policies")
gen.rollback_policy("20260830-120000")
```

Then restart replicas with `policy_file="data/policies/policy_latest.yaml"`.
`rollback_policy` validates the version format and existence of the artifact
before republishing; it never touches the DB. Rollback is always safe and
instant — the artifact list is your undo log.

**Emergency "everything escalates" lever** (Stop-the-line, e.g., you suspect
the gate is delegating garbage): publish an empty policy and hot-reload:

```python
from decision_ledger.policy import PolicyGenerator
gen = PolicyGenerator("data/policies")
gen.generate_policy({}, policy_version="20260830-235959", force=True)  # no contexts => all ESCALATE
ledger.gatekeeper.reload_policy_from_file("data/policies/policy_latest.yaml")
```

### 4.3 How to reset a context

When a context misbehaves (drifted/q_hat wrong) you reset it so it reverts to
DRAINING → ESCALATE until fresh outcomes re-qualify it.

**Soft reset (no data loss)** — publish a policy that simply omits the
offending context so it no longer delegates. There is no dedicated
"skip context" API: build a `results` mapping that excludes the context and
call `generate_policy` directly (the `CalibrationPipeline` calibrates every
context that has outcomes, so it cannot selectively skip). The supported and
simplest ops path when you do not have a results mapping handy is the hard
reset below — it drives the same outcome (DRAINING → ESCALATE).

**Hard reset (data removal)** — removes the context's decisions/outcomes so
its sample count drops to zero and the gate escalates again:

```python
from decision_ledger import Database
ctx = make_context_hash("qwen-7b", "routing")      # bytes, 16 bytes exactly
db = Database("ledger.db")
db.execute_write("DELETE FROM joined_records WHERE context_hash = ?", (ctx,))
db.execute_write("DELETE FROM outcomes WHERE decision_id IN "
                 "(SELECT decision_id FROM decisions WHERE context_hash = ?)", (ctx,))
db.execute_write("DELETE FROM decisions WHERE context_hash = ?", (ctx,))
db.close()
ledger.calibrate()   # republish; the context is now DRAINING (or absent)
```

Verify the reset: `ledger.gatekeeper.evaluate(ctx, 0.95, "route")` →
`ESCALATE` until `min_sample_size` new independent, non-exploratory outcomes
arrive.

**Reset by construction:** changing any input of `make_context_hash` (weights,
quantization, adapter, prompt template, temperature, decision type) produces a
brand-new context hash that automatically starts DRAINING. Model/prompt
upgrades need no manual reset.

### 4.4 How to clear corrupted data

Only delete what integrity checks prove corrupt; otherwise repair file-level.

**Identify corruption**

```python
from decision_ledger import Database
db = Database("ledger.db")
rows = db.execute_query("PRAGMA integrity_check")    # "ok" per page → healthy
rows = db.execute_query("PRAGMA quick_check")        # fast variant
db.verify_schema()                                   # schema-level check, raises
db.close()
```

**Repair path (in order)**

1. **Vacuum/repair a logically inconsistent DB** (not a crash): correct the
   offending rows yourself (§5.4 delete tooling), then `VACUUM` (§5.2).
2. **Crash-corrupt file:** restore from the last good backup (§ Appendix D),
   then replay the ring-buffer audit trail from the JSONL/ad-hoc export if you
   keep one (`JsonlExport`), and re-run `verify_schema()` before resuming
   writes.
3. **To surgically purge bad rows** (e.g., a poisoned batch of outcomes from a
   bad source), delete by source and time window — `joined_records` rows must
   be removed after their `outcomes` rows so they do not linger:

```python
import time
from decision_ledger import Database

db = Database("ledger.db")
purge_since_ns = int((time.time() - 7 * 86400) * 1_000_000_000)  # last 7 days
db.execute_write(
    "DELETE FROM outcomes WHERE outcome_source = ? AND timestamp_ns >= ?",
    ("model_verification", purge_since_ns))
db.execute_write(
    "DELETE FROM joined_records WHERE outcome_timestamp_ns >= ?",
    (purge_since_ns,))
db.close()
```

Use `outcomes.outcome_source` to filter (e.g., a bad `model_verification`
batch). After any purge, **re-run calibration** so published thresholds
reflect the cleaned data.

---

## 5. Maintenance

### 5.1 Regular health checks

| Cadence | Checks | Command |
| ------- | ------ | ------- |
| Daily | stats sanity, fill, drops, escalation | `ledger.stats()` dashboards; alert on thresholds §2.2 |
| Daily | join rate, drift count | §2.4, §2.5 |
| Weekly | `verify_schema()`, unmatched ratio | §3.5 script |
| Weekly | old artifact prune | §5.3 |
| Monthly | `VACUUM` + `ANALYZE`, backup test restore | §5.2, Appendix D |
| Monthly | retention enforcement run | §5.4 |

Runnable daily check:

```python
from decision_ledger import DecisionLedger
ledger = DecisionLedger(policy_file="data/policies/policy_latest.yaml")
s = ledger.stats()
assert s["dropped_records"] == 0, "AUDIT LOSS"
assert s["join_rate"] >= 0.3, "JOIN LOW"
assert s["gatekeeper"]["escalation_rate"] < 0.5, "ESCALATING"
assert s["ring_buffer_fill"] < 0.75, "BUFFER PRESSURE"
assert s["policy_version"], "NO POLICY"
ledger.shutdown()
```

### 5.2 Database optimization: VACUUM and ANALYZE

`VACUUM` reclaims space freed by retention DELETEs (requires exclusive write
access); `ANALYZE` refreshes the query planner stats. Run them in a
**maintenance window with the writer stopped** — no process may hold an open
transaction or lock.

Offline:

```bash
sqlite3 /var/opt/decision-ledger/ledger.db "PRAGMA quick_check; PRAGMA integrity_check;"
sqlite3 /var/opt/decision-ledger/ledger.db "VACUUM;"
sqlite3 /var/opt/decision-ledger/ledger.db "ANALYZE;"
```

In-process (service fully stopped):

```python
from decision_ledger import Database
db = Database("ledger.db")
assert db.execute_query("PRAGMA integrity_check")[0][0] == "ok"
db.execute_write("VACUUM")
db.execute_write("ANALYZE")
db.close()
```

Precautions:

- VACUUM compacts to a temporary file — needs ~2× database size free.
- Never VACUUM while the consumer thread is running (see §3.2 case 3).
- After a large VACUUM, plan the next backup; the file is physically new.

### 5.3 Policy archive (cleaning old policies)

Each calibration writes `policy_<version>.yaml` plus repointing
`policy_latest.yaml`. Retention job — keep the newest N artifacts (and never
delete the one referenced by `policy_latest.yaml`):

```python
import os, re
from decision_ledger.policy import PolicyGenerator

POLICIES_DIR = "data/policies"          # the same dir you passed to the generator
keep = 10
gen = PolicyGenerator(POLICIES_DIR)
versions = gen.get_policy_history()     # newest first, e.g. ['20260830-130000', ...]
_VER = re.compile(r"^\d{8}-\d{6}$")
latest = os.path.realpath(os.path.join(POLICIES_DIR, "policy_latest.yaml"))
for v in versions[keep:]:
    if not _VER.match(v):
        continue
    p = os.path.realpath(os.path.join(POLICIES_DIR, f"policy_{v}.yaml"))
    if os.path.exists(p) and p != latest:      # never delete the served artifact
        os.remove(p)
        print("archived", os.path.basename(p))
```

> Keep at least three versions so §4.2 rollback always has a last-known-good.
> `gen.rollback_policy` needs the artifact file present — archive, don't hard-delete,
> if you have any doubt.

### 5.4 Data retention (deleting old decisions/outcomes)

Retain `decisions`/`outcomes` per your compliance window, then purge oldest
first. Delete order matters (FKs are ON): `joined_records` (independent copy)
→ `outcomes` → `decisions`.

```python
import time
from decision_ledger import Database

db = Database("ledger.db")
cutoff_ns = int((time.time() - 90 * 86400) * 1_000_000_000)  # 90 days
db.execute_write("DELETE FROM joined_records WHERE decision_timestamp_ns < ?", (cutoff_ns,))
db.execute_write(
    "DELETE FROM outcomes WHERE decision_id IN "
    "(SELECT decision_id FROM decisions WHERE timestamp_ns < ?)", (cutoff_ns,))
db.execute_write("DELETE FROM decisions WHERE timestamp_ns < ?", (cutoff_ns,))
db.close()
```

Schedule this as a **daily cron during a maintenance window**, avoid running
it concurrently with flushes. Note the index `idx_decisions_timestamp_ns`
serves the purge; for very large tables add a time-bounded batch loop to keep
each transaction short. After purges, run the monthly `VACUUM`/`ANALYZE`
(§5.2). Deleting decisions below `min_sample_size` worth of data will push
contexts back to DRAINING — schedule retention *after* calibration runs, not
before.

---

## 6. Scaling

### 6.1 When to add resources

| Signal | Threshold | Resource |
| ------ | --------- | -------- |
| `ring_buffer_fill` sustained | `> 0.75` despite healthy flushes | Raise capacity (§6.2) + faster disk |
| `avg_flush_time_ms` | `> 50` sustained | Faster local SSD; one-writer topology |
| p99 latency | `> 500 µs` sustained | Reduce telemetry cost or split traffic |
| `backlog_records` | `> 50k` sustained | Raise batch size (§6.3), capacity (§6.2) |
| DB file size / growth | linear growth vs retention budget | Tighten retention (§5.4), then VACUUM |
| Lock retries | frequent "database is locked" | It's almost always a topology problem (§3.2), not capacity |

Rule of thumb: **capacity should keep worst-case in-flight decisions below
~50% of the ring buffer.** 100k default ≈ 50k decisions of headroom — tune to
your p95 decision rate × maximum consumer stall you can tolerate.

### 6.2 How to increase buffer size

The ring buffer size is fixed at construction (`ring_buffer_capacity`). You
cannot resize a live buffer — the procedure is **drain, then rebuild**:

```python
ledger.consumer.drain_now()              # persist everything first (no loss)
ledger.consumer.stop(timeout=10)
s = ledger.stats()                       # durable count captured
# keep an audit note: stats()["total_decisions"] at this point
# Rebuild the ledger with the larger capacity:
ledger = DecisionLedger(
    db_path="ledger.db",
    policy_file="data/policies/policy_latest.yaml",
    ring_buffer_capacity=500_000,        # new size
)
# Historical decisions are already durable; only future traffic fills the buffer.
```

For replicas built from raw components, construct `RingBuffer(capacity=N)` and
pass it to `Gatekeeper(..., telemetry=buffer)` and
`BatchConsumer(buffer, db, ...)`.

### 6.3 How to increase batch sizes

Two knobs, both on the consumer: `batch_size` (records held before a forced
flush) and `flush_interval` (max seconds between flushes).

- Via the `DecisionLedger` facade: only `flush_interval` is exposed.
  `batch_size` is fixed at the internal consumer's default (`5_000`).
- To tune `batch_size`, wire components directly (same topology the facade
  uses), as in `src/examples/stress_test.py`:

```python
from decision_ledger import Database, Gatekeeper, RingBuffer, BatchConsumer
ring = RingBuffer(capacity=200_000)
dk = Database("ledger.db")
gk = Gatekeeper(policy=..., telemetry=ring)
consumer = BatchConsumer(ring, dk, flush_interval=2.0, batch_size=50_000)
consumer.start()
```

Trade-off: bigger batches = fewer, larger transactions (lower flush overhead,
higher `avg_flush_time_ms` per flush and longer write locks — §3.2). Raise
`flush_interval` with `batch_size` only when the producer rate is high enough
to fill batches in a reasonable window; never raise batch size on a slow
consumer until you've also checked disk and locking (§3.3, §6.1).

### 6.4 Database query optimization

- The schema ships with indexes for the hot paths: `decisions` on
  `(timestamp_ns)`, `(context_hash)`, `(context_hash, action_taken)`;
  `outcomes` on `(decision_id)`, `(source)`; `joined_records` on
  `(context_hash)`, `(decision_timestamp_ns)`; `policies` on
  `(is_active)`, `(generated_at)`.
- Verify a query actually uses them and read the plan:

```python
plan = db.execute_query("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM decisions WHERE context_hash = ?", (ctx,))
print([r["detail"] for r in plan])   # look for 'USING INDEX idx_decisions_context_hash'
```

- Calibration reads `joined_records` by context hash — keep that index; purge
  function scans by `timestamp_ns` — the timestamp index covers it.
- Batch range reads (don't `get_decisions()` the whole table; pass
  `start_time`/`end_time`/`context_hash` filters, paginate).
- Run `ANALYZE` monthly (§5.2) so the planner keeps using those indexes after
  heavy churn.
- If one database outgrows the machine, the supported scale path is
  throughput, not sharding: bigger batches (§6.3), bigger buffer (§6.2), and a
  single dedicated writer with readers reading `joined_records`/`decisions`
  directly.

---

## Appendix A — Metric keys quick reference

| Source | Keys |
| ------ | ---- |
| `DecisionLedger.stats()` | `total_decisions`, `total_outcomes`, `join_rate`, `decisions_by_action`, `contexts_active`, `contexts_draining`, `ring_buffer_fill`, `ring_buffer_size`, `dropped_records`, `policy_version`, `gatekeeper`, `consumer` |
| `gatekeeper.get_metrics()` | `delegate`, `escalate`, `explore`, `total`, `escalation_rate`, `exploration_rate`, `per_decision_type{route…abstain → {calls, escalations, escalation_rate}}` |
| `consumer.get_metrics()` | `total_records_processed`, `total_records_flushed`, `total_records_dropped`, `total_flushes`, `last_flush_time`, `avg_flush_time_ms`, `backlog_records` |
| `outcome_collector.get_metrics()` | `outcomes_logged`, `batches_logged`, `last_logged_at` |
| `CalibrationPipeline.get_calibration_stats()` | `total_decisions`, `total_outcomes`, `join_rate`, `contexts_calibrated`, `samples_per_context`, `q_hat_distribution{activated_count, draining_count, activated_contexts, draining_contexts, q_hats}`, `drift{drift_detected_count, drifted_contexts}` |
| `ConformalCalibrator.detect_drift(ctx)` | `drift_detected`, `full_range_accuracy`, `active_range_accuracy`, `divergence` (threshold `0.05`) |
| CLI `python -m decision_ledger.outcomes` | flags `--decision-id --outcome-value --source --metadata --db`; exit `0` ok / `2` validation / `3` database |

## Appendix B — Alert threshold quick card

`fill >= 0.95` or `dropped_records > 0` → **page (data loss)** ·
`fill >= 0.75` warn · `backlog >= 50k` warn · `escalation +20pp` warn ·
`escalation ~100%` page · `join_rate <= 0.3` warn / `<= 0.15` page ·
`drift_detected` warn · p99 latency `> 700 µs` warn / `>= 900 µs` page ·
`avg_flush_time_ms > 50` warn.

## Appendix C — SQL cheat sheet

```sql
PRAGMA busy_timeout;
PRAGMA integrity_check;
PRAGMA quick_check;
VACUUM;
ANALYZE;
EXPLAIN QUERY PLAN SELECT ...;
SELECT COUNT(*) FROM decisions;
SELECT COUNT(*) FROM outcomes;
-- retention (run from Python with params; delete order joined->outcomes->decisions)
```

## Appendix D — Backup and restore

SQLite `online_backup` of the single file (rollback journal → consistent copy):

```python
import sqlite3, datetime
src, dst = "ledger.db", f"backup-{datetime.date.today().isoformat()}.db"
with sqlite3.connect(src) as c, sqlite3.connect(dst) as b:
    c.backup(b)
print("backed up to", dst)
```

Restore procedure:

1. Stop writing services (or run a restore-tier replica).
2. `copy /Y backup-2026-08-30.db ledger.db` (or `cp`).
3. `python -c "from decision_ledger import Database; d=Database('ledger.db'); d.verify_schema(); d.close()"`.
4. Restart services; treat `policy_latest.yaml` as the restore target too
   (restore the artifact dir alongside the DB if you rolled both back).
5. Re-run calibration on the restored data.

Keep backups on a different volume than the DB; restore-test monthly (§5.1).
Also archive the `<db_stem>_stats.json` snapshots with backups for audit.