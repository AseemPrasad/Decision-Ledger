# 06 — Features & Design Patterns

## 6.1 Feature inventory

### Serving (hot path)
| Feature | Where | Notes |
| --- | --- | --- |
| `evaluate()` DELEGATE/ESCALATE | `gatekeeper.py:193-245` | fail-closed |
| EXPLORE_SHADOW mode | `gatekeeper.py:231-245` | deterministic by call count |
| Deterministic exploration | `gatekeeper.py:18-21` | `n % int(1/rate) == 0`; no PRNG |
| Context isolation | `utils.make_context_hash` | each context has own threshold |
| Hot reload of policy | `reload_policy` / `gatekeeper.py` | atomic swap under single lock |
| Metrics | `get_metrics()` | escalation rates per decision type + summary |
| Telemetry on/off | `telemetry` flag on app init | opt-out for peak perf |

### Calibration & risk
| Feature | Where | Notes |
| --- | --- | --- |
| Per-context conformal threshold | `calibration.py:143-204` | `compute_threshold` |
| `min_sample_size` enforcement | `calibration.py` | escalate below it |
| Drift detection | `detect_drift` | threshold + feature-drift signal |
| Group balancing for parity | `calibration.py` | balances by outcome value |
| Wilson coverage lower bound | `calibration.py` | reported alongside q_hat |
| Exploratory records excluded | `calibration.py` record flags | never pollute the decision threshold |

### Outcomes & persistence
| Feature | Where | Notes |
| --- | --- | --- |
| Durable batch insert | `database.py:493-502` | one tx per batch |
| Lock retry | `database.py:358-377` | 3 attempts on `database is locked` |
| Thread-local connections | `database.py:252-281` | per-thread isolation |
| Joiner (idempotent) | `database.py:687-701` | `ON CONFLICT DO NOTHING` |
| JSONL export sink | `consumer.py` (JsonlExport) | alternative persistence |
| Outcome validation | `outcomes.py` | decision exists, value ∈ [0,1], known source, valid JSON |
| InMemoryOutcomeCollector | `outcomes.py` | test/offline substitute |

### Policy artifacts
| Feature | Where | Notes |
| --- | --- | --- |
| Versioned YAML, `policy_latest` | `policy.py` | immutable artifacts + pointer |
| `validate_policy` | `policy.py` | schema/type/value checks before publish |
| States: ACTIVE/DRAINING/REVOKED | `policy.py` | lifecycle joinable to serving |
| `load_policy` / `save_policy` / `rollback_policy` | `policy.py` | artifact round-trip + revert |
| Artifact history table | DB `policies` | journal of generated artifacts |

### CLI
| Command | Where | Notes |
| --- | --- | --- |
| `outcome` CLI | `outcomes.py` `main()` | scripted outcome logging from files |

## 6.2 Design-pattern catalog (with code anchors)

| Pattern | Anchor | Why here |
| --- | --- | --- |
| Facade / composition root | `__init__.py` `DecisionLedger` | one wiring point hides DOM wiring |
| Constructor DI | `Gatekeeper(telemetry=...)`, `ConformalCalibrator(database)`, `BatchConsumer(telemetry=...)` | test swaps; explicit deps |
| Strategy | `BatchConsumer` vs `JsonlExport`; `calibrate_context` vs `calibrate_by_context` | swap sinks/modes |
| Immutable snapshot / copy-on-write | `reload_policy`; `DecisionRecord` frozen+slots | lock-free readers |
| Repository-lite | `Database.get_decisions`, `get_join_statistics`, `coronium_snapshot` | typed surface over SQL |
| Producer/consumer (publish) | `RingBuffer` + `BatchConsumer` poll | hot/unhot decoupling |
| Validation funnel | `OutcomeCollector`, `validate_policy`, `normalize` in `__init__` | single choke-point checks |
| Fail-closed defaults | `evaluate` catch-all → ESCALATE (`__init__.py:225-233`) | safe default on unexpected errors |
| Watermark + idempotent writes | joiner `last_joined` + `DO NOTHING` | crash-recoverable pipelines |
| Enum/typing-driven API | `DecisionType`, `GateAction`, `ContextStatus`, `PolicyStatus`, `OutcomeSourceStatus` | self-documenting states |

## 6.3 Key software-engineering concepts demonstrated

1. **Non-conformity scoring.** The prefix-risk method over sorted
   non-conformity scores is conformal prediction's empirical-risk cousin;
   distinct from raw confidence thresholds precisely because it is computed
   from *paired outcomes*, not model internals.
2. **The offer/verdict separation.** The ledger records the *offer*
   (decision) eagerly and the *verdict* (outcome) asynchronously; the 
   calibration only ever consumes *paired* rows.
3. **Backpressure without blocking.** Bounded ring buffer + drop-first-then-log
   (exploratory records) avoids unbounded memory growth with zero producer
   stalls.
4. **Fail-closed vs fail-open.** Every trust-uncertain path defaults to
   ESCALATE; the docs repeatedly state "if we can't prove it's safe, escalate."
5. **Determinism over randomness** in exploration — enables exact rate checks in
   tests and reproducible budgets.
6. **Durability via batching.** The consumer trades a bounded staleness window
   for amortized writes; autocommit + explicit tx for the batch reconciles
   "no data-loss in the happy path" with throughput.
7. **Snapshot-based concurrency.** Copy-on-write policy dict, frozen records,
   and `threading.local` connections sidestep fine-grained locking on the hot
   path.

## 6.4 Anti-patterns deliberately avoided (with evidence)

| Anti-pattern | Decision Ledger instead |
| --- | --- |
| Global mutable singleton config | CI-style injected composition (DI everywhere) |
| Scattered SQL | `Database` domain queries + one schema owner |
| Framework-locked storage | swap-able sinks (SQLite/JSONL/in-memory) |
| Code-only config | validated, versioned, file-based policy artifacts |
| Silent data loss | drop *documented* (tiering) + critical-logging backlog cap; never silent |