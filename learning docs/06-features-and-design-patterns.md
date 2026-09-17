# 06 — Features & Design Patterns

## 6.1 Feature inventory

### Serving (hot path) & Acceleration
| Feature | Where | Notes |
| --- | --- | --- |
| `evaluate()` DELEGATE/ESCALATE | `gatekeeper.py:193-245` | fail-closed |
| Native Rust PyO3 Engine | `crates/decision_ledger_core` | SIMD BLAKE3 hashing + GIL-released evaluation |
| Universal Zero-Code LLM Wrappers | `adapters/openai_adapter.py`, `anthropic_adapter.py` | Drop-in `AutoLedgerOpenAI` & `AutoLedgerAnthropic` SDK wrappers |
| EXPLORE_SHADOW mode | `gatekeeper.py:231-245` | deterministic by call count |
| Deterministic exploration | `gatekeeper.py:18-21` | `n % int(1/rate) == 0`; no PRNG |
| Context isolation | `utils.make_context_hash` | each context has own threshold |
| Hot reload of policy | `reload_policy` / `gatekeeper.py` | atomic swap under single lock |
| Metrics & OpenTelemetry | `observability.py` / `gatekeeper.py` | OTel metrics + W3C TraceContext propagation |

### Calibration, Risk & Operations
| Feature | Where | Notes |
| --- | --- | --- |
| Per-context conformal threshold | `calibration.py:143-204` | `compute_threshold` |
| Joint Multi-Objective CRC | `calibration.py` (`MultiObjectiveConformalCalibrator`) | Multi-vector Pareto bounds across Accuracy, Latency & Cost |
| Off-Policy IPW Calibration | `calibration.py` (`IPWConformalCalibrator`) | Horvitz-Thompson IPW sampling for shadow log bias correction |
| Event-Driven Auto-Recalibration | `pipeline.py` (`AutoRecalibrationPipeline`) | Automated closed-loop drift detection & policy reload |
| Incident Webhooks | `webhooks.py` (`WebhookNotifier`) | Slack, PagerDuty & Teams alerts with exponential backoff |
| `min_sample_size` enforcement | `calibration.py` | escalate below it |
| Drift detection | `detect_drift` | threshold + feature-drift signal |

### Distributed Control-Plane & Data Infrastructure
| Feature | Where | Notes |
| --- | --- | --- |
| Ed25519 Policy Signing | `policy.py` | Cryptographic public/private key verification for YAML policies |
| Distributed Postgres & Redis | `docker-compose.yml`, `redis_bus.py` | Redis rate limiters & Pub/Sub policy channels |
| ClickHouse Telemetry Data Lake | `decision_ledger_server/clickhouse.py` | Column-oriented MergeTree storage for sub-second quantile queries |
| Bi-Directional gRPC Stream | `proto/decision_ledger.proto`, `grpc_server.py` | HTTP/2 Protobuf binary streaming |

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