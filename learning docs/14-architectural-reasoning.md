# 14 — Senior Architect Review: Why The Design Is What It Is

A decision-by-decision analysis of the repository as a whole system. Every
significant architectural and implementation choice is examined with the same
template:

> **WHY THIS** · **WHY NOT THAT** · **TRADEOFF** · **FAILURE POINT** ·
> **CHANGE CONDITION** · **SCALE CONDITION** · **LEARNING QUESTION**

**Reading protocol.** Evidence `[E]` means the claim is directly observable in
the code/config/docs. Inference `[I]` means it is a reasonable engineering
explanation *consistent with* the evidence but not stated by the developer.
Nothing here assumes the developer's intentions; where the rationale is
inferred it is labeled as such.

Decisions analyzed (index):

| # | Decision | Layer |
| --- | --- | --- |
| 1 | Embedded library, not a managed service | Architecture |
| 2 | Ring buffer + background consumer as the durability seam | Async processing |
| 3 | One dedicated writer + thread-local connections | Concurrency / state |
| 4 | SQLite, single file, WAL off | Database |
| 5 | Autocommit + batch transactions; retry reads, backlog writes | Database / error handling |
| 6 | Materialized `joined_records` + `ON CONFLICT DO NOTHING` | Database / state |
| 7 | One outcome per decision + outcome-source trust gating | State / integrity |
| 8 | Deterministic call-count exploration instead of a PRNG | State / sampling |
| 9 | Fail-closed as the universal error-handling default | Error handling / authorization |
| 10 | Context identity as a factor-hash; UUIDv7 record ids | API design / state |
| 11 | Immutable YAML policy artifacts + atomic in-memory snapshot swap | State / deployment |
| 12 | Non-conformity prefix-risk threshold + `min_sample_size` floor | Domain logic |
| 13 | Drift detection via accuracy divergence (full vs active range) | Domain logic |
| 14 | Dependency isolation: numpy/YAML off the hot path + lazy import | Abstractions / dependencies |
| 15 | Packaging: src-layout, `py.typed`, version-adaptive deps, Makefile gates | Infrastructure / deployment |

---

## 1. Embedded library — not a managed service

**WHY THIS**
* **What it does:** the entire product is an importable Python package. An
  embedding application constructs `DecisionLedger` (the composition root,
  `__init__.py:104-157`) and calls methods; there is no daemon, no HTTP
  server, no socket.
* **Where implemented:** `pyproject.toml` (a plain library distribution),
  `DecisionLedger` facade, the four examples that *embed* it
  (`src/examples/*.py`).
* **Problem it solves:** a trust gate for small-model control-plane decisions
  must sit *on the hot path of the application*, not behind a network call
  that adds latency and a deployment. `evaluate()` is ~1 µs — only possible
  if it shares the process.
* **Engineering principle:** *composition over operation* — ship behavior, not
  a process; let the operator own deployment.
* **Evidence:** verified zero imports of any HTTP/web framework across the
  package; the design docs call serving "the hot path"; the facade is a
  classic DI composition root.

**WHY NOT THAT** (alternatives)
- **HTTP service (REST/gRPC):** app calls a remote gatekeeper.
  *Advantages*: multi-language, multi-process, central policy, operability.
  *Disadvantages*: adds ~ms RPC latency + availability coupling + auth/TLS +
  deployment complexity. *Complexity*: high. *Performance*: destroys the µs
  hot path. *Scalability*: better horizontally, but now you have two systems
  to scale. *Maintainability/testability*: separate service with its own
  release cycle; harder local dev. *Operational consequences*: TLS, authN,
  load balancing, versioning — an ops surface that does not exist today.
- **A management CLI as the primary interface:** fine for ops, unusable as a
  per-call gate.
- **SDK over a sidecar daemon (OpenTelemetry-style agent):** keeps a thin
  client but adds process management; still imports a network hop.

**TRADEOFF**
Gains: zero-ops embeddability, ~µs latency, fully in-process state, trivial
dev/test. Sacrifices: cross-language reach, out-of-the-box multi-process
sharing, a clean external API versioning story `[I]`.

**FAILURE POINT**
Every replica embeds its own gatekeeper; the in-process `policy`
snapshot must be reloaded per replica. There is no shared calibration state,
so two processes can run different thresholds.

**CHANGE CONDITION**
A second service (different language, or a separate calibration job) needs to
consume the same decisions/policies, or latency becomes acceptable for network
hops. Then the design must sprout a server, not bend the library `[I]`.

**SCALE CONDITION**
Appropriate for single-process deployments (one app, one DB file). Becomes
awkward at multi-replica scale where the gate must be consistent across
instances.

**LEARNING QUESTION**
`evaluate()` is ~1µs *because* the library is embedded. If it became a network
service, which two budget numbers in this repo would break first, and which
architecture decision (ring buffer vs consumer) would have to change to
compensate?

---

## 2. Ring buffer + background consumer as the durability seam

**WHY THIS**
* **What it does:** `evaluate()` writes nothing to disk. It pushes a frozen
  `DecisionRecord` into a bounded `deque` (default 100k, `telemetry.py:115`);
  one consumer thread drains 1000 at a time every 0.1s and flushes batched
  transactions (`consumer.py:201-234`).
* **Where implemented:** `telemetry.py` (RingBuffer), `consumer.py`
  (BatchConsumer), wired in `__init__.py:149-157`.
* **Problem it solves:** SQLite on the hot path would make the producer wait
  on I/O. Separating *capture* (µs) from *durability* (µs-to-ms, batched)
  decouples serving latency from disk behavior — including stalls.
* **Design principle:** *eventual durability, never-blocking producer*; the
  buffer is a classic write-back with a documented adversarial policy (drop
  exploratory-first under overload, `telemetry.py:162-204`).
* **Evidence:** `push` "never blocks and never raises" (`telemetry.py:7-8`);
  flush conditions `batch_size (5000) OR flush_interval`; measured p50 7.7µs
  e2e with telemetry.

**WHY NOT THAT**
- **Synchronous write per decision:** simplest mental model; but each call
  pays fsync/tx overhead → hot path becomes disk-bound; write amplification.

  *Advantages*: exact durability, no loss window, trivial reasoning.
  *Disadvantages*: latency, jitter under load. *Complexity*: lowest.
  *Performance*: worst. *Scalability*: falls off a cliff. *Maintainability*:
  simplest. *Testability*: easiest. *Operational*: disk hiccups stall the
  producer. — Only viable if decisions are rare.
- **External durable queue (Kafka/Pulsar/Redis):** push to a broker, consume
  elsewhere. *Advantages*: no local loss, multi-process, replay. *Disadvantages*:
  ops burden, network latency on the hot path, new failure domain.
  *Complexity*: high. *Performance*: µs → ~ms. *Scalability*: best.
  *Operational*: this is a *different product* — explicitly rejected by the
  "embedded single-file" stances `[E]`.

**TRADEOFF**
Gains: hot path never blocks; amortized writes (~118k rows/s in one tx).
Sacrifices: bounded staleness (a decision is durable only after flush) and a
small **hard-kill loss window** (up to ~one batch/100ms); under sustained
overload the buffer becomes *lossy* (documented, counted, logged — never
silent) `[E]`.

**FAILURE POINT**
- **MED-3** (known): `drain_now()` and the loop thread both flush; the loop
  pops by count, so records appended between a pop and a flush can be
  popped-but-never-written.
- A hard kill during the poll→flush window loses in-flight records.
- `BACKLOG_CAPACITY = 100_000` + `BACKLOG_ALERT_LEVEL = 50_000`: if the DB
  stays down, oldest records are *dropped deliberately* with CRITICAL logs.

**CHANGE CONDITION**
A requirement for **durable-at-capture** semantics (every decision committed
before the caller returns) would force a synchronous path or a WAL/queue
first. Also any requirement to share decisions across processes.

**SCALE CONDITION**
The single buffer + single consumer caps practical throughput at what one
SQLite writer sustains (~10⁵/s); above that — or when multi-host durability is
mandatory — replace the buffer/consumer with an external log `[I]`.

**LEARNING QUESTION**
The buffer is "lossy under overload by design". Why is dropping
*exploratory records first* the right loss policy, and what statistic would
silently corrupt if the code dropped *delegated* records instead?

---

## 3. One dedicated writer + thread-local connections

**WHY THIS**
* **What it does:** only the consumer thread batch-writes decisions; every
  other thread gets its own lazy SQLite connection (`threading.local`,
  `database.py:252-281`), and `close()` can run from any thread
  (`check_same_thread=False`).
* **Problem it solves:** SQLite allows one writer at a time. Channeling all
  *decision* writes through one thread removes most write-write contention
  outright, and `busy_timeout` + retry handles the residue.
* **Principle:** *single-writer discipline* as a concurrency strategy — the
  SQLite analog of "one goroutine mutates shared state".
* **Evidence:** consumer docstring "durable" path; `_open_new` sets
  `isolation_level=None`, `busy_timeout=2000ms`, `foreign_keys=ON`; the Joiner
  docstring; tests assert no data loss under producer/consumer concurrency.

**WHY NOT THAT**
- **One shared global connection:** simplest; but SQLite connections are not
  thread-safe by default, and one connection serializes *reads* too.
  *Advantages*: trivial. *Disadvantages*: cross-thread serialization of all
  queries; `check_same_thread` hazards. *Operational*: one bad cursor poisons
  every caller.
- **Write pooling / multiple writers:** gives concurrency but Linear
  SQLite *regression* soon dominates; only worth it with WAL +
  server-side queuing.
- **A full connection pool (sqlalchemy):** heavyweight for the query surface
  here; the repo keeps DB as a thin typed layer (no ORM).

**TRADEOFF**
Gains: near-zero read/read and write/read contention, per-thread isolation,
simple mental model. Sacrifices: no multi-process writer story; the "one
writer" is an *assumption* relied on to keep the default journal mode sane.

**FAILURE POINT**
A second process writing to the same file breaks the single-writer invariant;
contention returns and the 3-retry lock handler starts firing. The code says
so implicitly — `_run_with_lock_retry` exists because locks still happen
(`database.py:358-377`).

**CHANGE CONDITION**
Multi-process deployment, or a second writer (e.g. an offline importer)
writing while serving.

**SCALE CONDITION**
Healthy in one process ~10⁵ decisions/s. Beyond that, the single connection
becomes the bottleneck and the whole storage layer must change (`[I]`, see
alterative B in §11 of `13-all-features-deep-dive.md`).

**LEARNING QUESTION**
`_open_new` sets `check_same_thread=False` but every thread still gets its own
connection. In what realistic interleaving does the "own connection" invariant
hold, yet a `database is locked` still fires?

---

## 4. SQLite, single file, WAL off

**WHY THIS**
* **What it does:** one `.db` file is the entire durable store; published
  intent is that the DB stays a **single portable file** — hence WAL is
  explicitly disabled (`database.py:20-22`).
* **Problem it solves:** zero-ops durability that can be copied, synced,
  backed up atomically (e.g. OneDrive-style file sync), and shipped around.
* **Principle:** *simplest correct store*; portability over peak write
  concurrency.
* **Evidence:** code comment cites the single-file rationale; no migration
  layer exists (schema is idempotently re-applied per connection,
  `database.py:280`).

**WHY NOT THAT**
- **WAL journaling on:** standard SQLite advice. *Advantages*: concurrent
  readers during a writer, crash rollback, faster. *Disadvantages*: creates
  `-wal`/`-shm` companion files — breaks the "one file" promise and makes
  naive file-sync/backup unsafe. Here that promise is the point.
- **Postgres/etc.:** real server durability, concurrency, but an ops system —
  contradicts the embedded pitch (see Decision 1).
- **DuckDB/libsql:** interesting, but adds a dependency and a less-tested
  embedding story than stdlib `sqlite3`.

**TRADEOFF**
Gains: portable single file, zero ops, stdlib support. Sacrifices: writer/
reader contention in default journal mode (ameliorated by single-writer
discipline + retry), no cross-process durability story, fsync per autocommit
write.

**FAILURE POINT**
- External file-sync (OneDrive) copying the file while a batch is mid-commit
  can produce a torn state (idempotent joins limit the damage) `[LOW-10]`.
- A crash between `policy_<ts>.yaml` write and `policy_latest` repoint leaves
  `latest` behind (non-atomic artifact publish, LOW-6).

**CHANGE CONDITION**
Required write concurrency that default-journal mode can't absorb, or
multi-writer/multi-host durability requirements.

**SCALE CONDITION**
Single-file SQLite stays comfortable to ~GBs and ~10⁵ writes/s serialized;
fine for this product's stated scale (per-context *calibration data*, not
a transactions ledger) `[I]`.

**LEARNING QUESTION**
Turning WAL on "fixes" contention but breaks the one-file promise the comment
at `database.py:20-22` was written to protect. What real-world workflow does
the single-file form enable, and where does that workflow itself become the
fragility?

---

## 5. Autocommit writes + explicit batch transactions; retry reads, backlog writes

**WHY THIS**
* **What it does:** every connection is autocommit (`isolation_level=None`),
  so each `execute_write` commits immediately; the consumer groups up to
  `batch_size` decisions into **one** explicit `BEGIN…COMMIT`
  (`database.py:493+`). DB-locked *reads* retry up to 3 times with backoff
  (`_run_with_lock_retry`, `database.py:358-377`); *writes* do **not** retry
  indefinitely — the consumer keeps records in memory (cap 100k, alert
  50k) and backoff-flushes (`consumer.py:247-269,273-337`).
* **Problem it solves:** per-record commits are durable but slow; one batch
  tx amortizes while preserving atomicity *per batch*. Reads can afford to
  spin briefly; the *writer* must never block the producer, so it buffers
  instead.
* **Principle:** two different philosophies for two different invariants —
  *reads*: attempt-and-retry; *writes*: never stall the pipeline, degrade
  loudly (drop + CRITICAL) rather than silently.
* **Evidence:** constants `_LOCK_RETRIES=3`, `_LOCK_RETRY_DELAY_S=0.05`,
  `_BUSY_TIMEOUT_MS=2000`, `FAILURE_BACKOFF_S=1.0`, `BACKLOG_CAPACITY`,
  `BACKLOG_ALERT_LEVEL`.

**WHY NOT THAT**
- **Single global transaction context (like Django's `atomic`):** convenient
  but fights the producer/consumer split; long-lived tx would freeze the
  single writer.
- **Retry writes too:** a down DB would keep the consumer thread busy
  spinning; buffering + backoff is strictly better for throughput.
- **Full WAL + busy_timeout only:** see Decision 4.

**TRADEOFF**
Gains: durable-lite semantics (records are safe within a batch's commit),
bounded staleness, no producer stalls. Sacrifices: the *batch* is the atomic
unit (not the record), and under persistent failure records are eventually
dropped — loudly.

**FAILURE POINT**
- MED-3 race (drain vs flush).
- If the DB is down *past* the buffer capacity, oldest records are dropped —
  still an intentional, counted, reported loss (never silent).
- Three failed retries on a *read* raise `DatabaseError` — monitoring must
  treat this as an alert signal, not a retry loop.

**CHANGE CONDITION**
An SLA that requires zero-loss even under DB outage (then: external queue),
or per-record atomicity (then: synchronous writes).

**SCALE CONDITION**
The batched-write amortization keeps per-row cost flat up to what one writer
sustains (~10⁵/s); the memory-backed backlog is a *failure* mode, not a
scaling path.

**LEARNING QUESTION**
Reads retry with backoff; writes buffer and eventually *drop*. Why is
"retry the reader" the right call and "buffer the writer" the right call —
which two invariants does each choice protect, and which one breaks if you
swap them?

---

## 6. Materialized `joined_records` + `ON CONFLICT DO NOTHING`

**WHY THIS**
* **What it does:** the joiner materializes a denormalized copy of
  `decisions ⋈ outcomes` into `joined_records` — one row per decision
  (`database.py:640-704`). Calibration reads *that single flat table*, never
  a live join (`calibration.py:262-274`).
* **Problem it solves:** calibration over a live join is O(corpus) per run;
  a materialized copy with the right indexes is O(reading one table). Re-run
  safety comes from `joined_id = decision_id` + `ON CONFLICT(joined_id) DO
  NOTHING`, i.e., **each decision is joined at most once** (Joiner docstring).
* **Principle:** *denormalize for the read path; make the materialization
  idempotent so refresh is a no-op re-run*.
* **Evidence:** `database.py:687-701`; calibration's single `EXISTS`-filtered
  SELECT; pipeline `_context_hashes()` reads joined rows.

**WHY NOT THAT**
- **Live view:** `CREATE VIEW joined AS …` — *Advantages*: always fresh, zero
  maintenance. *Disadvantages*: every calibration pass re-executes the join;
  can't index the materialized columns the same way; subtle after failure
  semantics. *Performance/scalability*: worse as corpus grows.
- **On-the-fly join per context in calibration:** same O(corpus) cost.
- **Event-sourced replay:** powerful but heavyweight for this scope.

**TRADEOFF**
Gains: fast, indexed, predictable calibration reads; idempotent crash-safe
materialization. Sacrifices: staleness between joins; a store to keep in sync;
**MED-1**: a decision joined *before* its outcome arrives keeps `NULL`
forever — the late outcome never calibrates.

**FAILURE POINT**
- MED-1 is the canonical failure: slow-label pipelines (human review) can
  silently exclude a fraction of data from threshold updates.
- Joiner defensively "keeps the first-inserted outcome" (docstring) — if the
  single-outcome invariant (Decision 7) were ever relaxed, results here would
  silently be "first label wins".

**CHANGE CONDITION**
Outcomes that routinely arrive *after* the first join (human-in-the-loop
labels) → need a re-join or `DO UPDATE` pass, or a purpose-built join-window.

**SCALE CONDITION**
Covers the repo's scale comfortably. At very large corpora the materialized
table grows 1:1 with decisions (bounded, indexable) — less dangerous than the
*staleness* failure than raw volume `[I]`.

**LEARNING QUESTION**
`ON CONFLICT(joined_id) DO NOTHING` gives idempotency and NULL-staleness at
the same time. Rewrite it (a) to rejoin late outcomes and (b) keep it
idempotent — and name the invariant that both mechanisms are balancing.

---

## 7. One outcome per decision + outcome-source trust gating

**WHY THIS**
* **What it does:** a decision can receive **at most one** outcome. The
  collector enforces it (`UNIQUE` decision linkage + validation,
  `outcomes.py:378-385`), and `OutcomeSource` (human, task_metric,
  user_report, model_verification) gates which labels may calibrate —
  `MODEL_VERIFICATION` is **excluded** as self-confirming
  (`calibration.py:58-60`, `_INDEPENDENT_OUTCOME_SOURCES`).
* **Problem it solves:** the risk math needs exactly one label per decision;
  a self-verifying model would create circular evidence (confidence feeds
  outcome feeds threshold).
* **Principle:** *integrity-by-construction* (schema + whitelist) instead of
  trusting callers.
* **Evidence:** schema `OUTCOMES.decision_id` references decisions + UNIQUE;
  the joiner's "first-inserted outcome" defensive note; the drift detector
  reads paired rows only.

**WHY NOT THAT**
- **Many outcomes per decision (time-series labels):** *Advantages*: supports
  repeated evaluation. *Disadvantages*: calibration must then define an
  aggregation (first? last? mean loss?) — added math + ambiguity. The repo
  chose "first label is the ground truth" implicitly via the join.
- **Latest-wins:** loses the append-only audit story.
- **Trusting any source:** `model_verification` would enter calibration and
  bias q_hat optimistically (the model grading its own confidence is not
  exchangeable evidence).

**TRADEOFF**
Gains: unambiguous labels, clean math, no self-confirmation. Sacrifices: no
support for tasks that need repeated/multi-label outcomes; "one label wins"
loses information if a decision genuinely deserves several evaluations.

**FAILURE POINT**
- Partial-credit or multi-objective outcomes can't be represented.
- If a wrong label is written first, it is locked in (no correction path in
  the schema).
- The `human` default on the facade (`outcome_source="human"`) vs the table
  default `'task_metric'` is a silent asymmetry worth noticing `[E]`.

**CHANGE CONDITION**
Business rules requiring aggregate/multi-label outcomes, or editable labels
with an audit trail.

**SCALE CONDITION**
No scale issue in itself; it's a *semantics* decision.

**LEARNING QUESTION**
`MODEL_VERIFICATION` outcomes are stored but never calibrate. Why is storing
them at all (rather than rejecting them) the right call, and what observability
value do they retain?

---

## 8. Deterministic call-count exploration instead of a PRNG

**WHY THIS**
* **What it does:** eligible call `n` explores when `n % int(1/rate) == 0` —
  at rate 0.02, exactly calls 0, 50, 100… (`gatekeeper.py:231-245`). The
  counter advances only for *eligible* calls (post fail-closed checks).
* **Problem it solves:** shadow-observation must happen at a *known, honest,
  reproducible* rate. A PRNG gives a rate only *statistically*.
* **Principle:** *determinism over randomness* for a policy that must be
  audited and tested.
* **Evidence:** design note in the module docstring; the counter treated as
  atomic under GIL + RLock; tests assert the exact 1-in-50 behavior.

**WHY NOT THAT**
- **`random.random() < rate` per call:** *Advantages*: statistically
  unbiased per-call, no ordering coupling. *Disadvantages*: unrepeatable
  experiments, rate never exactly 2%, flaky tests, harder audit story.
- **Lockstep time-bucket sampling:** e.g. "2% of each minute" — overkill; the
  call counter is simpler and self-contained.
- **Weighted random (context-stratified):** useful if contexts cycle
  unevenly, but adds state.

**TRADEOFF**
Gains: exact measurable rate, reproducibility, trivial assertions. Sacrifices:
per-call independence — the sample is *periodic in call count*, so if a
context's load is itself periodic (e.g., aligned with the 50-call period)
exploration could correlate with load.

**FAILURE POINT**
- A caller that can predict the exploration slots and bias their call timing
  could starve or flood shadow observations (low real-world risk for an
  internal control plane `[I]`).
- The rate is a global constant per gatekeeper, not per-context — a context
  under light load explores rarely in absolute terms.

**CHANGE CONDITION**
A requirement for *per-context* or *adaptive* exploration rates, or workload
patterns where call-periodic sampling demonstrably correlates with input
distributions.

**SCALE CONDITION**
None — the counter is O(1). It's a correctness/statistics decision, not a
scaling one.

**LEARNING QUESTION**
The counter only advances for *eligible* calls (after the fail-closed checks).
Why does that detail matter for the drift detector in Decision 13, and what
would silently happen if `_should_explore` counted *every* call including
unknown contexts?

---

## 9. Fail-closed as the universal error-handling default

**WHY THIS**
* **What it does:** the five "don't trust" cases + any unexpected exception in
  `evaluate` all return `ESCALATE` (`gatekeeper.py:193-214`,
  `__init__.py:225-233`); after shutdown every API raises
  (`_require_open`). The system's default answer to uncertainty is "the small
  model does not act; fall back to the strong model/human".
* **Problem it solves:** control-plane *action* is the dangerous resource. The
  cost of a wrong delegation (damage) is assumed far higher than the cost of a
  needless escalation (money/latency). So doubt must resolve *up*, never
  *down*.
* **Principle:** *availability of safety over availability of delegation* — the
  inverse of typical fail-open web scaling.
* **Evidence:** module docstring "Fail closed"; `min_sample_size` enforcement
  (Decision 12) is a special case; the facade catch-all exists specifically
  "Any unexpected failure on the hot path fails closed" (`__init__.py:227`).

**WHY NOT THAT**
- **Fail-open (delegate on uncertainty):** *Advantages*: maximum serving
  availability. *Disadvantages*: the exact hazard the product exists to
  prevent. Rejected on domain grounds.
- **Fail-soft / last-known-good:** keep the old threshold on new failure
  types. *Advantages*: more availability than hard-close. *Disadvantages*: an
  old threshold on *stale* data is exactly a drift hazard; and "last known
  good" interacts badly with the calibration cadence `[I]`.
- **Circuit-breaker semantics (throttle then trip):** attractive for *load*,
  but conflates load-safety with data-safety.

**TRADEOFF**
Gains: hard safety default; a confused system degrades to "nothing
delegates". Sacrifices: **economic risk** — if the confidence signal
systematically breaks or contexts churn faster than calibration (see Decision
10's failure point), the gate escalates everything and the falling-through
cost (frontier-model spend, latency) runs up.

**FAILURE POINT**
- Sustained drift + churn → 100% escalation → cost blow-up the design accepts
  but monitoring must catch (the drift summary prints `drift=N`).
- A bug *inside* the fail-closed branch itself can't be distinguished from a
  legitimate deny — operational forensics need the Escalation metrics
  (`get_metrics`).

**CHANGE CONDITION**
If the *cost of false escalation* becomes comparable to the cost of false
delegation (e.g., every decision going to a frontier model at scale), a
fail-soft tier with explicit risk labels becomes justifiable.

**SCALE CONDITION**
None — fail-closed is a policy, not a scale function.

**LEARNING QUESTION**
ESCALATE is "deny by default". Design the metric you would watch to catch
the scenario where a *correct* fail-closed system is quietly bankrupting the
product by escalating everything — and explain why the current
`get_metrics()` layout supports that watch.

---

## 10. Context identity as a factor-hash; UUIDv7 record ids

**WHY THIS**
* **What it does:** a "context" is a deterministic 128-bit hash of `(model_id,
  task_type, prompt_template_version, quantization, adapter_config)`
  (`make_context_hash`, `utils.py:86-129`), domain-separated by a version
  prefix; each decision gets a time-ordered UUIDv7 id (`generate_uuidv7`,
  `utils.py:132-160`).
* **Problem it solves:** calibration is keyed on identity. The hash guarantees
  that *any* factor change yields a fresh context — "calibration and caching
  keyed on this hash can never silently mix two different model/task
  configurations" (docstring). UUIDv7 gives causal ordering without a central
  sequence.
* **Principle:** *identity derived from the observable configuration*; ids
  that order themselves.
* **Evidence:** factor tuple + version prefix + `repr` serialization (so
  `("ab","c") ≠ ("a","bc")`); the UUIDv7 → stdlib/PyPI/degenerate fallback
  chain.

**WHY NOT THAT**
- **Raw context strings as PK:** readable, but a "context" is a *compound*
  configuration; strings invite ad-hoc formats and silent collisions `[I]`.
- **Auto-increment integers:** no cross-restart stability, no config
  semantics — useless as a *cache key*.
- **Full 256-bit hashes:** 16 bytes is already astronomically collision-safe
  and keeps keys compact in the hot-path dict.

**TRADEOFF**
Gains: hard identity guarantee, opaque-but-validated keys, ordered ids.
Sacrifices: **debuggability** (a `context_hash.hex()` means nothing without a
registry of factor sets), and **identity explosion**: per-entity calibration
needs *enough* samples per hash before anything delegates.

**FAILURE POINT**
Frequent config churn (a new adapter version weekly) mints new contexts faster
than they collect ≥100 independent outcomes → permanent escalation → the
fail-closed economics of Decision 9 materialize. The hash is simultaneously
the feature (no mixing) and the failure (no sharing) — there is **no grouping
or fallback semantics** between adjacent contexts `[E]`.

**CHANGE CONDITION**
A requirement to generalize trust across related contexts (e.g. "any version
of this adapter") or to debug serving by human-readable context labels —
needs an identity registry/grouping layer.

**SCALE CONDITION**
Memory-wise trivial. The real bound is the *sample-accumulation* cost per
(context) × (config churn rate).

**LEARNING QUESTION**
Deploying adapter v2 mints a brand-new context hash. Explain — exactly, from
`make_context_hash` + `min_sample_size` — what a caller must do before *any*
of their decisions under v2 delegate, and why that is a *feature* and a
*failure* at the same time.

---

## 11. Immutable YAML policy artifacts + atomic in-memory snapshot swap

**WHY THIS**
* **What it does:** calibration results are serialized to versioned, schema-1.0
  YAML files (`policy_<ts>.yaml`) plus a `policy_latest.yaml` pointer
  (`policy.py:310-361,421-427`); loading validates, converts to
  `CalibrationContext` dict, and **replaces the whole dict reference under one
  short RLock** (`reload_policy`, `gatekeeper.py:247-254`).
* **Problem it solves:** policies must be auditable (append-only history),
  rollbackable (`rollback_policy`), and *swap-consistent* — a reader must
  never see half old/half new rules.
* **Principle:** *immutable artifact + copy-on-write snapshot*; files are the
  interchange, memory is a pure reflection.
* **Evidence:** artifact library with dedup errors; `_refresh_latest_link`
  symlink-or-copy (Windows-aware); docs on atomic swap; in-flight evaluate
  finishes on the old snapshot.

**WHY NOT THAT**
- **Only the DB `policies` table serves the gate:** correct but lacks the
  human/ops file ergonomics and the "click to rollback" story.
- **Mutable in-place threshold update:** *Advantages*: simplest.
  *Disadvantages*: concurrent `evaluate()` could observe torn state; no
  audit trail — rejected in favor of the swap.
- **Central policy service (K/V, etcd-style):** right only when there are many
  replicas (see Decision 1).

**TRADEOFF**
Gains: auditable, rollbackable, consistent reads. Sacrifices: artifact writes
are **not atomic** (LOW-6 — `write_text` directly to the final name); the
`latest` pointer is a second thing to keep consistent; symlink semantics vary
by OS.

**FAILURE POINT**
- A crash between artifact write and link repoint OR a torn file under sync
  (LOW-6/10) → `load_policy` fails → **reload is skipped → the old snapshot
  serves** (safe, but silently stale).
- `policy_latest.yaml` copied on Windows instead of symlinked — two files that
  must not drift.

**CHANGE CONDITION**
Multi-node serving (shared policy registry), or a requirement that every
publish be atomic+failed-closed (then: temp-write + rename + fsync).

**SCALE CONDITION**
Fine at human/ops cadence (calibrate is a batch job, not a hot path). Not a
scaling concern.

**LEARNING QUESTION**
`reload_policy` swaps the *entire* dict under a lock — but the artifact file
write is non-atomic. Order the events of "artifact written", "latest
repointed", and "snapshot swapped" for the *safe* case, and describe the one
ordering where the serving system and the on-disk latest disagree.

---

## 12. Non-conformity prefix-risk threshold + `min_sample_size` floor

**WHY THIS**
* **What it does:** calibration converts each verified decision to a
  non-conformity score `S = 1 - confidence` and a binary loss; sorts by S;
  computes the cumulative empirical risk of each prefix; sets `q_hat` to the
  largest prefix score with risk ≤ `alpha` (default 0.05); reports a Wilson
  coverage lower bound (`calibration.py:143-204`). Below `min_sample_size`
  (default 100) `q_hat` is `None` (→ escalate, Decision 9).
* **Problem it solves:** a raw confidence cutoff encodes *nothing* about real
  outcomes. Bounding *measured* prefix risk roots trust in data, not in the
  model's self-report.
* **Principle:** *empirical risk control over exchangeable record pairs*;
  conservative defaults (floor) beat clever ones (variance-penalized).
* **Evidence:** the filter `is_independent and not is_exploratory`
  (`calibration.py:151`); the `min_sample_size` gate; the Wilson bound; label
  "Split Conformal Risk Control" in the class docstring.

**WHY NOT THAT**
- **Fixed threshold "conf > 0.7":** no outcome coupling, unmeasurable — the
  thing this product exists to avoid.
- **Logistic/isotonic recalibration of confidence:** *Advantages*: a smoother
  probability model. *Disadvantages*: a training+validation split, feature
  engineering, recalibration cadence — an ML project inside the trust layer.
  *Complexity/testability*: far worse. The prefix-risk is 10 lines of numpy.
- **Full conformal quantile:** similar spirit; the empirical-risk-prefix
  variant is simpler and *monotone*, matching the "largest score that stays in
  budget" business rule.

**TRADEOFF**
Gains: a purchase on measured risk + tuneable `alpha` + an honest coverage
bound. Sacrifices: needs labeled data (hence the outcome side of the ledger),
a floor before trusting, and — **critically — the *docs claim finite-sample
guarantees the estimator alone does not deliver*** (`MED-2`: the implemented
quantity is an empirical-risk plug-in, not the CRC monotone-risk quantile).
Also excludes exploratory data, delaying activation until 100 independent
non-exploratory outcomes accumulate `[E]` + `[I]`.

**FAILURE POINT**
- Non-exchangeable input (concept drift, correlated batches) weakens the
  prefix-risk interpretation.
- Class imbalance: binary 0/1 loss hides asymmetric costs; a "95% correct"
  domain with catastrophic mistakes passes the risk gate.
- The empirical-risk claim overreach (MED-2) is a *documentation-failure*
  until implemented.

**CHANGE CONDITION**
Asymmetric error costs, or multi-class outcomes, or a genuine finite-sample
guarantee requirement (then implement the monotone-risk construction).

**SCALE CONDITION**
Data-accumulation-bound: every context needs ≥ `min_sample_size` outcomes;
cost grows linearly in (contexts × samples). Computation itself is
`O(n log n)` numpy and benchmarked < 120 s at 100k×100 contexts.

**LEARNING QUESTION**
Wildly confident contexts (accuracy 0.99) get `q_hat` *close to zero* — but
so do contexts with `accuracy` just under `alpha`. Explain in one sentence
what q_hat measures that raw accuracy does **not**, and why two contexts with
the same accuracy can still be assigned very different `q_hat`.

---

## 13. Drift detection via accuracy divergence (full vs active range)

**WHY THIS**
* **What it does:** per context, accuracy over *all* paired records (esp.
  `EXPLORE_SHADOW`) is compared to accuracy over `DELEGATE` records; an
  absolute divergence > 5% (`DRIFT_DIVERGENCE_THRESHOLD = 0.05`) flags drift
  (`calibration.py:296-345`).
* **Problem it solves:** the confidence score is the *input* to the whole
  threshold. The cheapest, most direct drift = "confidence stopped predicting
  success". The full-vs-active split is a built-in exchangeability probe.
* **Principle:** *measure the assumption, not a proxy* — the assumption is
  that `S` is informative; divergence of success between explored (low
  confidence) and delegated (high confidence) ranges is exactly that
  assumption breaking.
* **Evidence:** `DRIFT_DIVERGENCE_THRESHOLD`; the docstring "the confidence
  score has stopped being informative"; `get_calibration_stats` surfaces
  `drift_detected_count` + drifted contexts.

**WHY NOT THAT**
- **PSI/KS on feature distributions:** superbly general, but needs raw input
  features — the ledger stores none (only confidence, action, outcome). Not
  implementable without a harvesting change.
- **`q_hat`-change monitoring (σ over time):** catches threshold movement, not
  *why*; noisy at sample floors.
- **Online error-rate control (CUSUM/SPRT):** the honest statistician's tool,
  but adds sequential-testing machinery; the batched calibrate cadence makes
  a batched divergence check the pragmatic middle.

**TRADEOFF**
Gains: zero extra data, simple, interpretable, actionable summary. Sacrifices:
narrow coverage — covariate shift that *does not move* the full-vs-active
accuracy gap is invisible, and drift only **warns**; the pipeline still
republishes and does not block (that is now flagged `[note]` in the deep-dive,
§12 Q18).

**FAILURE POINT**
- Drift in the *delegated tail only* (no exploratory coverage there) is
  undetectable by this comparison.
- Early on, both ranges have tiny n — the accuracy estimates are noisy and
  the divergence check can false-positive.

**CHANGE CONDITION**
A requirement to monitor raw-feature drift or to *block* republish on drift —
either needs data/features the repo deliberately does not store, or a policy
change.

**SCALE CONDITION**
None (O(rows) read, batched).

**LEARNING QUESTION**
The check compares EXPLORE_SHADOW-vs-DELEGATE accuracy. Why is this the "right
assumption to check" for *this* system, and what specific drift scenario could
flat-line this check while trust is still badly degraded?

---

## 14. Dependency isolation: numpy/YAML off the hot path + lazy import

**WHY THIS**
* **What it does:** numpy (`numpy as np`) appears *only* in `calibration.py` for
  the vectorized quantile; PyYAML *only* in `policy.py`; `gatekeeper`,
  `telemetry`, `consumer` import only stdlib + `utils`; the one cycle
  (`gatekeeper` ⇄ `policy`) is broken by a **lazy import inside a method**
  (`gatekeeper.py:323`).
* **Problem it solves:** (a) the hot path (`evaluate`'s callers) never pays
  numpy import cost or memory; (b) the module graph stays a clean DAG so
  circular-import breakage can't appear later; (c) failure/deck surface is
  localized (e.g., numpy misinstall only harms calibration).
* **Principle:** *dependency direction follows execution frequency*; *import
  cost is architecture*.
* **Evidence:** per-module import lines (grep-verified); the lazy import
  comment; benchmark showing ~1µs evaluate (no numpy in that path).

**WHY NOT THAT**
- **Import numpy everywhere (or a single `common` catch-all module):**
  *Advantages*: one place to look. *Disadvantages*: every process import pays
  numpy; every module gains a heavyweight transitive dep; the DAG
  simplification is lost.
- **Pure-Python quantile (bisect/statistics):** *Advantages*: zero numpy, no
  version pinning. *Disadvantages*: slower on 100k-scale calibration; numpy is
  already a declared runtime dep — the repo accepted it *for calibration
  where it's run rarely*.
- **Remove the cycle by moving file-loading up**: the lazy import is a
  deliberate, documented seam — reorganizing solely to remove it would create
  an artificial dependency inversion.

**TRADEOFF**
Gains: cheap imports on the serving path, a clean DAG, localized dep risk.
Sacrifices: a slightly subtle lazy import that future editors must not
"clean up" into a top-level import (which would re-introduce the cycle).

**FAILURE POINT**
- A future editor hoisting the lazy import to module scope breaks the package
  import (circular).
- numpy/PyyAML version pins must be maintained (deps declared in
  `pyproject.toml`).
- On platforms without numpy wheels, *calibration* fails while serving is
  fine — a split availability profile some operators will not expect.

**CHANGE CONDITION**
Dropping numpy (pure-Python quantile) if wheels become an ops problem; adding
any feature that needs a shared numeric type across hot/cold paths would
increase coupling.

**SCALE CONDITION**
None for imports. Numpy's calibration memory is O(n) float arrays — fine at
stated scale.

**LEARNING QUESTION**
Why is the *direction* of the dependency (`hot` modules must not import
`cold` modules) more important than the *names* of the libraries — and what
specific runtime failure would the lazy import at `gatekeeper.py:323`
prevent?

---

## 15. Packaging, tools, and the release gate

**WHY THIS**
* **What it does:** src-layout (`src/decision_ledger`), `py.typed` (PEP 561),
  dynamic version from `__version__.py`, dual config (`pyproject.toml` PEP 621
  + `setup.py` mirror), `MANIFEST.in` for docs in the sdist, and a **local
  gate** (Makefile: black → flake8 → mypy --strict → pytest) with **no hosted
  CI** (`.github` absent). Dependencies are **version-adaptive**: `uuid7`
  stdlib-on-3.14 → PyPI `uuid6` → degenerate uuid4; `hashlib.blake3` on
  3.14 → PyPI `blake3` (`utils.py:126-160`, `_blake3_factory`).
* **Problem it solves:** one importable, typed, self-describing artifact that
  runs on anything ≥3.8 without forking a repo or hosting a platform; the
  "release gate" lives where the work happens.
* **Principle:** *package hygiene + testable distribution*, and *adapt to the
  interpreter rather than pinning it*.
* **Evidence:** `src-layout` (tests import installed pkg), `py.typed`,
  `pyproject.toml` tool configs, the fallback chains (grep-verified), no CI
  manifest.

**WHY NOT THAT**
- **Hosted CI (GH Actions/ etc.):** *Advantages*: enforced gates on merge,
  matrix runs (3.8–3.14), artifact publishing. *Disadvantages*: none really —
  this is a **gap** as much as a choice: the gate is developer-local and can
  be skipped. `[I]`: the repo documents the "checklist" as a commit-bearing
  gate, which a team of one can live with; any team needs real CI.
- **Pin to Python 3.14-only, stdlib-everything:** *Advantages*: no fallbacks,
  simpler truth table. *Disadvantages*: abandons ≥3.8 users the package claims
  to support.
- **No `py.typed`/src-layout:** hurts downstream typing and import hygiene
  (`stray root-package` trap).

**TRADEOFF**
Gains: broad interpreter support, clean typed distribution, reproducible local
gates. Sacrifices: fallback branches that are largely **untested** (the
`uuid6`/`blake3`/`uuid4` paths and the 3.8–3.13 matrix are not exercised in
CI — a real test gap), and a gate that a committed developer can bypass.

**FAILURE POINT**
- A 3.8 user hits a fallback bug that only ever ran on 3.14 here.
- `pip install -e` from the root relies on src-layout wiring — a misconfigured
  venv silently imports stale code.
- `dist/` + egg-info are ignored but `build/` artifacts can drift from `src/`.

**CHANGE CONDITION**
A team/multi-contributor workflow (then: hosted CI), or a minimum-version bump
that lets the fallbacks be deleted. The `pyproject` still carries **placeholder
URLs** (LOW-5) — a release-blocking tidy-up before public distribution.

**SCALE CONDITION**
None (it's a distribution concern, not a runtime scale).

**LEARNING QUESTION**
List the *three distinct Python-version fallback chains* in `utils.py`, tell
me which one is actively exercised by the test suite on 3.14, and predict the
kind of bug the other two hide until someone runs this on 3.11.

---

## Cross-cutting summary

**What the whole picture says.** The repository is one coherent bet: *"embed a
statistical trust loop in-memory, keep the serving path µs and allocation-free,
keep every state transition auditable, and fail to safety on any doubt."*
Every decision cross-locks with the others:

* Decisions 1–3 make the hot path possible and cheap.
* Decisions 4–7 make the data honest and durable.
* Decisions 8–10 make sampling deterministic and identity unambiguous.
* Decisions 9 + 12 are the *safety spine* (deny when uncertain).
* Decision 11 is the *operational switch* (audit/rollback).
* Decisions 13–15 keep the system measurable, clean to depend on, and cheap to
  ship.

The visible weaknesses are all *known seams* of these choices (MED-1 late
outcomes, MED-2 guarantee overreach, MED-3 flush race, LOW-6 non-atomic
artifact writes, LOW-5 placeholder metadata, no CI). None of them invalidates
the core bet; each is a *testable* next feature `[E]` + `[I]`.

**The single most important question.** Ask an engineer to redesign *one*
layer and watch how the others shift: "Make the ledger multi-process." It
touches Decisions 1, 2, 3, 4, 11 at once — which is exactly what an
architecture-in-the-large interview should probe.

## Appendix — evidence map for the claims above

| Claim | Anchor |
| --- | --- |
| Hot path no HTTP / embedded | verified: zero web-framework imports; facade `__init__.py:104` |
| RingBuffer never blocks, drop policy | `telemetry.py:7-10,146-204` |
| Consumer flush conditions / backoff / caps | `consumer.py:40-47,201-269,273-337` |
| Batch transaction | `database.py:451+` (`batch_insert`) |
| Thread-local connections / autocommit / busy timeout / FK | `database.py:252-281` |
| Lock retry (reads) | `database.py:358-377` |
| WAL-off single-file rationale | `database.py:20-22` |
| Joiner idempotency + first-outcome | `database.py:640-704` |
| Calibration single-table read + source filter | `calibration.py:262-274`, `_INDEPENDENT_OUTCOME_SOURCES` |
| One-outcome-per-decision | `outcomes.py:478-485$` UNIQUE linkage |
| Deterministic exploration | `gatekeeper.py:231-245`; design note `gatekeeper.py:18-21` |
| Fail-closed branches + catch-all | `gatekeeper.py:193-214`, `__init__.py:225-233` |
| Factor hash / uuid7 fallback chain | `utils.py:86-160` |
| Artifact generation + latest + rollback | `policy.py:310-361,382-427` |
| Prefix-risk threshold + Wilson + floor | `calibration.py:143-204,215-235` |
| Drift divergence | `calibration.py:296-345`; `DRIFT_DIVERGENCE_THRESHOLD` |
| Numpy/yaml isolation + lazy cycle break | import lines per module; `gatekeeper.py:323` |
| Packaging / typing / tooling | `pyproject.toml`, `py.typed`, `Makefile`, no `.github` |