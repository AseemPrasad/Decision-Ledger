# Telemetry API Reference: `RingBuffer` + `DecisionRecord`

The audit trail. Every gatekeeper evaluation becomes a `DecisionRecord` in a
bounded, non-blocking `RingBuffer`; a consumer drains it off the serving path.

```python
from decision_ledger import RingBuffer, DecisionRecord
```

## `class RingBuffer(capacity=100_000)`

Bounded single-producer buffer built on `collections.deque(maxlen=capacity)`.

| Parameter | Type | Default | Meaning |
| --- | --- | --- | --- |
| `capacity` | `int` | `100_000` | Maximum records held. `capacity < 1` raises `ValueError`. |

`RingBuffer` has **no lock**: `deque` append/popleft operations are atomic
under the CPython GIL, so one producer may `push` while a consumer
`pop_batch`es. The contract assumes a single producer (the gatekeeper, itself
serialized by its `RLock`).

### `push(record: DecisionRecord) -> bool`

Append a record without blocking or raising.

- Returns `True` when the record is stored.
- Returns `False` (and increments `dropped_count`) when:
  - `record` is not a `DecisionRecord` (rejected input), or
  - the buffer is **full and ≥95% filled** and `record` is exploratory
    (`action_taken == "EXPLORE_SHADOW"`), or
  - the buffer is **full and ≥95% filled** and room is made by evicting one
    existing record (exploratory records first, then FIFO oldest) — that
    eviction is counted as a drop too.
- `total_pushed` counts every valid record submitted (dropped or not).

Because `deque(maxlen=...)` never blocks, there is no backpressure on the
producer; the backpressure signal is **log warnings at fill tiers** (below)
plus the explicit `dropped_count` accounting.

### `pop_batch(max_records=1000) -> list[DecisionRecord]`

FIFO drain of up to `max_records` records in one call.

- `max_records <= 0` → returns `[]`.
- Returns only what is currently available — never blocks waiting for more.
- O(1) per record; a full 1000-record drain is ~53–72 µs of pure move work.

```python
batch = buffer.pop_batch()              # up to 1000
batch = buffer.pop_batch(max_records=5000)
```

### Occupancy & accounting

| Member | Type | Returns |
| --- | --- | --- |
| `size()` | `int` | Records currently held. |
| `fill_level()` | `float` | `size() / capacity`, i.e. `0.0`–`1.0`. |
| `total_pushed` | `int` | Property; every valid record ever submitted. |
| `dropped_count` | `int` | Property; every record lost (rejected input, wrapped eviction, overload sacrifice). |

### Backpressure tiers

Warnings fire from `push` as fill crosses thresholds, throttled per buffer:

| Fill level | Log level | Cadence |
| --- | --- | --- |
| `< 50%` | silent | — |
| 50–75% | `WARNING` "50-75% tier" | once per minute |
| 75–90% | `WARNING` "75-90% tier" | every 10 s |
| 90–95% | `CRITICAL` "drops imminent" | every 5 s |
| ≥95% (and full) | `CRITICAL` "dropping records" | every 5 s; drop logic engages |

A buffer at capacity is drop-tolerant by design: escalation/delegation
records survive overload longest, exploratory shadow records are sacrificed
first, then the oldest FIFO record. Drops are always **counted**, never a
crash. Monitor `dropped_count`; alarm before tier 4.

### Usage example

```python
from decision_ledger import Gatekeeper, RingBuffer, context_hash, policy_from_results

buffer = RingBuffer(capacity=1 << 16)
gk = Gatekeeper(policy.contexts, exploration_rate=0.02, telemetry=buffer)

# ... serve requests (each evaluate() appends one DecisionRecord) ...

records = buffer.pop_batch()
print(buffer.size(), buffer.fill_level(), buffer.dropped_count)
for r in records:
    print(r.decision_id, r.action_taken, r.latency_us)
```

## `class DecisionRecord`

Frozen `slots=True` dataclass: one immutable record per evaluation.

| Field | Type | Meaning |
| --- | --- | --- |
| `decision_id` | `str` | Time-ordered UUIDv7, 36-char lowercase canonical form. Sorts by creation time. |
| `timestamp_ns` | `int` | Wall-clock creation time, nanoseconds (`utils.now_ns`). |
| `context_hash` | `bytes` | 16-byte context reference the decision was made under. |
| `decision_type` | `str` | `"route"` / `"judge"` / `"speculate"` / `"mutate"` / `"summarize"` / `"abstain"`. |
| `model_confidence` | `float` | Clamped confidence actually used for the decision. |
| `non_conformity` | `float` | `1.0 - model_confidence` (the calibration score). |
| `action_taken` | `str` | `"DELEGATE"` / `"ESCALATE"` / `"EXPLORE_SHADOW"`. |
| `latency_us` | `int` | Gatekeeper decision time, integer microseconds. |

### Factory

```python
DecisionRecord.from_evaluation(
    *,
    context_hash: bytes,
    decision_id: str,
    decision_type: str,
    action: str,
    confidence: float,
    latency_us: int,
    timestamp_ns: int | None = None,   # defaults to utils.now_ns()
) -> DecisionRecord
```

Used internally by `Gatekeeper.evaluate`; identical for tests and scripts.

### Serialization

```python
record.to_dict()  # dict with context_hash hex-encoded, JSONL-friendly
```

The `BatchConsumer` writes one `json.loads(to_dict())` line per record.

## Performance characteristics

| Operation | Cost | Notes |
| --- | --- | --- |
| `RingBuffer(capacity=n)` | O(1) | `deque(maxlen=n)` allocates once. |
| `push(record)` | O(1) | Amortized; no reallocation. |
| `push` with backpressure check | ~50 ns overhead | `_maybe_warn` is a couple of monotonic reads + comparisons; throttled writes. |
| `pop_batch(k)` | O(k) | `popleft` is O(1); returns a `k`-element list. |
| 1000-record drain | ~53–72 µs | Pure move work; records retained by the consumer. |
| + teardown (discard consumer) | +~110 µs | Deallocating 8-field frozen dataclasses + strings + bytes. A discard-style poller should budget ~150–170 µs total. |
| DecisionRecord construction | ~6 µs | One UUIDv7 + one slots dataclass + one deque append (this is the gap between no-telemetry and telemetry latency). |

Mitigations in place: `DecisionRecord` uses `slots=True`, and the gatekeeper
measures and records its own decision latency, so downstream analytics get the
true serving-path cost rather than a timestamp delta.

> Full methodology and scripts: [BENCHMARKS.md](BENCHMARKS.md).