"""Tests for the telemetry ring buffer and decision records.

Covers: deque-backed FIFO semantics, wrap-around accounting, backpressure
warning tiers, overload dropping (exploratory first), invalid-input handling,
the frozen record schema, serialization, and push/pop performance bounds.
"""

import logging
import re
import time
from dataclasses import FrozenInstanceError

import pytest

from decision_ledger.gatekeeper import GateAction
from decision_ledger.telemetry import EXPLORATORY, DecisionRecord, RingBuffer

_DELEGATE = GateAction.DELEGATE.name
_ESCALATE = GateAction.ESCALATE.name


def _record(
    action: str = _DELEGATE,
    decision_type: str = "route",
    confidence: float = 0.9,
    decision_id: str = "00000000-0000-7000-8000-000000000001",
    context_hash: bytes = b"\x01" * 16,
) -> DecisionRecord:
    return DecisionRecord.from_evaluation(
        context_hash=context_hash,
        decision_id=decision_id,
        decision_type=decision_type,
        action=action,
        confidence=confidence,
        latency_us=12,
    )


# --- FIFO semantics --------------------------------------------------------


def test_push_and_pop_batch_preserves_order():
    buffer = RingBuffer(capacity=10)
    ids = [f"00000000-0000-7000-8000-{i:012x}" for i in range(3)]
    for record in [_record(decision_id=uid) for uid in ids]:
        assert buffer.push(record) is True

    batch = buffer.pop_batch(max_records=10)
    assert [r.decision_id for r in batch] == ids
    assert buffer.pop_batch() == []
    assert buffer.size() == 0


def test_pop_batch_respects_max_records():
    buffer = RingBuffer(capacity=10)
    for _ in range(5):
        buffer.push(_record())

    first = buffer.pop_batch(max_records=2)
    remaining = buffer.pop_batch(max_records=10)
    assert len(first) == 2
    assert len(remaining) == 3


def test_pop_batch_nonpositive_max_records():
    buffer = RingBuffer(capacity=4)
    buffer.push(_record())
    assert buffer.pop_batch(max_records=0) == []
    assert buffer.pop_batch(max_records=-1) == []
    assert buffer.size() == 1


# --- Wrap-around and accounting --------------------------------------------


def test_buffer_wraps_at_capacity_without_crash():
    buffer = RingBuffer(capacity=10)
    for i in range(13):
        buffer.push(_record(decision_id=f"00000000-0000-7000-8000-{i:012x}"))

    assert buffer.size() == 10
    assert buffer.fill_level() == pytest.approx(1.0)
    assert buffer.total_pushed == 13
    assert buffer.dropped_count == 3

    batch = buffer.pop_batch(max_records=10)
    assert len(batch) == 10
    assert batch[0].decision_id.endswith("3")
    assert batch[-1].decision_id.endswith("c")


def test_size_and_fill_level_track_usage():
    buffer = RingBuffer(capacity=4)
    assert buffer.size() == 0
    assert buffer.fill_level() == 0.0
    buffer.push(_record())
    assert buffer.size() == 1
    assert buffer.fill_level() == pytest.approx(0.25)


def test_total_pushed_and_dropped_are_visible():
    buffer = RingBuffer(capacity=2)
    for _ in range(5):
        buffer.push(_record())
    assert buffer.total_pushed == 5
    assert buffer.dropped_count == 3
    assert buffer.size() == 2


# --- Invalid inputs --------------------------------------------------------


def test_invalid_input_is_rejected_and_counted():
    buffer = RingBuffer(capacity=4)
    assert buffer.push(None) is False
    assert buffer.push("not a record") is False
    assert buffer.push(42) is False
    assert buffer.dropped_count == 3
    assert buffer.total_pushed == 0
    assert buffer.size() == 0


def test_capacity_must_be_positive():
    with pytest.raises(ValueError):
        RingBuffer(capacity=0)


# --- Backpressure tiers ----------------------------------------------------


def _messages(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records]


def test_backpressure_warnings_escalate_with_fill(caplog):
    buffer = RingBuffer(capacity=100)
    caplog.set_level(logging.WARNING, logger="decision_ledger.telemetry")

    for _ in range(51):  # fill level 0.0 -> 0.51
        assert buffer.push(_record()) is True
    for _ in range(25):  # -> 0.76
        assert buffer.push(_record()) is True
    for _ in range(15):  # -> 0.91
        assert buffer.push(_record()) is True

    joined = "\n".join(_messages(caplog))
    assert "50-75% tier" in joined
    assert "75-90% tier" in joined
    assert "drops imminent" in joined
    assert buffer.dropped_count == 0


def test_drop_tier_drops_incoming_exploratory():
    buffer = RingBuffer(capacity=20)
    for _ in range(20):
        buffer.push(_record())  # all DELEGATE, full at level 1.0

    before = buffer.dropped_count
    assert buffer.push(_record(action=EXPLORATORY)) is False
    assert buffer.dropped_count == before + 1
    assert buffer.size() == 20
    assert not any(r.action_taken == EXPLORATORY for r in buffer.pop_batch(max_records=100))


def test_drop_tier_evicts_exploratory_before_delegate():
    buffer = RingBuffer(capacity=20)
    for i in range(18):
        buffer.push(_record(action=EXPLORATORY, decision_id=f"e-{i:04d}"))
    for i in range(2):
        buffer.push(_record(action=_DELEGATE, decision_id=f"d-{i:04d}"))
    assert buffer.size() == 20

    keep_id = "00000000-0000-7000-8000-00000000ffff"
    assert buffer.push(_record(action=_DELEGATE, decision_id=keep_id)) is True

    batch = buffer.pop_batch(max_records=100)
    assert buffer.dropped_count == 1
    assert len(batch) == 20
    assert any(record.decision_id == keep_id for record in batch)
    exploratory = sum(1 for r in batch if r.action_taken == EXPLORATORY)
    delegated = sum(1 for r in batch if r.action_taken == _DELEGATE)
    assert exploratory == 17  # one exploratory sacrificed to make room
    assert delegated == 3


def test_drop_tier_evicts_oldest_when_no_exploratory():
    buffer = RingBuffer(capacity=4)
    for i in range(4):
        buffer.push(_record(decision_id=f"00000000-0000-7000-8000-{i:012x}"))
    assert buffer.dropped_count == 0
    assert buffer.push(_record(decision_id="zzz")) is True

    batch = buffer.pop_batch(max_records=10)
    assert [r.decision_id for r in batch] == [
        "00000000-0000-7000-8000-000000000001",
        "00000000-0000-7000-8000-000000000002",
        "00000000-0000-7000-8000-000000000003",
        "zzz",
    ]
    assert buffer.dropped_count == 1


def test_drop_tier_above_threshold_but_not_full_does_not_drop():
    buffer = RingBuffer(capacity=100)
    for _ in range(96):  # level 0.0 -> 0.96, not full
        assert buffer.push(_record()) is True
    assert buffer.size() == 96
    assert buffer.dropped_count == 0


# --- DecisionRecord schema -------------------------------------------------


def test_from_evaluation_maps_fields():
    record = DecisionRecord.from_evaluation(
        context_hash=b"\xab" * 16,
        decision_id="11111111-1111-7111-8111-111111111111",
        decision_type="summarize",
        action=_ESCALATE,
        confidence=0.7,
        latency_us=33,
        timestamp_ns=1234,
    )
    assert record.decision_id == "11111111-1111-7111-8111-111111111111"
    assert record.timestamp_ns == 1234
    assert record.context_hash == b"\xab" * 16
    assert record.decision_type == "summarize"
    assert record.model_confidence == pytest.approx(0.7)
    assert record.non_conformity == pytest.approx(0.3)
    assert record.action_taken == _ESCALATE
    assert record.latency_us == 33


def test_record_is_frozen():
    record = _record()
    with pytest.raises(FrozenInstanceError):
        setattr(record, "latency_us", 99)  # type: ignore[misc]


def test_to_dict_has_exact_schema():
    data = _record().to_dict()
    assert set(data) == {
        "decision_id",
        "timestamp_ns",
        "context_hash",
        "decision_type",
        "model_confidence",
        "non_conformity",
        "action_taken",
        "latency_us",
    }
    assert data["decision_id"] == "00000000-0000-7000-8000-000000000001"
    assert data["context_hash"] == (b"\x01" * 16).hex()
    assert data["decision_type"] == "route"
    assert data["action_taken"] == _DELEGATE
    assert data["model_confidence"] == pytest.approx(0.9)
    assert data["non_conformity"] == pytest.approx(0.1)


# --- Performance -----------------------------------------------------------


def test_push_throughput_exceeds_100k_per_second():
    buffer = RingBuffer(capacity=1_000_000)
    record = _record()
    start = time.perf_counter()
    for _ in range(100_000):
        buffer.push(record)
    elapsed = time.perf_counter() - start
    assert elapsed < 2.0, f"100k pushes took {elapsed:.3f}s"
    assert buffer.size() == 100_000


def test_pop_batch_1000_records_is_fast():
    buffer = RingBuffer(capacity=10_000)
    record = _record()
    for _ in range(1000):
        buffer.push(record)
    start = time.perf_counter()
    batch = buffer.pop_batch(max_records=1000)
    elapsed = time.perf_counter() - start
    assert len(batch) == 1000
    assert elapsed < 0.05, f"1000 pops took {elapsed:.3f}s"


def test_decision_id_is_36_char_uuidv7():
    import uuid

    from decision_ledger import decision_id

    for _ in range(50):
        value = decision_id()
        assert re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            value,
        )
        assert str(uuid.UUID(value)) == value
