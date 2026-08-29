"""Tests for the async consumers (decision_ledger.consumer)."""

import logging
import time

import pytest

from decision_ledger import Database, DatabaseError, JsonlExport, RingBuffer
from decision_ledger.consumer import BatchConsumer
from decision_ledger.telemetry import DecisionRecord

CONTEXT = b"\xab" * 16


def make_record(decision_id: str) -> DecisionRecord:
    return DecisionRecord(
        decision_id=decision_id,
        timestamp_ns=i_clock(),
        context_hash=CONTEXT,
        decision_type="route",
        model_confidence=0.9,
        non_conformity=0.1,
        action_taken="DELEGATE",
        latency_us=5,
    )


def i_clock():
    """Monotonic-ish unique nanosecond timestamps for test records."""
    return time.time_ns() // 2


def push_records(buffer: RingBuffer, count: int, prefix: str = "d") -> None:
    for index in range(count):
        buffer.push(make_record(f"{prefix}-{index}"))


def wait_until(predicate, timeout: float = 5.0, step: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


@pytest.fixture
def ledger_db(tmp_path):
    database = Database(str(tmp_path / "ledger.db"))
    yield database
    database.close()


# --------------------------------------------------------------------------- #
# End-to-end drain
# --------------------------------------------------------------------------- #


def test_consumer_drains_to_sqlite(ledger_db):
    buffer = RingBuffer()
    push_records(buffer, 500)
    consumer = BatchConsumer(buffer, ledger_db, flush_interval=0.05)
    consumer.start()

    try:
        assert wait_until(
            lambda: consumer.get_metrics()["total_records_flushed"] == 500
        )
    finally:
        consumer.stop()

    rows = ledger_db.get_decisions()
    assert len(rows) == 500
    assert rows[0]["context_hash"] == CONTEXT  # stored as raw bytes (BLOB)
    assert {row["action_taken"] for row in rows} == {"DELEGATE"}
    assert buffer.size() == 0
    assert buffer.dropped_count == 0


def test_batch_size_triggers_flush(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, ledger_db, flush_interval=3600.0, batch_size=200)
    push_records(buffer, 200)
    consumer.start()

    try:
        assert wait_until(lambda: consumer.get_metrics()["total_flushes"] >= 1)
    finally:
        consumer.stop()

    assert len(ledger_db.get_decisions()) == 200
    assert consumer.get_metrics()["total_records_flushed"] == 200


def test_interval_triggers_flush(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, ledger_db, flush_interval=0.05, batch_size=10_000)
    push_records(buffer, 30)
    consumer.start()

    try:
        assert wait_until(lambda: consumer.get_metrics()["total_records_flushed"] == 30)
    finally:
        consumer.stop()

    assert len(ledger_db.get_decisions()) == 30


def test_stop_performs_final_flush(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(
        buffer, ledger_db, flush_interval=3600.0, batch_size=10_000
    )
    push_records(buffer, 7)
    consumer.start()

    assert wait_until(lambda: consumer.get_metrics()["total_records_processed"] == 7)
    consumer.stop()

    assert len(ledger_db.get_decisions()) == 7
    assert consumer.get_metrics()["total_records_flushed"] == 7


# --------------------------------------------------------------------------- #
# Failure handling
# --------------------------------------------------------------------------- #


class DownDatabase(Database):
    """A database whose batch_insert always fails."""

    def __init__(self) -> None:
        super().__init__(db_path=":memory:")

    def batch_insert(self, table, records):
        raise DatabaseError("database unavailable")


class FlakyDatabase(Database):
    """A database that fails the first ``fail_count`` batch_insert calls."""

    def __init__(self, real: Database, fail_count: int) -> None:
        super().__init__(db_path=str(real._db_path))
        self.real = real
        self.fail_count = fail_count
        self.calls = 0

    def batch_insert(self, table, records):
        self.calls += 1
        if self.calls <= self.fail_count:
            raise DatabaseError("transient outage")
        return self.real.batch_insert(table, records)


def test_flush_errors_retried_then_success(ledger_db):
    buffer = RingBuffer()
    flaky = FlakyDatabase(ledger_db, fail_count=2)
    consumer = BatchConsumer(buffer, flaky, flush_interval=3600.0, batch_size=10_000)
    push_records(buffer, 5)
    consumer.start()
    try:
        assert wait_until(
            lambda: consumer.get_metrics()["total_records_processed"] == 5
        )
    finally:
        consumer.stop()

    assert flaky.calls == 3  # initial attempt + 2 retries
    assert len(ledger_db.get_decisions()) == 5  # nothing lost


def test_database_down_buffers_and_drops_at_cap(ledger_db, caplog):
    caplog.set_level(logging.CRITICAL, logger="decision_ledger.consumer")
    buffer = RingBuffer(capacity=20_000)
    consumer = BatchConsumer(buffer, DownDatabase(), flush_interval=0.02)
    consumer._backlog_capacity = 1000
    consumer._backlog_alert_level = 500

    consumer.start()
    try:
        push_records(buffer, 2600)
        assert wait_until(
            lambda: consumer.get_metrics()["total_records_dropped"] >= 1600
        )
    finally:
        consumer.stop()

    metrics = consumer.get_metrics()
    assert metrics["total_records_dropped"] == 1600  # 2600 - 1000 cap
    assert metrics["backlog_records"] == 1000
    assert metrics["total_records_flushed"] == 0
    assert any("alert level" in record.message for record in caplog.records)


def test_consumer_survives_database_down(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, DownDatabase(), flush_interval=0.02)
    consumer.start()
    try:
        push_records(buffer, 50)
        assert wait_until(lambda: consumer.get_metrics()["backlog_records"] >= 1)
    finally:
        consumer.stop()  # must not raise


def test_stop_before_start_is_safe(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, ledger_db)
    before = time.monotonic()
    consumer.stop()  # never started -> returns immediately, no 10s join
    assert time.monotonic() - before < 1.0


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


def test_metrics_are_accurate(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, ledger_db, flush_interval=0.05)
    push_records(buffer, 250)
    consumer.start()
    try:
        assert wait_until(
            lambda: consumer.get_metrics()["total_records_flushed"] == 250
        )
    finally:
        consumer.stop()

    metrics = consumer.get_metrics()
    assert metrics["total_records_processed"] == 250
    assert metrics["total_records_flushed"] == 250
    assert metrics["total_records_dropped"] == 0
    assert metrics["total_flushes"] >= 1
    assert metrics["last_flush_time"] > 0.0
    assert metrics["avg_flush_time_ms"] >= 0.0
    assert metrics["backlog_records"] == 0


def test_metrics_reset_state_before_start(ledger_db):
    buffer = RingBuffer()
    consumer = BatchConsumer(buffer, ledger_db)
    m = consumer.get_metrics()
    for key in (
        "total_records_processed",
        "total_records_flushed",
        "total_records_dropped",
        "total_flushes",
    ):
        assert m[key] == 0
    assert m["last_flush_time"] == 0.0
    assert m["backlog_records"] == 0


# --------------------------------------------------------------------------- #
# JsonlExport
# --------------------------------------------------------------------------- #


def test_jsonl_export_flushes_all_pending_records(tmp_path):
    """Regression: a flush must write the accumulated batch, not just the
    latest poll (records arriving across multiple polls must not be lost)."""
    buffer = RingBuffer()
    exporter = JsonlExport(
        buffer,
        tmp_path / "out",
        flush_interval_ms=60_000,  # force accumulation across polls
        batch_size=2,
        poll_interval_ms=5,
    )
    buffer.push(make_record("d-1"))
    time.sleep(0.03)  # let one poll pick it up
    buffer.push(make_record("d-2"))

    exporter.start()
    try:
        assert wait_until(
            lambda: len(list((tmp_path / "out" / "decisions").rglob("*.jsonl"))) >= 1
        )
    finally:
        exporter.stop()

    files = list((tmp_path / "out" / "decisions").rglob("*.jsonl"))
    lines = sum(1 for file in files for _ in file.open(encoding="utf-8"))
    assert lines == 2
    assert buffer.size() == 0
