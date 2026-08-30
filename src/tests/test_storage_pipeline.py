"""Storage and consumer pipeline integration tests.

Covers the durable path end to end: ring buffer ``->`` BatchConsumer ``->``
SQLite, batch insertion, outcome logging, decision-outcome joining, and
graceful shutdown.

Why the ``database`` fixture is a *file* rather than ``:memory:``:
``Database`` opens one connection per thread (``threading.local``), so an
in-memory database is per-thread by construction -- the consumer thread's
writes would never be visible to the test thread. A temp file is the shared
store; ``tmp_path`` removes it after each test (teardown also closes the
``Database`` and stops every started consumer).
"""

import gc
import time

import pytest

from decision_ledger import (
    BatchConsumer,
    Database,
    DecisionRecord,
    Joiner,
    OutcomeCollector,
    OutcomeSource,
    RingBuffer,
)
from decision_ledger.utils import now_ns

CONTEXT = b"\xab" * 16


def make_record(decision_id: str, timestamp_ns: int | None = None) -> DecisionRecord:
    return DecisionRecord(
        decision_id=decision_id,
        timestamp_ns=timestamp_ns if timestamp_ns is not None else now_ns(),
        context_hash=CONTEXT,
        decision_type="route",
        model_confidence=0.9,
        non_conformity=0.1,
        action_taken="DELEGATE",
        latency_us=5,
    )


def to_decision_row(record: DecisionRecord) -> dict:
    return {
        "decision_id": record.decision_id,
        "timestamp_ns": record.timestamp_ns,
        "context_hash": record.context_hash,
        "decision_type": record.decision_type,
        "model_confidence": record.model_confidence,
        "non_conformity": record.non_conformity,
        "action_taken": record.action_taken,
        "latency_us": record.latency_us,
    }


def push_records(buffer: RingBuffer, count: int, prefix: str = "d") -> None:
    for index in range(count):
        buffer.push(make_record(f"{prefix}-{index}"))


def stored_ids(database: Database) -> set[str]:
    return {row["decision_id"] for row in database.get_decisions()}


def wait_until(predicate, timeout: float = 10.0, step: float = 0.02) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture
def database(tmp_path):
    db_path = tmp_path / "pipeline.db"
    db = Database(str(db_path))
    yield db
    db.close()
    # The consumer thread's SQLite connection is freed only once the object is
    # garbage-collected (it cannot be closed cross-thread), so on Windows the
    # file handle lingers briefly. Reap it, then retry the unlink briefly.
    gc.collect()
    for _ in range(20):
        try:
            db_path.unlink(missing_ok=True)
            return
        except PermissionError:
            time.sleep(0.05)


@pytest.fixture
def ring_buffer():
    return RingBuffer(capacity=100_000)


@pytest.fixture
def consumer(ring_buffer, database):
    c = BatchConsumer(ring_buffer, database, flush_interval=0.05, batch_size=5_000)
    c.start()
    try:
        yield c
    finally:
        c.stop()  # stop consumer threads properly


@pytest.fixture
def sample_decisions(ring_buffer):
    records = [make_record(f"d-{index}") for index in range(100)]
    for record in records:
        ring_buffer.push(record)
    return records


# --------------------------------------------------------------------------- #
# Test 1: consumer flushes decisions to SQLite
# --------------------------------------------------------------------------- #


def test_consumer_flushes_decisions_to_sqlite(consumer, sample_decisions, database):
    # The consumer (flush_interval=0.05s) must drain all 100 records within the
    # 10s window; wait_until fails fast if a flush stalls.
    assert wait_until(lambda: consumer.get_metrics()["total_records_flushed"] == 100, timeout=10.0)

    rows = database.get_decisions()
    expected = {record.decision_id for record in sample_decisions}
    assert len(rows) == 100  # no duplicates
    assert stored_ids(database) == expected  # no data loss
    assert all(row["context_hash"] == CONTEXT for row in rows)  # raw bytes BLOB

    metrics = consumer.get_metrics()
    assert metrics["total_records_processed"] == 100
    assert metrics["total_records_flushed"] == 100
    assert metrics["total_records_dropped"] == 0
    assert consumer.ring_buffer.size() == 0
    assert consumer.ring_buffer.dropped_count == 0


# --------------------------------------------------------------------------- #
# Test 2: batch insertion is correct
# --------------------------------------------------------------------------- #


def test_batch_insertion_is_correct(database):
    records = [make_record(f"d-{index}") for index in range(5_000)]

    inserted = database.batch_insert("decisions", [to_decision_row(r) for r in records])
    assert inserted == 5_000

    rows = database.get_decisions()
    assert len(rows) == 5_000  # count matches

    by_id = {row["decision_id"]: row for row in rows}
    assert len(by_id) == 5_000  # no dupes or collisions
    for record in records:
        row = by_id[record.decision_id]
        assert row["context_hash"] == record.context_hash
        assert row["model_confidence"] == record.model_confidence
        assert row["non_conformity"] == record.non_conformity
        assert row["decision_type"] == record.decision_type
        assert row["action_taken"] == record.action_taken


# --------------------------------------------------------------------------- #
# Test 3: outcome logging and querying
# --------------------------------------------------------------------------- #


def test_outcome_logging_and_querying(database):
    decision = make_record("d-outcome-1")
    database.batch_insert("decisions", [to_decision_row(decision)])

    collector = OutcomeCollector(database)
    outcome_id = collector.log_outcome(
        decision.decision_id,
        outcome_value=0.85,
        outcome_source=OutcomeSource.TASK_METRIC,
    )

    outcomes = collector.get_outcomes_for_decision(decision.decision_id)
    assert len(outcomes) == 1
    assert outcomes[0]["outcome_id"] == outcome_id
    assert outcomes[0]["outcome_value"] == 0.85
    assert outcomes[0]["outcome_source"] == "task_metric"

    # Query by decision_id through the database layer too.
    assert database.get_outcomes(decision_id=decision.decision_id)[0]["outcome_value"] == 0.85


# --------------------------------------------------------------------------- #
# Test 4: decision-outcome joining
# --------------------------------------------------------------------------- #


def test_decision_outcome_joining(database):
    decisions = [make_record(f"d-{index}") for index in range(100)]
    database.batch_insert("decisions", [to_decision_row(d) for d in decisions])

    # 75 of 100 decisions receive an outcome; 25 stay unmatched.
    outcomes = [
        {
            "outcome_id": f"o-{index}",
            "decision_id": f"d-{index}",
            "timestamp_ns": 999_000,
            "outcome_value": 1.0,
            "outcome_source": "task_metric",
            "metadata": None,
        }
        for index in range(75)
    ]
    database.batch_insert("outcomes", outcomes)

    created = Joiner(database).join_decisions_and_outcomes()
    assert created == 100  # LEFT JOIN writes every decision, matched or not

    joined = database.get_joined_records()
    assert len(joined) == 100
    matched = database.get_joined_records(include_unmatched=False)
    assert len(matched) == 75  # only the 75 decisions with an outcome

    stats = database.get_join_statistics()
    assert stats["total_decisions"] == 100
    assert stats["total_outcomes"] == 75
    assert stats["joined_count"] == 75
    assert stats["match_rate"] == pytest.approx(0.75)  # 75% (75/100)


# --------------------------------------------------------------------------- #
# Test 5: concurrent consumer + producer
# --------------------------------------------------------------------------- #


def test_concurrent_consumer_and_producer(database, ring_buffer):
    consumer = BatchConsumer(ring_buffer, database, flush_interval=0.05)
    consumer.start()
    try:
        # Produce 10,000 records while the consumer thread drains concurrently.
        for index in range(10_000):
            ring_buffer.push(make_record(f"d-{index}"))

        assert wait_until(
            lambda: consumer.get_metrics()["total_records_flushed"] == 10_000,
            timeout=30.0,
        )
    finally:
        consumer.stop()  # must not raise / crash

    assert not consumer.is_alive()
    assert len(database.get_decisions()) == 10_000  # nothing raced or dropped
    assert stored_ids(database) == {f"d-{index}" for index in range(10_000)}

    metrics = consumer.get_metrics()
    assert metrics["total_records_processed"] == 10_000
    assert metrics["total_records_flushed"] == 10_000
    assert metrics["total_records_dropped"] == 0
    assert ring_buffer.size() == 0
    assert ring_buffer.dropped_count == 0


# --------------------------------------------------------------------------- #
# Test 6: consumer graceful shutdown
# --------------------------------------------------------------------------- #


def test_consumer_graceful_shutdown(database, ring_buffer):
    # Huge flush window/batch: nothing is flushed until stop() runs its final
    # flush, which must persist everything still buffered.
    consumer = BatchConsumer(ring_buffer, database, flush_interval=1_000_000.0, batch_size=10**9)
    consumer.start()
    try:
        for index in range(1_000):
            ring_buffer.push(make_record(f"d-{index}"))
        # Wait until every record is popped out of the buffer into the batch.
        assert wait_until(
            lambda: consumer.get_metrics()["total_records_processed"] == 1_000,
            timeout=10.0,
        )
    finally:
        consumer.stop()

    assert not consumer.is_alive()  # thread joined cleanly
    assert len(database.get_decisions()) == 1_000  # final flush drained all
    assert stored_ids(database) == {f"d-{index}" for index in range(1_000)}

    metrics = consumer.get_metrics()
    assert metrics["total_records_flushed"] == 1_000
    assert metrics["total_records_dropped"] == 0
    assert ring_buffer.size() == 0
