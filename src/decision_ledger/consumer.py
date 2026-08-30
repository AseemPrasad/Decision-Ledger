"""Async consumers that drain the ring buffer off the serving path.

Two sinks live here:

* **``BatchConsumer``** (the durable path) -- a daemon :class:`threading.Thread`
  that polls ``RingBuffer.pop_batch`` every 100ms and flushes accumulated
  records to SQLite via :meth:`Database.batch_insert
  <decision_ledger.database.Database.batch_insert>`. It is resilient: a failed
  flush is retried up to two extra times with a short backoff, and if the
  database stays unavailable the records are buffered *in memory* (100k cap,
  critical log above 50k) so an outage loses nothing the buffer can hold. The
  producer never blocks on disk.

* **``JsonlExport``** -- an ad-hoc export sink that partitions date/hour JSONL
  files under ``output_dir/decisions/``. Kept from the earlier MVP for
  debugging and test output; ``drain_now()`` gives a synchronous one-shot
  drain.

Both consumers only mutate the shared ``RingBuffer`` on the producer side
through its thread-safe ``pop_batch``; no lock is required on the buffer under
the CPython GIL.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from .database import Database, DatabaseError
from .telemetry import DecisionRecord, RingBuffer

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 0.1
FLUSH_RETRY_ATTEMPTS = 3  # initial attempt + up to 2 retries
RETRY_DELAY_S = 0.05
BACKLOG_CAPACITY = 100_000
BACKLOG_ALERT_LEVEL = 50_000
FAILURE_BACKOFF_S = 1.0
FLUSH_LOG_EVERY = 10_000
METRICS_LOG_INTERVAL_S = 60.0


class BatchConsumer(threading.Thread):
    """Background thread draining the ring buffer to SQLite.

    Each poll drains up to 1000 records from the ring buffer into an
    in-memory batch. The batch is flushed to the ``decisions`` table when it
    reaches ``batch_size`` records or when ``flush_interval`` seconds have
    passed since the last successful flush. ``batch_insert`` writes the whole
    batch in one transaction, so a poll-sized drain is several orders of
    magnitude cheaper than individual-row inserts.

    Failure handling (never crash, never silently lose what we hold):

    * a failed flush is retried up to ``FLUSH_RETRY_ATTEMPTS`` times
      (initial attempt + 2 retries) with a short backoff;
    * if it still fails the records stay in the in-memory batch and the next
      flush is gated by ``FAILURE_BACKOFF_S`` so a down database is not
      hammered every poll;
    * the in-memory batch caps at ``BACKLOG_CAPACITY`` records -- the oldest
      are dropped and counted once the cap is hit, and a critical alert is
      logged as soon as the backlog exceeds ``BACKLOG_ALERT_LEVEL``;
    * the alert flag resets once a flush succeeds and the backlog drains
      below the alert level.

    Metrics are visible via :meth:`get_metrics` for monitoring.

    Example:
        >>> import time
        >>> from decision_ledger import BatchConsumer, Database, RingBuffer
        >>> db = Database("ledger.db")
        >>> consumer = BatchConsumer(buffer, db, flush_interval=5.0)
        >>> consumer.start()
        >>> # ... keep calling gatekeeper.evaluate(); it never blocks on disk
        >>> consumer.stop()       # final flush + join
        >>> db.close()
    """

    def __init__(
        self,
        ring_buffer: RingBuffer,
        database: Database,
        flush_interval: float = 5.0,
        batch_size: int = 5_000,
        daemon: bool = True,
    ) -> None:
        """Configure and register the consumer.

        The thread does not start until :meth:`start` is called.

        Args:
            ring_buffer: The ``RingBuffer`` to drain. One consumer owns the
                drain side; no other consumer should pop from it.
            database: The ``Database`` to flush ``decisions`` rows into.
            flush_interval: Maximum seconds allowed between flushes; a time
                window triggers a flush even if ``batch_size`` is not reached.
            batch_size: Records held in memory before a flush is forced.
            daemon: Run as a daemon thread so the process can exit without an
                explicit :meth:`stop`.
        """
        super().__init__(name="ledger-batch-consumer", daemon=daemon)
        self.ring_buffer = ring_buffer
        self.database = database
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self.running = True
        self._started_flag = False

        self._batch: Deque[DecisionRecord] = deque()
        self._lock = threading.Lock()
        self._backlog_capacity = BACKLOG_CAPACITY
        self._backlog_alert_level = BACKLOG_ALERT_LEVEL
        self._alert_raised = False

        self._total_processed = 0
        self._total_flushed = 0
        self._total_dropped = 0
        self._flush_count = 0
        self._last_flush_time = 0.0
        self._flush_ms_total = 0.0
        self._last_10k_report = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Start the background drain thread (inherited from ``Thread``).

        Raises:
            RuntimeError: If :meth:`start` is called more than once; create a
                fresh consumer instead.
        """
        super().start()
        self._started_flag = True

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the drain loop, flush what remains, and join the thread.

        Sets ``running = False`` (the loop exits after its current poll),
        joins the loop thread with a ``timeout``, then performs a final
        ``_flush_to_db()`` so records that arrived in the very last poll are
        not stranded. The final flush follows the same retry/buffer rules as
        the loop; on a down database the pending records stay in the consumer.

        Args:
            timeout: Seconds to wait for the loop thread to exit; the final
                flush still runs afterwards, on the calling thread.
        """
        if not self.running and not self._started_flag:
            return
        self.running = False
        if self._started_flag:
            self.join(timeout)
        still_alive = self._started_flag and self.is_alive()
        self._flush_to_db()
        with self._lock:
            flushed = self._total_flushed
            dropped = self._total_dropped
            backlog = len(self._batch)
        if still_alive:
            logger.warning("batch consumer still alive after %.0fs join timeout", timeout)
        logger.info(
            "batch consumer stopped: total_flushed=%d total_dropped=%d" " backlog=%d",
            flushed,
            dropped,
            backlog,
        )

    def drain_now(self) -> int:
        """Synchronously drain the ring buffer into SQLite (ops/tests).

        Pulls everything currently in the ring buffer into the in-memory batch
        and flushes it in one transaction. Safe to call while the loop thread
        is running: :meth:`~.telemetry.RingBuffer.pop_batch` is atomic per
        record, so a concurrent drain may split the buffer but never drops or
        duplicates a record.

        Returns:
            Number of records drained from the ring buffer.
        """
        pending = self.ring_buffer.pop_batch(max_records=10_000)
        if not pending:
            return 0
        with self._lock:
            self._batch.extend(pending)
        self._flush_to_db()
        return len(pending)

    # ------------------------------------------------------------------ #
    # The drain loop
    # ------------------------------------------------------------------ #

    def run(self) -> None:
        """Drain loop: pop, accumulate, flush, sleep --- repeat.

        Runs on the background thread once :meth:`start` is called. Exits as
        soon as ``running`` is ``False`` after poll/sleep cadence.
        """
        last_flush = time.monotonic()
        last_metrics_log = time.monotonic()
        next_attempt = 0.0
        while self.running:
            try:
                records = self.ring_buffer.pop_batch(max_records=1000)
                if records:
                    self._append_to_batch(records)

                now = time.monotonic()
                should_flush = (
                    len(self._batch) >= self.batch_size or (now - last_flush) >= self.flush_interval
                )
                if should_flush and now >= next_attempt:
                    if self._flush_to_db():
                        last_flush = now
                        next_attempt = 0.0
                    else:
                        next_attempt = now + FAILURE_BACKOFF_S

                if now - last_metrics_log >= METRICS_LOG_INTERVAL_S:
                    logger.info("batch consumer metrics: %s", self.get_metrics())
                    last_metrics_log = now
            except Exception as exc:  # never let one bad poll kill the loop
                logger.exception("batch consumer loop error: %s", exc)
            finally:
                time.sleep(POLL_INTERVAL_S)
        logger.info("batch consumer run loop exiting")

    # ------------------------------------------------------------------ #
    # Batching and flushing
    # ------------------------------------------------------------------ #

    def _append_to_batch(self, records: List[DecisionRecord]) -> None:
        """Stash popped records in the in-memory batch, enforcing the cap."""
        with self._lock:
            self._total_processed += len(records)
            self._batch.extend(records)
            self._apply_backlog_locked()

    def _apply_backlog_locked(self) -> None:
        """Drop-oldest + alert once the in-memory buffer saturates.

        Caller must hold ``self._lock``.
        """
        level = len(self._batch)
        if level > self._backlog_alert_level and not self._alert_raised:
            logger.critical(
                "in-memory backlog %d exceeds %d alert level; flushing is"
                " stalled (database down?)",
                level,
                self._backlog_alert_level,
            )
            self._alert_raised = True
        overflow = level - self._backlog_capacity
        if overflow > 0:
            for _ in range(overflow):
                self._batch.popleft()
                self._total_dropped += 1
            self._alert_raised = True
            logger.critical(
                "in-memory backlog hit %d cap; dropped %d oldest records",
                self._backlog_capacity,
                overflow,
            )

    def _flush_to_db(self) -> bool:
        """Flush the whole in-memory batch to SQLite; ``True`` if handled.

        A batch that is empty returns ``True`` (nothing to do). Otherwise it
        is inserted in one ``batch_insert`` transaction; on ``DatabaseError``
        the attempt is retried up to ``FLUSH_RETRY_ATTEMPTS`` total times with
        a short backoff. If all attempts fail the records are kept in memory
        and ``False`` is returned so the caller can back off.

        Returns:
            ``True`` if the batch was flushed or was already empty, ``False``
            if it is still buffered after retries.
        """
        with self._lock:
            if not self._batch:
                return True
            rows = [self._record_to_row(record) for record in self._batch]

        attempt = 0
        while True:
            started = time.perf_counter()
            try:
                inserted = self.database.batch_insert("decisions", rows)
            except DatabaseError as exc:
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                attempt += 1
                logger.error(
                    "SQLite flush failed (attempt %d/%d) after %.1fms: %s",
                    attempt,
                    FLUSH_RETRY_ATTEMPTS,
                    elapsed_ms,
                    exc,
                )
                if attempt < FLUSH_RETRY_ATTEMPTS:
                    time.sleep(RETRY_DELAY_S * attempt)
                    continue
                logger.error(
                    "SQLite flush failed after %d attempts; buffering %d" " records in memory",
                    FLUSH_RETRY_ATTEMPTS,
                    len(rows),
                )
                return False

            elapsed_ms = (time.perf_counter() - started) * 1000.0
            with self._lock:
                self._total_flushed += inserted
                self._flush_count += 1
                self._last_flush_time = time.monotonic()
                self._flush_ms_total += elapsed_ms
                for _ in range(len(rows)):
                    if self._batch:
                        self._batch.popleft()
                if len(self._batch) <= self._backlog_alert_level:
                    self._alert_raised = False
                if (
                    self._total_flushed // FLUSH_LOG_EVERY
                    > self._last_10k_report // FLUSH_LOG_EVERY
                ):
                    self._last_10k_report = self._total_flushed
                    logger.info(
                        "batch consumer has flushed %d decision records total",
                        self._total_flushed,
                    )
            logger.info("[Flushed %d decision records to SQLite]", inserted)
            return True

    @staticmethod
    def _record_to_row(record: DecisionRecord) -> Dict[str, Any]:
        """Shape a :class:`DecisionRecord` for the ``decisions`` table.

        Uses the raw 16-byte ``context_hash`` (``to_dict()`` would hex-encode
        it, but the schema column is a ``BLOB``).
        """
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

    # ------------------------------------------------------------------ #
    # Metrics
    # ------------------------------------------------------------------ #

    def get_metrics(self) -> Dict[str, Any]:
        """Snapshot of the consumer's counters for monitoring.

        Returns a dict with ``total_records_processed``,
        ``total_records_flushed``, ``total_records_dropped``,
        ``total_flushes``, ``last_flush_time`` (monotonic seconds),
        ``avg_flush_time_ms`` (successful flushes only) and
        ``backlog_records`` (currently buffered, not yet durable).
        """
        with self._lock:
            avg = self._flush_ms_total / self._flush_count if self._flush_count else 0.0
            return {
                "total_records_processed": self._total_processed,
                "total_records_flushed": self._total_flushed,
                "total_records_dropped": self._total_dropped,
                "total_flushes": self._flush_count,
                "last_flush_time": self._last_flush_time,
                "avg_flush_time_ms": round(avg, 3),
                "backlog_records": len(self._batch),
            }


@dataclass
class JsonlExport:
    """Poll the ring buffer and flush batches to durable JSONL files.

    Writes one JSON record per line, partitioned by date/hour under
    ``output_dir/decisions/date=YYYY-MM-DD/hour=HH/``. Intended for ad-hoc
    exports and debugging; the durable production sink is
    :class:`BatchConsumer` (SQLite).
    """

    ring_buffer: RingBuffer
    output_dir: str | Path
    flush_interval_ms: int = 100
    batch_size: int = 10_000
    poll_interval_ms: int = 1

    _thread: Optional[threading.Thread] = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Start the background exporter thread (idempotent)."""
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="ledger-jsonl-consumer", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal shutdown, flush anything pending, and join the thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout)
        self._thread = None

    def _run(self) -> None:
        pending: List[DecisionRecord] = []
        last_flush = time.monotonic()
        while not self._stop_event.is_set():
            records = self.ring_buffer.pop_batch(max_records=1024)
            pending.extend(records)
            now = time.monotonic()
            should_flush = (
                len(pending) >= self.batch_size
                or (now - last_flush) >= self.flush_interval_ms / 1000.0
            )
            if should_flush and pending:
                self._write(pending)
                pending = []
                last_flush = now
            self._stop_event.wait(self.poll_interval_ms / 1000.0)
        if pending:
            self._write(pending)

    def _write(self, records: List[DecisionRecord]) -> None:
        """Append ``records`` as newline-delimited JSON under a date/hour dir."""
        if not records:
            return
        local = time.localtime()
        relative = (
            Path("decisions") / f"date={local.tm_year:04d}-{local.tm_mon:02d}-{local.tm_mday:02d}"
        )
        relative = relative / f"hour={local.tm_hour:02d}"
        path = Path(self.output_dir) / relative / f"batch-{time.time_ns()}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record.to_dict()))
                handle.write("\n")

    def drain_now(self) -> int:
        """Synchronously drain whatever is in the buffer (tests/ops)."""
        pending = self.ring_buffer.pop_batch(max_records=10_000)
        self._write(pending)
        return len(pending)
