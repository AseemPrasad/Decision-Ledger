"""Batch consumer: drains the ring buffer to durable JSONL files.

The production design writes Parquet to S3; this MVP writes newline-delimited
JSON partitioned by date/hour under ``output_dir/decisions/``. The consumer
runs on a background thread so it never blocks the serving path.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from .telemetry import DecisionRecord, RingBuffer


@dataclass
class BatchConsumer:
    """Poll the ring buffer and flush batches to disk on a schedule."""

    ring_buffer: RingBuffer
    output_dir: str | Path
    flush_interval_ms: int = 100
    batch_size: int = 10_000
    poll_interval_ms: int = 1

    _thread: Optional[threading.Thread] = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="ledger-batch-consumer", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
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
                self._write(records)
                pending = []
                last_flush = now
            self._stop_event.wait(self.poll_interval_ms / 1000.0)
        if pending:
            self._write(pending)

    def _write(self, records: List[DecisionRecord]) -> None:
        if not records:
            return
        local = time.localtime()
        relative = (
            Path("decisions")
            / f"date={local.tm_year:04d}-{local.tm_mon:02d}-{local.tm_mday:02d}"
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
