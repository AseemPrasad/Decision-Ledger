"""Remote sync consumer for exporting decision telemetry to a central SaaS control plane.

Designed to operate asynchronously without blocking local serving threads.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from decision_ledger.telemetry import DecisionRecord, RingBuffer

logger = logging.getLogger(__name__)


class RemoteSyncConsumer:
    """Asynchronously drains the RingBuffer and pushes decision batches to a remote SaaS server.

    Does not raise exceptions on network errors; instead, buffers locally and retries.
    """

    def __init__(
        self,
        ring_buffer: RingBuffer,
        endpoint_url: str,
        api_key: str,
        tenant_id: str = "default",
        batch_size: int = 100,
        flush_interval_sec: float = 1.0,
        timeout_sec: float = 5.0,
        auto_start: bool = True,
    ) -> None:
        self.ring_buffer = ring_buffer
        self.endpoint_url = endpoint_url.rstrip("/") + "/api/v1/ingest"
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.batch_size = max(1, batch_size)
        self.flush_interval_sec = max(0.1, flush_interval_sec)
        self.timeout_sec = timeout_sec

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._total_pushed = 0
        self._failed_attempts = 0

        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the background worker thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="decision-ledger-remote-sync",
                daemon=True,
            )
            self._thread.start()
            logger.info("RemoteSyncConsumer started -> %s", self.endpoint_url)

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the worker to stop and wait for completion."""
        self._stop_event.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
            logger.info("RemoteSyncConsumer stopped.")

    def drain_now(self) -> int:
        """Synchronously drain all current records from the ring buffer and push to SaaS."""
        records = self.ring_buffer.pop_batch(max(1000, self.batch_size))
        if not records:
            return 0

        sent_count = self._send_batch(records)
        return sent_count

    def _run_loop(self) -> None:
        """Background loop flushing batches periodically or when threshold is reached."""
        while not self._stop_event.is_set():
            try:
                if len(self.ring_buffer) >= self.batch_size:
                    self.drain_now()
                else:
                    self._stop_event.wait(self.flush_interval_sec)
                    if len(self.ring_buffer) > 0:
                        self.drain_now()
            except Exception as exc:
                logger.warning("Unexpected exception in RemoteSyncConsumer loop: %s", exc)
                time.sleep(1.0)

    def _send_batch(self, records: List[DecisionRecord]) -> int:
        """HTTP POST batch payload to remote control plane."""
        payload = {
            "tenant_id": self.tenant_id,
            "count": len(records),
            "records": [r.to_dict() for r in records],
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self.api_key,
                "X-Tenant-ID": self.tenant_id,
                "User-Agent": "DecisionLedger-RemoteSync/1.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                if 200 <= resp.status < 300:
                    self._total_pushed += len(records)
                    self._failed_attempts = 0
                    return len(records)
                else:
                    logger.warning("Remote SaaS returned status %d", resp.status)
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            self._failed_attempts += 1
            logger.debug("Failed to push batch to %s: %s", self.endpoint_url, err)
        return 0

    @property
    def total_pushed(self) -> int:
        """Total records successfully exported to remote SaaS."""
        return self._total_pushed
