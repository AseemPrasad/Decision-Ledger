"""Remote policy watcher for dynamic policy hot-reloading from SaaS control plane."""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Optional

from decision_ledger.gatekeeper import Gatekeeper

logger = logging.getLogger(__name__)


class RemotePolicyWatcher:
    """Polls a remote SaaS endpoint for policy updates and hot-swaps them into a Gatekeeper."""

    def __init__(
        self,
        gatekeeper: Gatekeeper,
        endpoint_url: str,
        api_key: str,
        tenant_id: str = "default",
        poll_interval_sec: float = 10.0,
        timeout_sec: float = 5.0,
        auto_start: bool = True,
    ) -> None:
        self.gatekeeper = gatekeeper
        self.endpoint_url = endpoint_url.rstrip("/") + "/api/v1/policy/active"
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.poll_interval_sec = max(1.0, poll_interval_sec)
        self.timeout_sec = timeout_sec

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._current_version: Optional[str] = None
        self._lock = threading.Lock()

        if auto_start:
            self.start()

    def start(self) -> None:
        """Start the policy watcher background thread."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="decision-ledger-policy-watcher",
                daemon=True,
            )
            self._thread.start()
            logger.info("RemotePolicyWatcher started -> %s", self.endpoint_url)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the background watcher thread."""
        self._stop_event.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    def check_and_apply(self) -> bool:
        """Fetch active policy from remote server and apply if updated."""
        req = urllib.request.Request(
            self.endpoint_url,
            headers={
                "Accept": "application/json",
                "X-API-Key": self.api_key,
                "X-Tenant-ID": self.tenant_id,
            },
            method="GET",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                if resp.status == 200:
                    payload = json.loads(resp.read().decode("utf-8"))
                    version = payload.get("policy_version")
                    if version and version != self._current_version:
                        self.gatekeeper.reload_policy(payload)
                        self._current_version = version
                        logger.info("Hot-reloaded policy version %s from remote SaaS", version)
                        return True
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as err:
            logger.debug("Failed to poll policy from %s: %s", self.endpoint_url, err)
        return False

    def _run_loop(self) -> None:
        """Polling loop."""
        while not self._stop_event.is_set():
            self.check_and_apply()
            self._stop_event.wait(self.poll_interval_sec)
