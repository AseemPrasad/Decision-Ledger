"""Incident Webhook Dispatcher for Decision Ledger."""

from __future__ import annotations

import json
import logging
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class WebhookFormat(Enum):
    GENERIC_JSON = "generic"
    SLACK = "slack"
    PAGERDUTY = "pagerduty"


@dataclass
class WebhookTarget:
    url: str
    format: WebhookFormat = WebhookFormat.GENERIC_JSON
    secret_header: Optional[str] = None
    timeout_seconds: float = 5.0


class WebhookNotifier:
    """Dispatches diagnostic incident notifications via HTTP POST webhooks."""

    def __init__(
        self,
        targets: Optional[List[WebhookTarget]] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.targets = targets or []
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def add_target(self, target: WebhookTarget) -> None:
        self.targets.append(target)

    def dispatch_alert(
        self,
        event_type: str,
        context_hash: str,
        observed_risk: float,
        target_alpha: float,
        recalibration_status: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, bool]:
        """Send alert notification payload to all registered webhook targets."""
        results = {}
        for target in self.targets:
            payload = self._format_payload(
                target.format,
                event_type,
                context_hash,
                observed_risk,
                target_alpha,
                recalibration_status,
                details or {},
            )
            success = self._send_with_retry(target, payload)
            results[target.url] = success
        return results

    def _format_payload(
        self,
        fmt: WebhookFormat,
        event_type: str,
        context_hash: str,
        observed_risk: float,
        target_alpha: float,
        recalibration_status: str,
        details: Dict[str, Any],
    ) -> Dict[str, Any]:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())

        if fmt == WebhookFormat.SLACK:
            return {
                "text": f"🚨 *Decision Ledger Alert: {event_type}*",
                "attachments": [
                    {
                        "color": "#FF0000" if observed_risk > target_alpha else "#36a64f",
                        "fields": [
                            {"title": "Context Hash", "value": context_hash, "short": True},
                            {"title": "Recalibration Status", "value": recalibration_status, "short": True},
                            {"title": "Observed Risk", "value": f"{observed_risk:.4f}", "short": True},
                            {"title": "Target Alpha Bound", "value": f"{target_alpha:.4f}", "short": True},
                            {"title": "Timestamp", "value": timestamp, "short": False},
                        ],
                    }
                ],
            }

        elif fmt == WebhookFormat.PAGERDUTY:
            return {
                "event_action": "trigger",
                "payload": {
                    "summary": f"Decision Ledger SLA Breach: {event_type} on context {context_hash[:8]}",
                    "source": "decision-ledger-autorecalibrator",
                    "severity": "error" if observed_risk > target_alpha else "warning",
                    "timestamp": timestamp,
                    "custom_details": {
                        "context_hash": context_hash,
                        "observed_risk": observed_risk,
                        "target_alpha": target_alpha,
                        "recalibration_status": recalibration_status,
                        **details,
                    },
                },
            }

        else:  # GENERIC_JSON
            return {
                "event": event_type,
                "timestamp": timestamp,
                "context_hash": context_hash,
                "metrics": {
                    "observed_risk": observed_risk,
                    "target_alpha": target_alpha,
                    "drift_delta": observed_risk - target_alpha,
                },
                "recalibration_status": recalibration_status,
                "details": details,
            }

    def _send_with_retry(self, target: WebhookTarget, payload: Dict[str, Any]) -> bool:
        data = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if target.secret_header:
            headers["Authorization"] = target.secret_header

        for attempt in range(1, self.max_retries + 1):
            try:
                req = urllib.request.Request(target.url, data=data, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=target.timeout_seconds) as resp:
                    if 200 <= resp.status < 300:
                        logger.info("Successfully dispatched webhook to %s", target.url)
                        return True
            except Exception as err:
                logger.warning(
                    "Webhook POST attempt %d/%d to %s failed: %s",
                    attempt,
                    self.max_retries,
                    target.url,
                    err,
                )
                if attempt < self.max_retries:
                    time.sleep(self.backoff_factor * (2 ** (attempt - 1)))
        return False
