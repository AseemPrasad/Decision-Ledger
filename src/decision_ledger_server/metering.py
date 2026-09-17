"""Usage Credit & Metering Ledger for Decision Ledger Control Plane."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class UsageRecord:
    tenant_id: str
    decisions_count: int
    credits_deducted: float
    timestamp: float


class MeteringEngine:
    """Tracks consumption credits per tenant and logs usage audit records."""

    def __init__(self, credits_per_1k_decisions: float = 1.0) -> None:
        self.credits_per_1k_decisions = credits_per_1k_decisions
        # tenant_id -> credit_balance
        self._balances: Dict[str, float] = {}
        self._history: List[UsageRecord] = []
        self._lock = threading.Lock()

    def set_balance(self, tenant_id: str, credits: float) -> None:
        """Set credit balance for a tenant."""
        with self._lock:
            self._balances[tenant_id] = credits

    def get_balance(self, tenant_id: str) -> float:
        """Get remaining credit balance for a tenant."""
        with self._lock:
            return self._balances.get(tenant_id, 0.0)

    def record_usage(self, tenant_id: str, decisions_count: int) -> Tuple[bool, float]:
        """Deduct credits based on decision volume. Returns (success, remaining_balance)."""
        cost = (decisions_count / 1000.0) * self.credits_per_1k_decisions
        with self._lock:
            current = self._balances.get(tenant_id, 1000.0)  # Default 1000 trial credits
            if current < cost:
                return False, current

            new_balance = current - cost
            self._balances[tenant_id] = new_balance
            self._history.append(
                UsageRecord(
                    tenant_id=tenant_id,
                    decisions_count=decisions_count,
                    credits_deducted=cost,
                    timestamp=time.time(),
                )
            )
            return True, new_balance

    def get_tenant_history(self, tenant_id: str) -> List[UsageRecord]:
        """Retrieve historical usage records for a tenant."""
        with self._lock:
            return [r for r in self._history if r.tenant_id == tenant_id]
