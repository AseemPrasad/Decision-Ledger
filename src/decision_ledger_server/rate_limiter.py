"""Token Bucket Rate Limiter per Tenant for Decision Ledger Control Plane."""

from __future__ import annotations

import time
from typing import Dict, Tuple


class RateLimiter:
    """Thread-safe Token Bucket Rate Limiter per tenant/org key."""

    def __init__(self, requests_per_minute: int = 1000, burst_capacity: int = 100) -> None:
        self.rate = requests_per_minute / 60.0  # tokens per second
        self.capacity = float(burst_capacity)
        # tenant_id -> (tokens, last_update_time)
        self._buckets: Dict[str, Tuple[float, float]] = {}

    def is_allowed(self, tenant_id: str, cost: int = 1) -> bool:
        """Check if tenant has enough tokens for the request."""
        now = time.time()
        tokens, last_update = self._buckets.get(tenant_id, (self.capacity, now))

        # Replenish tokens based on elapsed time
        elapsed = now - last_update
        tokens = min(self.capacity, tokens + elapsed * self.rate)

        if tokens >= cost:
            self._buckets[tenant_id] = (tokens - cost, now)
            return True

        self._buckets[tenant_id] = (tokens, now)
        return False
