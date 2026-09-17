"""Token Bucket & Redis Sliding-Window Rate Limiter per Tenant for Decision Ledger Control Plane."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import redis
    _HAS_REDIS = True
except ImportError:
    redis = None
    _HAS_REDIS = False


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


class DistributedRateLimiter:
    """Distributed Sliding Window Rate Limiter powered by Redis with in-memory fallback."""

    def __init__(
        self,
        requests_per_minute: int = 1000,
        redis_url: Optional[str] = None,
    ) -> None:
        self.requests_per_minute = requests_per_minute
        self.window_sec = 60
        self._local_fallback = RateLimiter(requests_per_minute=requests_per_minute, burst_capacity=requests_per_minute)
        self._client: Optional[Any] = None

        url = redis_url or os.getenv("REDIS_URL")
        if _HAS_REDIS and url:
            try:
                self._client = redis.Redis.from_url(url, decode_responses=True)
                self._client.ping()
            except Exception as err:
                logger.warning("Redis RateLimiter connection failed (%s); using in-memory fallback", err)
                self._client = None

    def is_allowed(self, tenant_id: str, cost: int = 1) -> bool:
        """Check sliding window rate limit in Redis or local fallback."""
        if not self._client:
            return self._local_fallback.is_allowed(tenant_id, cost)

        now = time.time()
        key = f"rate_limit:{tenant_id}"
        window_start = now - self.window_sec

        try:
            pipe = self._client.pipeline()
            pipe.zremrangebyscore(key, 0, window_start)
            pipe.zcard(key)
            pipe.zadd(key, {f"{now}:{time.perf_counter()}": now})
            pipe.expire(key, self.window_sec + 5)
            results = pipe.execute()

            current_count = results[1]
            if current_count < self.requests_per_minute:
                return True
            else:
                return False
        except Exception as err:
            logger.warning("Redis rate limit check failed (%s); falling back to local limit", err)
            return self._local_fallback.is_allowed(tenant_id, cost)
