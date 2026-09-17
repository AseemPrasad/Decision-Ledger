"""Unit & Integration tests for Distributed Control Plane Backing & Redis/PostgreSQL Fallbacks."""

from __future__ import annotations

import time
import pytest
from decision_ledger_server.auth import AuthManager
from decision_ledger_server.metering import MeteringEngine
from decision_ledger_server.rate_limiter import DistributedRateLimiter, RateLimiter
from decision_ledger_server.redis_bus import RedisPolicyBus


def test_distributed_rate_limiter_in_memory_fallback():
    """Verify DistributedRateLimiter falls back gracefully to in-memory mode when Redis is unconfigured."""
    limiter = DistributedRateLimiter(requests_per_minute=2, redis_url=None)

    # 1st request -> allowed
    assert limiter.is_allowed("tenant-test-1") is True
    # 2nd request -> allowed
    assert limiter.is_allowed("tenant-test-1") is True
    # 3rd request -> rate limited
    assert limiter.is_allowed("tenant-test-1") is False


def test_redis_policy_bus_standalone_fallback():
    """Verify RedisPolicyBus standalone fallback when Redis is absent."""
    bus = RedisPolicyBus(redis_url=None)
    assert bus.is_connected is False

    # Publish returns False gracefully without throwing error
    published = bus.publish_policy_update("tenant-test-2", "v2026.01")
    assert published is False


def test_auth_manager_standalone_operations():
    """Test multi-tenant AuthManager key generation and validation."""
    auth = AuthManager()
    key = auth.generate_key(org_id="org-acme", tenant_id="tenant-prod")

    assert key.startswith("dl_live_")
    context = auth.authenticate(key)
    assert context is not None
    assert context["org_id"] == "org-acme"
    assert context["tenant_id"] == "tenant-prod"

    # Invalid key returns None
    assert auth.authenticate("invalid_key_123") is None


def test_metering_engine_credit_deductions():
    """Test MeteringEngine balance tracking and credit deduction history."""
    metering = MeteringEngine(credits_per_1k_decisions=1.0)
    metering.set_balance("tenant-corp", 10.0)

    # Record 2,000 decisions -> cost = 2.0 credits
    success, remaining = metering.record_usage("tenant-corp", 2000)
    assert success is True
    assert remaining == 8.0

    history = metering.get_tenant_history("tenant-corp")
    assert len(history) == 1
    assert history[0].decisions_count == 2000
    assert history[0].credits_deducted == 2.0
