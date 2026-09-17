"""Integration & Unit tests for Decision Ledger B2B SaaS Remote Client and Control Plane Server."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
import pytest

from decision_ledger.gatekeeper import Gatekeeper
from decision_ledger.telemetry import DecisionRecord, RingBuffer
from decision_ledger.client import RemoteSyncConsumer, RemotePolicyWatcher
from decision_ledger_server.app import create_server, ControlPlaneRequestHandler


@pytest.fixture(scope="module")
def saas_server():
    """Start local test SaaS control plane server on port 9876."""
    server = create_server("127.0.0.1", 9876)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Pre-register a test API key for org="org-test", tenant="tenant-alpha"
    api_key = ControlPlaneRequestHandler.auth_manager.generate_key("org-test", "tenant-alpha")
    ControlPlaneRequestHandler.metering_engine.set_balance("tenant-alpha", 500.0)

    # Set up active policy on server
    ControlPlaneRequestHandler.policy_service.set_policy(
        "tenant-alpha",
        {
            "policy_version": "v2026.01",
            "target_alpha": 0.05,
            "contexts": {},
        },
    )

    yield {
        "url": "http://127.0.0.1:9876",
        "api_key": api_key,
        "tenant_id": "tenant-alpha",
    }

    server.shutdown()
    server.server_close()


def test_auth_and_metering(saas_server):
    """Test auth failure, valid ingestion, and credit deduction."""
    url = saas_server["url"]
    api_key = saas_server["api_key"]
    tenant_id = saas_server["tenant_id"]

    # 1. Unauthorized request
    req = urllib.request.Request(f"{url}/api/v1/ingest", data=b"{}", headers={}, method="POST")
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req)
    assert exc_info.value.code == 401

    # 2. Valid ingestion
    rb = RingBuffer(capacity=100)
    rb.push(
        DecisionRecord(
            decision_id="01890a2f-1234-7000-8000-000000000001",
            context_hash="1f810209b58ae19f185af097ca9cf646",
            confidence=0.95,
            decision_type=0,
            gate_action=0,
            latency_ns=8000,
            created_at_ns=1000000000,
        )
    )

    consumer = RemoteSyncConsumer(
        ring_buffer=rb,
        endpoint_url=url,
        api_key=api_key,
        tenant_id=tenant_id,
        auto_start=False,
    )
    pushed = consumer.drain_now()
    assert pushed == 1
    assert consumer.total_pushed == 1

    # Check remaining credit balance
    balance = ControlPlaneRequestHandler.metering_engine.get_balance(tenant_id)
    assert balance < 500.0


def test_remote_policy_watcher_hot_reload(saas_server):
    """Test RemotePolicyWatcher fetching and hot-reloading policy into Gatekeeper."""
    url = saas_server["url"]
    api_key = saas_server["api_key"]
    tenant_id = saas_server["tenant_id"]

    gk = Gatekeeper()
    watcher = RemotePolicyWatcher(
        gatekeeper=gk,
        endpoint_url=url,
        api_key=api_key,
        tenant_id=tenant_id,
        auto_start=False,
    )

    updated = watcher.check_and_apply()
    assert updated is True
    assert gk._policy_version == "v2026.01"
