"""Unit & Integration tests for OpenTelemetry (OTel) ObservabilityManager and Gatekeeper instrumentation."""

from __future__ import annotations

import pytest
from decision_ledger.gatekeeper import CalibrationContext, GateAction, Gatekeeper
from decision_ledger.observability import ObservabilityManager
from decision_ledger.utils import make_context_hash


def test_observability_manager_noop_fallback():
    """Test ObservabilityManager when instantiated cleanly."""
    obs = ObservabilityManager(meter_name="test_meter")

    # Recording evaluations should operate without errors
    ctx = make_context_hash("qwen-7b", "routing")
    obs.record_evaluation(
        action="DELEGATE",
        decision_type="route",
        context_hash=ctx,
        latency_us=12,
    )
    obs.update_q_hat(ctx, 0.08)


def test_gatekeeper_with_observability_integration():
    """Test Gatekeeper evaluating with ObservabilityManager attached."""
    obs = ObservabilityManager(meter_name="gatekeeper_test_meter")
    ctx_hash = make_context_hash("qwen-7b", "routing")
    ctx = CalibrationContext(
        context_hash=ctx_hash,
        q_hat=0.10,
        min_sample_size=10,
        current_sample_size=20,
        is_active=True,
    )

    gk = Gatekeeper(
        policy={ctx_hash: ctx},
        exploration_rate=0.0,
        observability=obs,
    )

    # DELEGATE evaluation
    action1 = gk.evaluate(ctx_hash, 0.95, "route")
    assert action1 == GateAction.DELEGATE

    # ESCALATE evaluation
    action2 = gk.evaluate(ctx_hash, 0.85, "route")
    assert action2 == GateAction.ESCALATE
