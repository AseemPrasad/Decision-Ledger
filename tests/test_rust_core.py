"""Unit & Parity tests for Rust PyO3 decision_ledger_core extension and Python fallbacks."""

from __future__ import annotations

import pytest
from decision_ledger.gatekeeper import Gatekeeper, CalibrationContext, GateAction
from decision_ledger.utils import make_context_hash

try:
    import decision_ledger_core as _rust_core
    _RUST_AVAILABLE = True
except ImportError:
    _rust_core = None
    _RUST_AVAILABLE = False


def test_python_gatekeeper_parity_and_fallback():
    """Verify standard Python Gatekeeper behavior is unchanged and 100% consistent."""
    ctx_hash = make_context_hash("qwen-7b", "routing")
    ctx = CalibrationContext(
        context_hash=ctx_hash,
        q_hat=0.10,
        min_sample_size=10,
        current_sample_size=20,
        is_active=True,
    )

    gk = Gatekeeper(policy={ctx_hash: ctx}, exploration_rate=0.0)

    # 1 - 0.95 = 0.05 <= 0.10 -> DELEGATE
    assert gk.evaluate(ctx_hash, 0.95, "route") == GateAction.DELEGATE

    # 1 - 0.85 = 0.15 > 0.10 -> ESCALATE
    assert gk.evaluate(ctx_hash, 0.85, "route") == GateAction.ESCALATE


@pytest.mark.skipif(not _RUST_AVAILABLE, reason="Rust decision_ledger_core extension not compiled")
def test_rust_gatekeeper_direct():
    """Test direct Rust PyGatekeeper evaluation when extension is compiled."""
    rust_gk = _rust_core.PyGatekeeper(0.0)
    ctx_hash = make_context_hash("qwen-7b", "routing")

    rust_gk.set_context(
        ctx_hash,
        q_hat=0.10,
        min_sample_size=10,
        current_sample_size=20,
        is_active=True,
    )

    # Action 0 = DELEGATE, 1 = ESCALATE
    action1 = rust_gk.evaluate(ctx_hash, 0.95)
    assert action1 == 0

    action2 = rust_gk.evaluate(ctx_hash, 0.85)
    assert action2 == 1
