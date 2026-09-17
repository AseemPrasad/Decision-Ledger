"""Unit & Integration tests for Joint Multi-Objective Conformal Risk Control Engine."""

from __future__ import annotations

import pytest
from decision_ledger.calibration import (
    MultiObjectiveCalibrationRecord,
    MultiObjectiveConformalCalibrator,
)
from decision_ledger.gatekeeper import CalibrationContext, GateAction, Gatekeeper
from decision_ledger.utils import make_context_hash


def test_multi_objective_conformal_calibrator_quantiles():
    """Test MultiObjectiveConformalCalibrator computing Bonferroni-adjusted quantile bounds."""
    ctx_hash = make_context_hash("qwen-7b", "routing")

    # Target alphas: error=0.10, latency=0.10 (k=2 -> alpha_star = 0.05 per metric)
    calibrator = MultiObjectiveConformalCalibrator(
        target_alphas={"error": 0.10, "latency_us": 0.10},
        min_sample_size=10,
    )

    # 100 synthetic records
    records = []
    for i in range(100):
        records.append(
            MultiObjectiveCalibrationRecord(
                context_hash=ctx_hash,
                non_conformity_scores={"error": i / 100.0, "latency_us": i * 100.0},
                losses={"error": 0.0 if i < 90 else 1.0, "latency_us": 0.0 if i < 90 else 1.0},
                is_independent=True,
                is_exploratory=False,
            )
        )

    result = calibrator.compute_thresholds(records)
    assert result.sample_size == 100
    assert "error" in result.q_hat_vector
    assert "latency_us" in result.q_hat_vector

    # Bonferroni alpha_star = 0.05 -> rank = ceil(101 * 0.95) = 96th index -> ~0.95
    assert result.q_hat_vector["error"] >= 0.90
    assert result.q_hat_vector["latency_us"] >= 9000.0


def test_gatekeeper_multi_objective_joint_evaluation():
    """Test Gatekeeper.evaluate_multi_objective checking all vector bounds."""
    ctx_hash = make_context_hash("qwen-7b", "routing")

    ctx = CalibrationContext(
        context_hash=ctx_hash,
        q_hat_vector={"error": 0.10, "latency_us": 10000.0, "cost": 0.005},
        min_sample_size=10,
        current_sample_size=20,
        is_active=True,
    )

    gk = Gatekeeper(policy={ctx_hash: ctx}, exploration_rate=0.0)

    # 1. All metrics pass -> DELEGATE
    action_pass = gk.evaluate_multi_objective(
        ctx_hash,
        model_non_conformities={"error": 0.05, "latency_us": 8000.0, "cost": 0.002},
    )
    assert action_pass == GateAction.DELEGATE

    # 2. Single metric breach (latency exceeds SLA) -> ESCALATE
    action_fail_latency = gk.evaluate_multi_objective(
        ctx_hash,
        model_non_conformities={"error": 0.05, "latency_us": 15000.0, "cost": 0.002},
    )
    assert action_fail_latency == GateAction.ESCALATE

    # 3. Cost breach -> ESCALATE
    action_fail_cost = gk.evaluate_multi_objective(
        ctx_hash,
        model_non_conformities={"error": 0.05, "latency_us": 8000.0, "cost": 0.010},
    )
    assert action_fail_cost == GateAction.ESCALATE


def test_multi_objective_fallback_to_single_qhat():
    """Test evaluate_multi_objective fallback when only scalar q_hat is present."""
    ctx_hash = make_context_hash("qwen-7b", "routing")

    ctx = CalibrationContext(
        context_hash=ctx_hash,
        q_hat=0.10,
        min_sample_size=10,
        current_sample_size=20,
        is_active=True,
    )

    gk = Gatekeeper(policy={ctx_hash: ctx}, exploration_rate=0.0)

    action = gk.evaluate_multi_objective(
        ctx_hash,
        model_non_conformities={"error": 0.05},
    )
    assert action == GateAction.DELEGATE
