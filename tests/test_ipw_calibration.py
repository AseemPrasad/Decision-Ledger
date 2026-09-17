"""Unit & Integration tests for Off-Policy IPW Counterfactual Importance Sampling Calibrator."""

from __future__ import annotations

import pytest
from decision_ledger.calibration import (
    CalibrationRecord,
    ConformalCalibrator,
    IPWConformalCalibrator,
)
from decision_ledger.utils import make_context_hash


def test_ipw_calibrator_unweighted_parity():
    """Verify IPWConformalCalibrator produces identical results to ConformalCalibrator when all p_i = 1.0."""
    ctx_hash = make_context_hash("qwen-7b", "routing")

    records = []
    for i in range(100):
        records.append(
            CalibrationRecord(
                context_hash=ctx_hash,
                non_conformity_score=i / 100.0,
                loss=0.0 if i < 95 else 1.0,
                is_independent=True,
                is_exploratory=False,
                propensity_score=1.0,
            )
        )

    ipw_calibrator = IPWConformalCalibrator(target_alpha=0.05, min_sample_size=10)
    std_calibrator = ConformalCalibrator(target_alpha=0.05, min_sample_size=10)

    ipw_res = ipw_calibrator.compute_threshold(records)
    std_res = std_calibrator.compute_threshold(records)

    assert ipw_res.sample_size == 100
    assert ipw_res.q_hat == std_res.q_hat


def test_ipw_calibrator_bias_correction():
    """Test that IPW Importance Weighting corrects selection bias in exploratory shadow samples."""
    ctx_hash = make_context_hash("qwen-7b", "routing")

    records = []
    # 50 standard production records with propensity 1.0
    for i in range(50):
        records.append(
            CalibrationRecord(
                context_hash=ctx_hash,
                non_conformity_score=i / 100.0,  # 0.00 -> 0.49
                loss=0.0,
                is_independent=True,
                is_exploratory=False,
                propensity_score=1.0,
            )
        )

    # 50 exploratory shadow records over-sampled at low propensity p_i = 0.02
    for i in range(50):
        records.append(
            CalibrationRecord(
                context_hash=ctx_hash,
                non_conformity_score=0.50 + (i / 100.0),  # 0.50 -> 0.99
                loss=0.0 if i < 40 else 1.0,
                is_independent=True,
                is_exploratory=True,
                propensity_score=0.02,
            )
        )

    ipw_calibrator = IPWConformalCalibrator(target_alpha=0.05, min_sample_size=10, max_weight_clip=100.0)
    res = ipw_calibrator.compute_threshold(records)

    assert res.sample_size == 100
    assert res.q_hat is not None
    assert res.q_hat >= 0.90
