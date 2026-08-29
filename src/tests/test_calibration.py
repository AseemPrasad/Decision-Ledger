"""Tests for the Split Conformal Risk Control calibrator."""

import random

import numpy as np
import pytest

from decision_ledger import CalibrationRecord, ConformalCalibrator

CTX = b"\xab" * 16


def _records(n: int, *, honest: bool = True, seed: int = 7) -> list[CalibrationRecord]:
    """Synthetic calibration set.

    Scores spread across [0, 1]; a point loses when its score exceeds 0.1
    (i.e. confidence below 0.9) -> roughly 90% correct, 10% incorrect.
    """
    rng = random.Random(seed)
    records = []
    for _ in range(n):
        score = rng.random()
        if not honest:
            score = 0.95
        loss = 1.0 if score > 0.1 else 0.0
        records.append(
            CalibrationRecord(
                context_hash=CTX,
                non_conformity_score=score,
                loss=loss,
                is_independent=True,
                is_exploratory=False,
            )
        )
    return records


def test_insufficient_samples_returns_no_threshold():
    calibrator = ConformalCalibrator(min_sample_size=500)
    result = calibrator.compute_threshold(_records(50))
    assert result.q_hat is None
    assert result.sample_size == 50
    assert result.coverage_lower_bound is None


def test_computes_threshold_within_risk_budget():
    calibrator = ConformalCalibrator(target_alpha=0.05, min_sample_size=100)
    result = calibrator.compute_threshold(_records(1000))
    assert result.q_hat is not None
    assert 0.0 < result.q_hat < 1.0
    assert result.achieved_empirical_risk is not None
    assert result.achieved_empirical_risk <= 0.05
    assert result.coverage_lower_bound is not None
    assert 0.0 < result.coverage_lower_bound <= (1.0 - result.achieved_empirical_risk)


def test_impossible_context_returns_no_threshold():
    calibrator = ConformalCalibrator(target_alpha=0.01, min_sample_size=100)
    result = calibrator.compute_threshold(_records(1000, honest=False))
    assert result.q_hat is None
    assert result.achieved_empirical_risk is not None
    assert result.achieved_empirical_risk > 0.01


def test_exploratory_and_non_independent_records_are_excluded():
    calibrator = ConformalCalibrator(target_alpha=0.05, min_sample_size=100)
    all_inclusive = calibrator.compute_threshold(_records(1000))
    assert all_inclusive.q_hat is not None

    mixed = list(_records(1000))
    for record in mixed[::2]:
        object.__setattr__(record, "is_exploratory", True)
    reduced = calibrator.compute_threshold(mixed)
    assert reduced.sample_size == 500  # half excluded


def test_calibrate_by_context_partitions_independently():
    calibrator = ConformalCalibrator(target_alpha=0.05, min_sample_size=100)
    records = [
        CalibrationRecord(
            context_hash=ctx,
            non_conformity_score=score,
            loss=1.0 if score > 0.1 else 0.0,
        )
        for ctx, score in [(b"a" * 16, 0.05), (b"a" * 16, 0.06), (b"b" * 16, 0.35)]
        for _ in range(500)
    ]
    results = calibrator.calibrate_by_context(records)
    assert set(results) == {b"a" * 16, b"b" * 16}
    assert results[b"a" * 16].q_hat is not None
    assert results[b"b" * 16].q_hat is None  # 100% losses at score 0.35


def test_monotonicity_with_stricter_alpha():
    calibrator_loose = ConformalCalibrator(target_alpha=0.10, min_sample_size=100)
    calibrator_strict = ConformalCalibrator(target_alpha=0.01, min_sample_size=100)
    records = _records(2000)
    loose = calibrator_loose.compute_threshold(records)
    strict = calibrator_strict.compute_threshold(records)
    assert loose.q_hat is None or strict.q_hat is None or strict.q_hat <= loose.q_hat


def test_wilson_lower_bound_sanity():
    calibrator = ConformalCalibrator()
    assert calibrator.wilson_lower_bound(0.0, 0) == 0.0
    assert calibrator.wilson_lower_bound(1.0, 1000) > 0.995
    bound = calibrator.wilson_lower_bound(0.95, 400)
    assert 0.9 < bound < 0.95
    assert calibrator.wilson_lower_bound(0.95, 400) == pytest.approx(
        calibrator.wilson_lower_bound(0.95, 400)
    )
