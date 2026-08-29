"""Calibration engine: Split Conformal Risk Control.

Given a calibration set of *independent, non-exploratory* decision outcomes,
compute the largest non-conformity threshold ``q_hat`` such that the
empirical risk stays at or below the user's risk budget ``alpha``.

Finite-sample guarantee (exchangeable calibration data):
    P(S_test <= q_hat) >= 1 - alpha
i.e. the probability that a future decision's non-conformity score falls
below the threshold is at least ``1 - alpha`` — and therefore the delegated
decision is *correct* (loss = 0) at least ``1 - alpha`` of the time.

A Wilson score lower bound reports finite-sample confidence in the achieved
coverage, and contexts with fewer than ``min_sample_size`` records are never
activated (the gatekeeper keeps escalating them).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import numpy as np


@dataclass(frozen=True)
class CalibrationRecord:
    """One labeled decision used for conformal calibration."""

    context_hash: bytes
    non_conformity_score: float  # S = 1 - confidence
    loss: float  # 0.0 = correct, 1.0 = incorrect
    is_independent: bool = True  # outcome source is not the small model itself
    is_exploratory: bool = False  # True if gathered via shadow exploration


@dataclass(frozen=True)
class CalibrationResult:
    """Output of calibration for a single context."""

    q_hat: Optional[float]
    sample_size: int
    coverage_lower_bound: Optional[float]
    achieved_empirical_risk: Optional[float]


class ConformalCalibrator:
    """Split Conformal Risk Control over per-context calibration records."""

    def __init__(
        self,
        target_alpha: float = 0.05,
        min_sample_size: int = 500,
        confidence_level: float = 0.95,
    ) -> None:
        if not 0.0 < target_alpha < 1.0:
            raise ValueError("target_alpha must be within (0.0, 1.0)")
        self.target_alpha = target_alpha
        self.min_sample_size = min_sample_size
        self.confidence_level = confidence_level
        self.z_score = 1.96  # standard normal quantile for ~95%

    def compute_threshold(
        self, records: Iterable[CalibrationRecord]
    ) -> CalibrationResult:
        """Compute ``q_hat`` for a single context's calibration records."""
        valid = [r for r in records if r.is_independent and not r.is_exploratory]
        n = len(valid)

        if n < self.min_sample_size:
            return CalibrationResult(
                q_hat=None,
                sample_size=n,
                coverage_lower_bound=None,
                achieved_empirical_risk=None,
            )

        scores = np.array([r.non_conformity_score for r in valid], dtype=np.float64)
        losses = np.array([r.loss for r in valid], dtype=np.float64)

        order = np.argsort(scores)
        sorted_scores = scores[order]
        sorted_losses = losses[order]

        cumulative_losses = np.cumsum(sorted_losses)
        denominators = np.arange(1, n + 1, dtype=np.float64)
        empirical_risk = cumulative_losses / denominators

        valid_indices = np.where(empirical_risk <= self.target_alpha)[0]
        if len(valid_indices) == 0:
            # Even the tightest threshold would exceed the risk budget.
            return CalibrationResult(
                q_hat=None,
                sample_size=n,
                coverage_lower_bound=None,
                achieved_empirical_risk=float(empirical_risk[0]),
            )

        max_valid_idx = int(valid_indices[-1])
        q_hat = float(sorted_scores[max_valid_idx])
        achieved_risk = float(empirical_risk[max_valid_idx])
        coverage = 1.0 - achieved_risk

        return CalibrationResult(
            q_hat=q_hat,
            sample_size=n,
            coverage_lower_bound=self.wilson_lower_bound(coverage, max_valid_idx + 1),
            achieved_empirical_risk=achieved_risk,
        )

    def wilson_lower_bound(self, p: float, n: int) -> float:
        """Wilson score interval lower bound for finite-sample confidence."""
        if n == 0:
            return 0.0
        z = self.z_score
        denominator = 1.0 + z**2 / n
        center = p + z**2 / (2.0 * n)
        margin = z * np.sqrt((p * (1.0 - p) + z**2 / (4.0 * n)) / n)
        return max(0.0, (center - margin) / denominator)

    def calibrate_by_context(
        self, records: Iterable[CalibrationRecord]
    ) -> Dict[bytes, CalibrationResult]:
        """Group records by context hash and calibrate each independently."""
        grouped: Dict[bytes, List[CalibrationRecord]] = {}
        for record in records:
            grouped.setdefault(record.context_hash, []).append(record)
        return {
            ctx_hash: self.compute_threshold(group)
            for ctx_hash, group in grouped.items()
        }
