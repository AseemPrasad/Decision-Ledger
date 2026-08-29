"""Calibration engine: Split Conformal Risk Control.

Given a calibration set of *independent, non-exploratory* decisions that were
eventually observed (outcome known), compute the largest non-conformity
threshold ``q_hat`` such that the empirical risk stays at or below the user's
risk budget ``alpha``.

Finite-sample guarantee (exchangeable calibration data):
    P(Loss > 0) <= alpha
i.e. the probability that a future *delegated* decision is incorrect is at
most ``alpha``, so the small model may act at least ``1 - alpha`` of the time.

The engine is database-backed:

* :meth:`ConformalCalibrator.calibrate_context` reads ``joined_records`` for a
  context (the durable source of decision / outcome pairs), applies the
  calibration filters in SQL, and returns a :class:`CalibrationResult`. Only
  *independent* (never ``model_verification`` / self-verified) and
  *non-exploratory* (never ``EXPLORE_SHADOW``) decisions may calibrate the
  live threshold.
* :meth:`ConformalCalibrator.detect_drift` compares accuracy on the full
  confidence range (``EXPLORE_SHADOW`` records) against accuracy on the active
  delegation range (``DELEGATE`` records) and alerts when the divergence
  exceeds 5%.
* The pure-statistics core (:meth:`compute_threshold` /
  :meth:`calibrate_by_context`) stays available for offline demos and tests
  without a database.

Edge cases are handled explicitly: contexts below ``min_sample_size`` never
activate, a context whose empirical risk exceeds ``alpha`` at every threshold
activates nothing, and an all-success calibration set treats ``q_hat`` as
unconstrained (the largest observed non-conformity).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import numpy as np

from .database import Database
from .outcomes import OutcomeSource
from .telemetry import EXPLORATORY
from .utils import validate_context_hash

logger = logging.getLogger(__name__)

# Fraction of absolute accuracy divergence between the full confidence range
# and the active delegation range that triggers a drift alert.
DRIFT_DIVERGENCE_THRESHOLD = 0.05

# Stored ``outcome_source`` values that are *not* independent: the model
# verifying its own output would self-confirm, so it can never calibrate the
# live threshold. Everything else in :class:`OutcomeSource` is independent.
_INDEPENDENT_OUTCOME_SOURCES: tuple[str, ...] = tuple(
    source.value
    for source in OutcomeSource
    if source is not OutcomeSource.MODEL_VERIFICATION
)


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

    q_hat: Optional[float] = None
    sample_size: int = 0
    coverage_lower_bound: Optional[float] = None
    achieved_empirical_risk: Optional[float] = None
    min_observed_loss: float = 0.0
    max_observed_loss: float = 0.0


def loss_from_outcome_value(outcome_value: float) -> float:
    """Binary aggregation: 0.0 = correct, 1.0 = incorrect.

    Matches :attr:`JoinedRecord.loss` (``outcome_value >= 0.5`` is a success),
    so the database-backed path and the offline join path agree.
    """
    return 0.0 if outcome_value >= 0.5 else 1.0


def _accuracy(losses: List[float]) -> float:
    """Fraction of correct (loss == 0) records; 0.0 when no data exists."""
    if not losses:
        return 0.0
    return 1.0 - sum(losses) / len(losses)


class ConformalCalibrator:
    """Split Conformal Risk Control over per-context calibration data.

    Construct with the ledger :class:`Database` to calibrate live contexts
    (``calibrate_context``) and monitor drift (``detect_drift``). With no
    database the pure-statistics methods (``compute_threshold`` /
    ``calibrate_by_context``) still work on :class:`CalibrationRecord` inputs
    for offline demos and tests.
    """

    def __init__(
        self,
        database: Optional[Database] = None,
        target_alpha: float = 0.05,
        min_sample_size: int = 100,
        confidence_level: float = 0.95,
    ) -> None:
        if not 0.0 < target_alpha < 1.0:
            raise ValueError("target_alpha must be within (0.0, 1.0)")
        if min_sample_size < 1:
            raise ValueError("min_sample_size must be >= 1")
        if not 0.0 < confidence_level < 1.0:
            raise ValueError("confidence_level must be within (0.0, 1.0)")
        self.database = database
        self.target_alpha = target_alpha
        self.min_sample_size = min_sample_size
        self.confidence_level = confidence_level
        # Standard normal quantile for a 95% confidence interval.
        self.z_score = 1.96

    # ------------------------------------------------------------------ #
    # Pure-statistics core
    # ------------------------------------------------------------------ #

    def compute_threshold(
        self, records: Iterable[CalibrationRecord]
    ) -> CalibrationResult:
        """Compute ``q_hat`` for a single context's calibration records.

        Only ``is_independent and not is_exploratory`` records count. The
        threshold is the largest non-conformity score whose cumulative
        empirical risk (over the prefix of scores sorted ascending) stays at
        or below ``target_alpha``; with no such prefix, ``q_hat`` is ``None``.
        """
        valid = [r for r in records if r.is_independent and not r.is_exploratory]
        n = len(valid)

        if n == 0:
            return CalibrationResult()

        losses = [r.loss for r in valid]
        min_loss = min(losses)
        max_loss = max(losses)

        if n < self.min_sample_size:
            return CalibrationResult(
                q_hat=None,
                sample_size=n,
                coverage_lower_bound=None,
                achieved_empirical_risk=None,
                min_observed_loss=min_loss,
                max_observed_loss=max_loss,
            )

        scores = np.array([r.non_conformity_score for r in valid], dtype=np.float64)
        losses_arr = np.array(losses, dtype=np.float64)

        order = np.argsort(scores)
        sorted_scores = scores[order]
        sorted_losses = losses_arr[order]

        cumulative_losses = np.cumsum(sorted_losses)
        empirical_risk = cumulative_losses / np.arange(1, n + 1, dtype=np.float64)

        within_budget = np.where(empirical_risk <= self.target_alpha)[0]
        if len(within_budget) == 0:
            # Even the tightest threshold would exceed the risk budget.
            return CalibrationResult(
                q_hat=None,
                sample_size=n,
                coverage_lower_bound=None,
                achieved_empirical_risk=float(empirical_risk[0]),
                min_observed_loss=min_loss,
                max_observed_loss=max_loss,
            )

        max_valid_idx = int(within_budget[-1])
        achieved_risk = float(empirical_risk[max_valid_idx])
        coverage = 1.0 - achieved_risk

        return CalibrationResult(
            q_hat=float(sorted_scores[max_valid_idx]),
            sample_size=n,
            coverage_lower_bound=self._wilson_interval_lower(
                coverage, max_valid_idx + 1
            ),
            achieved_empirical_risk=achieved_risk,
            min_observed_loss=min_loss,
            max_observed_loss=max_loss,
        )

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

    def _wilson_interval_lower(self, p: float, n: int) -> float:
        """Lower bound of the Wilson score interval for coverage ``p``.

        Args:
            p: Point estimate of coverage (``1.0 - achieved_empirical_risk``).
            n: Number of samples the estimate is based on.

        Returns:
            The conservative lower bound (clamped to ``>= 0.0``).
        """
        if n <= 0:
            return 0.0
        z = self.z_score
        denominator = 1.0 + z * z / n
        center = p + z * z / (2.0 * n)
        margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
        return max(0.0, (center - margin) / denominator)

    def wilson_lower_bound(self, p: float, n: int) -> float:
        """Backward-compatible alias for :meth:`_wilson_interval_lower`."""
        return self._wilson_interval_lower(p, n)

    # ------------------------------------------------------------------ #
    # Database-backed context calibration
    # ------------------------------------------------------------------ #

    def calibrate_context(self, context_hash: bytes) -> CalibrationResult:
        """Calibrate one live context from the durable ``joined_records`` table.

        Reads every joined decision for ``context_hash`` that has an outcome,
        excluding ``EXPLORE_SHADOW`` records and any decision whose outcomes
        are not independent (``model_verification`` only). The threshold,
        Wilson coverage bound and observed loss span are reported.

        Args:
            context_hash: 16-byte (128-bit) context reference.

        Raises:
            ValueError: If ``context_hash`` is not a valid context hash or no
                database was provided to the constructor.
            DatabaseError: On a persistent store failure.
        """
        database = self._require_database()
        _require_valid_context_hash(context_hash)

        placeholders = ", ".join("?" for _ in _INDEPENDENT_OUTCOME_SOURCES)
        query = (
            "SELECT j.* FROM joined_records j"
            " WHERE j.context_hash = ?"
            " AND j.outcome_value IS NOT NULL"
            " AND j.action_taken <> ?"
            " AND EXISTS ("
            "   SELECT 1 FROM outcomes o"
            "   WHERE o.decision_id = j.decision_id"
            f"   AND o.outcome_source IN ({placeholders})"
            ")"
        )
        rows = database.execute_query(
            query, (context_hash, EXPLORATORY, *_INDEPENDENT_OUTCOME_SOURCES)
        )

        result = self.compute_threshold(
            CalibrationRecord(
                context_hash=context_hash,
                non_conformity_score=row["non_conformity"],
                loss=loss_from_outcome_value(row["outcome_value"]),
                is_independent=True,
                is_exploratory=False,
            )
            for row in rows
        )

        logger.info(
            "[Calibrated context %s: q_hat=%s samples=%d coverage=%s]",
            context_hash.hex(),
            result.q_hat,
            result.sample_size,
            result.coverage_lower_bound,
        )
        return result

    def detect_drift(self, context_hash: bytes) -> Dict[str, Any]:
        """Compare accuracy across confidence ranges to flag drift.

        Accuracy on the full confidence range (``EXPLORE_SHADOW`` records) is
        compared against accuracy on the active delegation range (``DELEGATE``
        records). A modest divergence is expected (delegation serves the
        high-confidence tail); a divergence above
        ``DRIFT_DIVERGENCE_THRESHOLD`` (5%) means the confidence score has
        stopped being informative and is flagged.

        Args:
            context_hash: 16-byte (128-bit) context reference.

        Returns:
            ``drift_detected``, ``full_range_accuracy``,
            ``active_range_accuracy`` and ``divergence`` (absolute difference).

        Raises:
            ValueError: If ``context_hash`` is not a valid context hash or no
                database was provided to the constructor.
            DatabaseError: On a persistent store failure.
        """
        database = self._require_database()
        _require_valid_context_hash(context_hash)

        rows = database.execute_query(
            "SELECT action_taken, outcome_value FROM joined_records"
            " WHERE context_hash = ? AND outcome_value IS NOT NULL",
            (context_hash,),
        )

        full_range_losses: List[float] = []
        active_range_losses: List[float] = []
        for row in rows:
            loss = loss_from_outcome_value(row["outcome_value"])
            if row["action_taken"] == EXPLORATORY:
                full_range_losses.append(loss)
            elif row["action_taken"] == "DELEGATE":
                active_range_losses.append(loss)

        full_range_accuracy = _accuracy(full_range_losses)
        active_range_accuracy = _accuracy(active_range_losses)
        divergence = abs(full_range_accuracy - active_range_accuracy)
        drift_detected = divergence > DRIFT_DIVERGENCE_THRESHOLD

        if drift_detected:
            logger.warning(
                "[Drift context %s: full=%.4f active=%.4f divergence=%.4f]",
                context_hash.hex(),
                full_range_accuracy,
                active_range_accuracy,
                divergence,
            )
        else:
            logger.info(
                "[Drift context %s: full=%.4f active=%.4f divergence=%.4f]",
                context_hash.hex(),
                full_range_accuracy,
                active_range_accuracy,
                divergence,
            )

        return {
            "drift_detected": drift_detected,
            "full_range_accuracy": full_range_accuracy,
            "active_range_accuracy": active_range_accuracy,
            "divergence": divergence,
        }

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _require_database(self) -> Database:
        if self.database is None:
            raise ValueError(
                "ConformalCalibrator needs a Database to calibrate contexts"
            )
        return self.database


def _require_valid_context_hash(context_hash: bytes) -> None:
    if not validate_context_hash(context_hash):
        raise ValueError(
            "context_hash must be exactly 16 bytes (128-bit context reference)"
        )
