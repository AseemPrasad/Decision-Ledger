"""End-to-end calibration pipeline.

Wires the ledger :class:`Database`, the :class:`ConformalCalibrator`, the
:class:`PolicyGenerator` and a running :class:`Gatekeeper` into one
``run_calibration`` call:

* enumerate the distinct contexts that have outcomes,
* calibrate each (``calibrate_context``) and measure drift (``detect_drift``),
* generate a fresh policy artifact from the aggregated results,
* hot-reload it into the gatekeeper,
* print a one-line summary and return the artifact path.

``get_calibration_stats`` reports the live join rate, per-context sample
sizes, the threshold (``q_hat``) distribution and drift status without
writing anything.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .calibration import CalibrationResult, ConformalCalibrator
from .database import Database
from .gatekeeper import Gatekeeper
from .policy import PolicyGenerator

logger = logging.getLogger(__name__)


class CalibrationPipeline:
    """Orchestrate calibration, policy generation and hot reload."""

    def __init__(
        self,
        database: Database,
        gatekeeper: Gatekeeper,
        policy_generator: PolicyGenerator,
        target_alpha: float = 0.05,
    ) -> None:
        if not 0.0 < target_alpha < 1.0:
            raise ValueError("target_alpha must be within (0.0, 1.0)")
        self.database = database
        self.gatekeeper = gatekeeper
        self.policy_generator = policy_generator
        self.target_alpha = target_alpha
        self.calibrator = ConformalCalibrator(
            database=database,
            target_alpha=target_alpha,
            min_sample_size=policy_generator.min_sample_size_default,
        )

    def run_calibration(self) -> str:
        """Calibrate every context, publish a policy, and reload the gatekeeper.

        Returns:
            The path of the newly generated and loaded policy artifact.
        """
        context_hashes = self._context_hashes()
        results: Dict[bytes, CalibrationResult] = {}
        drift_by_context: Dict[bytes, bool] = {}

        for context_hash in context_hashes:
            result = self.calibrator.calibrate_context(context_hash)
            drift = self.calibrator.detect_drift(context_hash)
            results[context_hash] = result
            drift_by_context[context_hash] = drift["drift_detected"]
            logger.info(
                "[Pipeline context %s: q_hat=%s samples=%d drift=%s]",
                context_hash.hex(),
                result.q_hat,
                result.sample_size,
                drift["drift_detected"],
            )

        policy_file = self.policy_generator.generate_policy(results)
        self.gatekeeper.reload_policy_from_file(policy_file)

        activated = [c for c in results if results[c].q_hat is not None]
        draining = [c for c in results if results[c].q_hat is None]
        drifted = [c for c in results if drift_by_context.get(c, False)]
        print(
            f"[Calibration summary: contexts={len(results)}"
            f" activated={len(activated)} draining={len(draining)}"
            f" drift={len(drifted)} policy={policy_file}]"
        )
        return policy_file

    def get_calibration_stats(self) -> Dict[str, Any]:
        """Report a read-only snapshot of the current calibration state.

        Returns:
            ``join_rate`` (decisions with an outcome / total decisions),
            ``samples_per_context`` (hex context to outcome count),
            ``q_hat_distribution`` (activated vs draining counts and the
            per-context thresholds) and ``drift`` (detected count plus the
            drifted context hexes).
        """
        join_stats = self.database.get_join_statistics()
        context_hashes = self._context_hashes()

        samples: Dict[str, int] = {}
        q_hats: Dict[str, Optional[float]] = {}
        drift_by_context: Dict[str, bool] = {}
        for context_hash in context_hashes:
            hex_hash = context_hash.hex()
            result = self.calibrator.calibrate_context(context_hash)
            samples[hex_hash] = result.sample_size
            q_hats[hex_hash] = result.q_hat
            drift_by_context[hex_hash] = self.calibrator.detect_drift(context_hash)[
                "drift_detected"
            ]

        activated = [h for h, q in q_hats.items() if q is not None]
        draining = [h for h, q in q_hats.items() if q is None]
        drifted = [h for h, d in drift_by_context.items() if d]

        return {
            "total_decisions": join_stats["total_decisions"],
            "total_outcomes": join_stats["total_outcomes"],
            "join_rate": join_stats["match_rate"],
            "contexts_calibrated": len(context_hashes),
            "samples_per_context": samples,
            "q_hat_distribution": {
                "activated_count": len(activated),
                "draining_count": len(draining),
                "activated_contexts": activated,
                "draining_contexts": draining,
                "q_hats": {h: q for h, q in q_hats.items() if q is not None},
            },
            "drift": {
                "drift_detected_count": len(drifted),
                "drifted_contexts": drifted,
            },
        }

    def _context_hashes(self) -> List[bytes]:
        """Distinct context hashes that currently have at least one outcome."""
        joined = self.database.get_joined_records(include_unmatched=False)
        return sorted({row["context_hash"] for row in joined})
