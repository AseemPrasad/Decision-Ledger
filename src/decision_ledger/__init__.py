"""Decision Ledger.

A systems primitive that answers a single question with formal statistical
guarantees: when is a small model safe to trust for control-plane decisions?

The ledger records every small-model decision, links it to independent
outcomes, computes confidence thresholds with Split Conformal Risk Control,
and enforces delegation through a fail-closed gatekeeper.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .calibration import (
    CalibrationRecord,
    CalibrationResult,
    ConformalCalibrator,
)
from .consumer import BatchConsumer, JsonlExport
from .database import (
    Database,
    DatabaseError,
    DatabaseIntegrityError,
    Joiner,
    init_database,
    outcome_source_from_text,
    outcome_source_to_text,
)
from .gatekeeper import (
    CalibrationContext,
    DecisionType,
    GateAction,
    Gatekeeper,
)
from .outcomes import (
    DecisionOutcomeJoiner,
    InMemoryOutcomeCollector,
    JoinedRecord,
    OutcomeCollector,
    OutcomeRecord,
    OutcomeSource,
    _coerce_outcome_source,
)
from .pipeline import CalibrationPipeline
from .policy import (
    PolicyError,
    PolicyGenerator,
    PolicyValidationError,
    ServingPolicy,
    load_policy,
    policy_from_dict,
    policy_from_results,
    save_policy,
)
from .telemetry import (
    DecisionRecord,
    RingBuffer,
)
from .utils import (
    context_hash,
    decision_id,
    generate_uuidv7,
    make_context_hash,
    now_ns,
    now_us,
    setup_logging,
    validate_confidence,
    validate_context_hash,
)

logger = logging.getLogger(__name__)

__version__ = "0.1.0"


class DecisionLedger:
    """Orchestrator that wires every ledger component into one serving API.

    Coordinates the SQLite :class:`Database`, the :class:`RingBuffer`, the
    :class:`~.gatekeeper.Gatekeeper`, the :class:`~.outcomes.OutcomeCollector`,
    the :class:`~.calibration.ConformalCalibrator`, the
    :class:`~.policy.PolicyGenerator` and the :class:`BatchConsumer`:

    * ``evaluate`` enforces the fail-closed gate and records every decision
      into the ring buffer (via the gatekeeper's telemetry hook),
    * ``log_outcome`` attaches an independent outcome to a decision,
    * ``calibrate`` drains and joins the ledger, recalibrates every context,
      publishes a fresh policy artifact and hot-reloads it into the
      gatekeeper,
    * ``stats`` reports a live operational snapshot, and
    * ``shutdown`` flushes, saves a final stats snapshot and closes the store.

    Example:
        >>> ledger = DecisionLedger("ledger.db", auto_start_consumer=False)
        >>> ctx = make_context_hash("qwen-7b", "routing")
        >>> ledger.evaluate(ctx, confidence=0.85, decision_type="route")
        'ESCALATE'
        >>> ledger.shutdown()
    """

    def __init__(
        self,
        db_path: str = "ledger.db",
        policy_file: Optional[str] = None,
        exploration_rate: float = 0.02,
        ring_buffer_capacity: int = 100_000,
        flush_interval: float = 5.0,
        auto_start_consumer: bool = True,
    ) -> None:
        """Construct a fully wired ledger.

        Args:
            db_path: Path to the SQLite ledger store.
            policy_file: Path to a schema-1.0 policy artifact (as written by
                :meth:`.policy.PolicyGenerator.generate_policy`). When given,
                the gatekeeper starts from that policy and the policy
                generator writes new artifacts next to it; otherwise the
                gatekeeper is empty (fail-closed) and artifacts go to
                ``data/policies/``.
            exploration_rate: Stratified shadow-sampling rate for the
                gatekeeper.
            ring_buffer_capacity: Capacity of the telemetry ring buffer.
            flush_interval: Maximum seconds allowed between SQLite flushes.
            auto_start_consumer: When ``True``, the background consumer thread
                starts immediately; otherwise call
                ``ledger.consumer.start()`` (or ``ledger.calibrate()``, which
                drains explicitly) first.
        """
        self._db_path = Path(db_path)
        self.database = Database(db_path)
        self.ring_buffer = RingBuffer(capacity=ring_buffer_capacity)
        self.gatekeeper = Gatekeeper(
            policy_file=policy_file,
            exploration_rate=exploration_rate,
            telemetry=self.ring_buffer,
        )
        self.outcome_collector = OutcomeCollector(self.database)
        self.calibrator = ConformalCalibrator(self.database)

        if policy_file:
            policies_dir = Path(policy_file).parent
        else:
            policies_dir = Path("data") / "policies"
        self.policy_generator = PolicyGenerator(str(policies_dir))

        self.consumer = BatchConsumer(
            self.ring_buffer,
            self.database,
            flush_interval=flush_interval,
        )
        self._closed = False

        if auto_start_consumer:
            self.consumer.start()

        logger.info(
            "[Ledger initialized: db=%s policy=%s exploration=%s"
            " buffer_capacity=%d consumer=%s]",
            db_path,
            self.gatekeeper._policy_version,
            exploration_rate,
            ring_buffer_capacity,
            "started" if auto_start_consumer else "stopped",
        )

    # ------------------------------------------------------------------ #
    # Serving path
    # ------------------------------------------------------------------ #

    def evaluate(
        self,
        context_hash: bytes,
        model_confidence: Optional[float] = None,
        decision_type: str = "route",
        *,
        confidence: Optional[float] = None,
    ) -> str:
        """Enforce the gate and record one decision.

        Delegates to :meth:`.gatekeeper.Gatekeeper.evaluate`; when telemetry
        is attached (always, for the orchestrator) the gatekeeper builds a
        :class:`DecisionRecord` and pushes it to the ring buffer on every
        call. The ring buffer feeds the consumer, which flushes decisions to
        SQLite.

        ``model_confidence`` and the alias ``confidence`` mean the same thing;
        pass exactly one. Both spellings are accepted so the serving call
        reads naturally::

            ledger.evaluate(ctx, model_confidence=0.85, decision_type="route")
            ledger.evaluate(ctx, confidence=0.85, decision_type="route")

        Args:
            context_hash: 16-byte (128-bit) context reference.
            model_confidence: Model confidence in [0.0, 1.0].
            decision_type: Free-form decision type (``"route"`` and similar);
                unknown types are treated as the default routing type.
            confidence: Alias for ``model_confidence``.

        Returns:
            The action taken as a string: ``"ESCALATE"`` (fail-closed),
            ``"DELEGATE"`` or ``"EXPLORE_SHADOW"``.

        Raises:
            ValueError: If neither or both of ``model_confidence`` /
                ``confidence`` are supplied, or the ledger is already shut
                down (``RuntimeError``).
        """
        self._require_open()
        if model_confidence is not None and confidence is not None:
            raise ValueError(
                "evaluate() got both model_confidence and confidence; " "pass exactly one of them"
            )
        if model_confidence is None:
            if confidence is None:
                raise ValueError(
                    "evaluate() needs a confidence value; "
                    "pass model_confidence=... or confidence=..."
                )
            model_confidence = confidence

        try:
            action = self.gatekeeper.evaluate(context_hash, model_confidence, decision_type)
        except Exception:
            # Any unexpected failure on the hot path fails closed.
            logger.exception(
                "evaluate failed for context %r; failing closed to ESCALATE",
                context_hash,
            )
            return GateAction.ESCALATE.name

        logger.debug(
            "[Ledger evaluate: context=%s action=%s]",
            context_hash.hex() if isinstance(context_hash, bytes) else context_hash,
            action.name,
        )
        return action.name

    # ------------------------------------------------------------------ #
    # Outcomes
    # ------------------------------------------------------------------ #

    def log_outcome(
        self,
        decision_id: str,
        outcome_value: float,
        outcome_source: str = "human",
        metadata: str | Mapping[str, Any] | None = None,
    ) -> str:
        """Attach an independent outcome to a recorded decision.

        Args:
            decision_id: The decision's ``decision_id``; it must already be
                durable in SQLite (i.e. flushed by the consumer) or a
                ``DecisionNotFoundError`` is raised.
            outcome_value: Float (or numeric string) in [0.0, 1.0]; 0.0 =
                fail, 1.0 = success.
            outcome_source: Source; ``"human"`` (default), ``"task_metric"``,
                ``"user_report"`` or ``"model_verification"`` (the latter is
                never used for calibration).
            metadata: Optional JSON string (or dict) with source-specific data.

        Returns:
            The generated ``outcome_id`` (UUIDv7, 36 chars).

        Raises:
            DecisionNotFoundError: The decision is not in the ledger.
            InvalidOutcomeValueError / InvalidOutcomeSourceError /
            InvalidMetadataError: Invalid arguments.
            DatabaseError: On a persistent store failure.
        """
        self._require_open()
        try:
            outcome_id = self.outcome_collector.log_outcome(
                decision_id,
                outcome_value,
                _coerce_outcome_source(outcome_source),
                metadata,
            )
        except Exception:
            logger.exception("log_outcome failed for decision %s", decision_id)
            raise
        logger.debug("[Ledger logged outcome %s for decision %s]", outcome_id, decision_id)
        return outcome_id

    # ------------------------------------------------------------------ #
    # Calibration
    # ------------------------------------------------------------------ #

    def calibrate(self, target_alpha: Optional[float] = None) -> str:
        """Run the full calibration pipeline and hot-reload the gatekeeper.

        Drains any buffered decisions into SQLite, materializes the
        decision/outcome join, calibrates every context that has outcomes,
        publishes a new policy artifact via the
        :class:`~.policy.PolicyGenerator`, reloads it into the gatekeeper and
        returns the artifact path.

        Args:
            target_alpha: Risk budget per context; defaults to the calibrator's
                current ``target_alpha`` (0.05).

        Returns:
            Path of the newly generated and loaded policy artifact.

        Raises:
            RuntimeError: If the ledger is shut down.
            PolicyValidationError: If calibration produced an invalid artifact.
            DatabaseError: On a persistent store failure.
        """
        self._require_open()
        alpha = self.calibrator.target_alpha if target_alpha is None else target_alpha
        try:
            self.consumer.drain_now()
            self.database.joiner.join_decisions_and_outcomes()
            pipeline = CalibrationPipeline(
                self.database,
                self.gatekeeper,
                self.policy_generator,
                target_alpha=alpha,
            )
            policy_path = pipeline.run_calibration()
        except Exception:
            logger.exception("calibration failed")
            raise
        logger.info("[Ledger calibrated: policy=%s]", policy_path)
        return policy_path

    # ------------------------------------------------------------------ #
    # Statistics
    # ------------------------------------------------------------------ #

    def stats(self) -> Dict[str, Any]:
        """Return a live operational snapshot of the whole ledger.

        Returns:
            Dict with ``total_decisions``, ``total_outcomes``, ``join_rate``
            (decisions with an outcome / total decisions), ``decisions_by_action``
            (counts per gatekeeper action), ``contexts_active`` and
            ``contexts_draining`` (from the loaded policy), ``ring_buffer_fill``
            (0.0-1.0), ``ring_buffer_size``, ``dropped_records``, the current
            ``policy_version``, plus ``gatekeeper`` and ``consumer`` metric
            sub-snapshots.
        """
        self._require_open()
        join_stats = self.database.get_join_statistics()
        action_rows = self.database.execute_query(
            "SELECT action_taken, COUNT(*) AS count FROM decisions" " GROUP BY action_taken"
        )
        decisions_by_action = {row["action_taken"]: int(row["count"]) for row in action_rows}

        contexts = self.gatekeeper.policy
        contexts_active = sum(1 for ctx in contexts.values() if ctx.is_active)

        return {
            "total_decisions": join_stats["total_decisions"],
            "total_outcomes": join_stats["total_outcomes"],
            "join_rate": join_stats["match_rate"],
            "decisions_by_action": decisions_by_action,
            "contexts_active": contexts_active,
            "contexts_draining": len(contexts) - contexts_active,
            "ring_buffer_fill": self.ring_buffer.fill_level(),
            "ring_buffer_size": self.ring_buffer.size(),
            "dropped_records": self.ring_buffer.dropped_count,
            "policy_version": self.gatekeeper._policy_version,
            "gatekeeper": self.gatekeeper.get_metrics(),
            "consumer": self.consumer.get_metrics(),
        }

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def shutdown(self, timeout: float = 10.0) -> Dict[str, Any]:
        """Stop the consumer, flush, save a final stats snapshot and close.

        Idempotent-guard: a second call raises ``RuntimeError``. The final
        stats snapshot is also written to ``<db stem>_stats.json`` next to
        the ledger database so the last-known-good numbers survive restarts.

        Returns:
            The final stats snapshot.
        """
        self._require_open()
        try:
            self.consumer.stop(timeout)
        except Exception:
            logger.exception("consumer shutdown failed; continuing")
        try:
            self.consumer.drain_now()
        except Exception:
            logger.exception("final drain failed; continuing")

        try:
            final_stats = self.stats()
        except Exception:
            logger.exception("final stats snapshot failed")
            final_stats = {}
        self._write_final_stats(final_stats)

        logger.info(
            "[Ledger shutdown: decisions=%d outcomes=%d join_rate=%.4f]",
            final_stats.get("total_decisions", 0),
            final_stats.get("total_outcomes", 0),
            float(final_stats.get("join_rate", 0.0)),
        )
        self.database.close()
        self._closed = True
        return final_stats

    def __enter__(self) -> "DecisionLedger":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.shutdown()

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("DecisionLedger is shut down; create a new instance to use it")

    def _write_final_stats(self, stats: Dict[str, Any]) -> None:
        stats_path = self._db_path.with_name(self._db_path.stem + "_stats.json")
        try:
            stats_path.write_text(json.dumps(stats, indent=2, sort_keys=True), encoding="utf-8")
        except Exception:
            logger.exception("could not write final stats to %s", stats_path)
            return
        logger.info("[Saved final stats to %s]", stats_path)


__all__ = [
    "BatchConsumer",
    "CalibrationContext",
    "CalibrationPipeline",
    "CalibrationRecord",
    "CalibrationResult",
    "ConformalCalibrator",
    "Database",
    "DatabaseError",
    "DatabaseIntegrityError",
    "DecisionOutcomeJoiner",
    "DecisionRecord",
    "DecisionLedger",
    "DecisionType",
    "GateAction",
    "Gatekeeper",
    "InMemoryOutcomeCollector",
    "JoinedRecord",
    "Joiner",
    "JsonlExport",
    "OutcomeCollector",
    "OutcomeRecord",
    "OutcomeSource",
    "PolicyError",
    "PolicyGenerator",
    "PolicyValidationError",
    "RingBuffer",
    "ServingPolicy",
    "context_hash",
    "decision_id",
    "generate_uuidv7",
    "init_database",
    "load_policy",
    "make_context_hash",
    "now_ns",
    "now_us",
    "outcome_source_from_text",
    "outcome_source_to_text",
    "policy_from_dict",
    "policy_from_results",
    "save_policy",
    "setup_logging",
    "validate_confidence",
    "validate_context_hash",
    "__version__",
]
