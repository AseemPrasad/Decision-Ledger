"""Decision Ledger.

A systems primitive that answers a single question with formal statistical
guarantees: when is a small model safe to trust for control-plane decisions?

The ledger records every small-model decision, links it to independent
outcomes, computes confidence thresholds with Split Conformal Risk Control,
and enforces delegation through a fail-closed gatekeeper.
"""

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
    JoinedRecord,
    OutcomeCollector,
    OutcomeRecord,
    OutcomeSource,
)
from .policy import (
    ServingPolicy,
    load_policy,
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

__version__ = "0.1.0"

__all__ = [
    "BatchConsumer",
    "CalibrationContext",
    "CalibrationRecord",
    "CalibrationResult",
    "ConformalCalibrator",
    "Database",
    "DatabaseError",
    "DatabaseIntegrityError",
    "DecisionOutcomeJoiner",
    "DecisionRecord",
    "DecisionType",
    "GateAction",
    "Gatekeeper",
    "JoinedRecord",
    "JsonlExport",
    "OutcomeCollector",
    "OutcomeRecord",
    "OutcomeSource",
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
    "policy_from_results",
    "save_policy",
    "setup_logging",
    "validate_confidence",
    "validate_context_hash",
    "__version__",
]
