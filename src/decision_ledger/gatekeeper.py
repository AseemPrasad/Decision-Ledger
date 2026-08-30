"""The Conformal Gatekeeper: fail-closed hot-path decision enforcement.

The gatekeeper is the *only* component on the serving path. Given a 16-byte
context hash, the small model's self-reported confidence, and a control-plane
decision type it returns one of:

``DELEGATE``         the small model may act
``ESCALATE``         reroute to the large/frontier model (fail-closed default)
``EXPLORE_SHADOW``   counterfactual sampling (both models run, frontier serves)

Design notes
------------

* **Thread safety.** A single ``threading.RLock`` guards the immutable
  policy reference. Evaluations hold it for their (microsecond) lifetime;
  ``reload_policy`` takes the same lock to swap in a new dict. No RCU or
  lock-free tricks are needed under the CPython GIL.
* **Deterministic exploration.** Instead of a PRNG, exploration uses the
  atomic call counter: call ``n`` explores when ``n % int(1/rate) == 0``.
  Results are fully reproducible and the measured exploration rate matches
  the configured parameter exactly.
* **Fail closed.** Unknown context, inactive context, insufficient data, or
  a missing ``q_hat`` all escalate to the frontier model.
* **File-backed reload.** ``from_policy_file`` / ``reload_policy_from_file`` /
  ``convert_policy_dict_to_contexts`` load the schema-1.0 artifacts produced
  by :mod:`~.policy` (``PolicyGenerator``) and swap them in atomically.
* **No allocations on the hot path.** The decision itself performs only dict
  lookups, integer increments, and float comparisons against pre-existing
  singletons. Optionally attaching a :class:`~.telemetry.RingBuffer` records
  each decision (measured latency included) but is off by default.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict, Optional

from .telemetry import DecisionRecord, RingBuffer
from .utils import decision_id, unhex_hash

logger = logging.getLogger(__name__)

# Control-plane decision type names as accepted by ``evaluate``.
_DECISION_TYPES: Dict[str, int] = {
    "route": 0,
    "judge": 1,
    "speculate": 2,
    "mutate": 3,
    "summarize": 4,
    "abstain": 5,
}
_DECISION_NAMES: Dict[int, str] = {code: name for name, code in _DECISION_TYPES.items()}
_UNKNOWN_TYPE = -1
_DEFAULT_TYPE_CODE = 0


class DecisionType(IntEnum):
    """Control-plane decisions a small model may be trusted with."""

    ROUTE = 0
    JUDGE = 1
    SPECULATE = 2
    MUTATE = 3
    SUMMARIZE = 4
    ABSTAIN = 5


class GateAction(IntEnum):
    """Outcome of a gatekeeper evaluation."""

    DELEGATE = 0
    ESCALATE = 1
    EXPLORE_SHADOW = 2


@dataclass
class CalibrationContext:
    """Calibrated operating envelope for a single context.

    ``context_hash`` is the 16-byte BLAKE3 (truncated) reference derived from
    the prompt template, model weights, quantization, adapter, temperature and
    decision type. ``q_hat`` is the conformal non-conformity threshold;
    ``current_sample_size`` and ``is_active`` are maintained by the
    calibration pipeline.
    """

    context_hash: bytes
    q_hat: Optional[float] = None
    min_sample_size: int = 100
    current_sample_size: int = 0
    is_active: bool = False

    @property
    def has_enough_data(self) -> bool:
        """True once the calibrated sample count meets ``min_sample_size``."""
        return self.current_sample_size >= self.min_sample_size


@dataclass
class Gatekeeper:
    """Evaluate delegation decisions against a calibrated policy dict.

    ``policy`` maps a 16-byte context hash to a :class:`CalibrationContext`.
    ``exploration_rate`` is the epsilon for stratified counterfactual
    sampling; it must lie in ``[0.0, 1.0]``.

    ``policy`` may be omitted (fail-closed empty policy, causing every
    evaluation to escalate) or replaced by ``policy_file``, which loads a
    schema-1.0 artifact from disk and takes precedence when both are given.
    ``policy_file`` wins because the artifact is the source of truth for
    ``policy_version``, which the load/reload logs report.
    """

    policy: Dict[bytes, CalibrationContext] = field(default_factory=dict)
    policy_file: Optional[str] = field(default=None, repr=False)
    exploration_rate: float = 0.02
    telemetry: Optional[RingBuffer] = None

    _policy_version: Optional[str] = field(default=None, init=False, repr=False)
    _lock: threading.RLock = field(init=False, repr=False)
    _call_count: int = field(init=False, repr=False)
    _delegate: int = field(init=False, repr=False)
    _escalate: int = field(init=False, repr=False)
    _explore: int = field(init=False, repr=False)
    _calls: Dict[int, int] = field(init=False, repr=False)
    _escalated: Dict[int, int] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not 0.0 <= self.exploration_rate <= 1.0:
            raise ValueError("exploration_rate must be within [0.0, 1.0]")
        if self.policy_file is not None:
            policy_dict = self._load_policy_file(self.policy_file)
            self.policy = self.convert_policy_dict_to_contexts(policy_dict)
            self._policy_version = str(policy_dict.get("policy_version"))
            logger.info(
                "[Loaded policy: version=%s, contexts=%d]",
                self._policy_version,
                len(self.policy),
            )
        self._lock = threading.RLock()
        self._call_count = 0
        self._delegate = 0
        self._escalate = 0
        self._explore = 0
        self._calls = {code: 0 for code in _DECISION_NAMES}
        self._escalated = {code: 0 for code in _DECISION_NAMES}

    def evaluate(
        self, context_hash: bytes, model_confidence: float, decision_type: str
    ) -> GateAction:
        """Decide whether the small model may act for this context.

        Latency target: sub-microsecond amortized; never more than 1 ms per
        call. The decision path allocates no objects.
        """
        code = _DECISION_TYPES.get(decision_type, _UNKNOWN_TYPE)
        if code == _UNKNOWN_TYPE:
            logger.warning(
                "unknown decision_type %r; treating as %r",
                decision_type,
                _DECISION_NAMES[_DEFAULT_TYPE_CODE],
            )
            code = _DEFAULT_TYPE_CODE

        confidence = model_confidence
        if confidence < 0.0:
            logger.warning(
                "negative confidence %.3f for context %s; clamping to 0.0",
                model_confidence,
                context_hash.hex(),
            )
            confidence = 0.0
        elif confidence > 1.0:
            confidence = 1.0
        elif confidence != confidence:  # NaN
            logger.warning(
                "NaN confidence for context %s; clamping to 0.0",
                context_hash.hex(),
            )
            confidence = 0.0

        start_ns = time.perf_counter_ns() if self.telemetry is not None else 0

        with self._lock:
            calls = self._calls
            escalated = self._escalated
            calls[code] += 1

            context = self.policy.get(context_hash)
            if context is None:
                # Unknown context: fail closed.
                self._escalate += 1
                escalated[code] += 1
                return self._record(context_hash, confidence, code, GateAction.ESCALATE, start_ns)

            if not context.is_active:
                self._escalate += 1
                escalated[code] += 1
                return self._record(context_hash, confidence, code, GateAction.ESCALATE, start_ns)

            if context.current_sample_size < context.min_sample_size:
                # Insufficient statistical power: fail closed.
                self._escalate += 1
                escalated[code] += 1
                return self._record(context_hash, confidence, code, GateAction.ESCALATE, start_ns)

            if context.q_hat is None:
                self._escalate += 1
                escalated[code] += 1
                return self._record(context_hash, confidence, code, GateAction.ESCALATE, start_ns)

            if self._should_explore():
                self._explore += 1
                return self._record(
                    context_hash, confidence, code, GateAction.EXPLORE_SHADOW, start_ns
                )

            non_conformity = 1.0 - confidence
            if non_conformity <= context.q_hat:
                self._delegate += 1
                return self._record(context_hash, confidence, code, GateAction.DELEGATE, start_ns)

            self._escalate += 1
            escalated[code] += 1
            return self._record(context_hash, confidence, code, GateAction.ESCALATE, start_ns)

    def _should_explore(self) -> bool:
        """Deterministic stratified exploration by call number.

        Call ``n`` explores when ``n % int(1.0 / exploration_rate) == 0``, so
        with rate 0.02 exactly one call in every fifty explores. The counter
        is only advanced for *eligible* calls (active context, enough data,
        valid ``q_hat``). Safe to treat as atomic: only ever called while
        holding the RLock, and int increments are atomic under the GIL.
        """
        rate = self.exploration_rate
        if rate <= 0.0:
            return False
        self._call_count += 1
        period = int(1.0 / rate)
        return self._call_count % period == 0

    def reload_policy(self, new_policy: Dict[bytes, CalibrationContext]) -> None:
        """Thread-safe policy swap: replace the context dict reference.

        Evaluations already in flight finish against the old snapshot; every
        subsequent evaluation observes the new one.
        """
        with self._lock:
            self.policy = new_policy

    @classmethod
    def from_policy_file(cls, policy_file: str, exploration_rate: float = 0.02) -> Gatekeeper:
        """Create a gatekeeper from a schema-1.0 policy artifact YAML file.

        Args:
            policy_file: path to a validated policy artifact (as written by
                :class:`~.policy.PolicyGenerator`).
            exploration_rate: stratified counterfactual sampling rate.

        Raises:
            FileNotFoundError: the artifact does not exist.
            PolicyValidationError: the artifact is invalid (subclass of
                :class:`ValueError`).
        """
        return cls(policy_file=policy_file, exploration_rate=exploration_rate)

    def reload_policy_from_file(self, policy_file: str) -> None:
        """Reload policy from a schema-1.0 artifact YAML file.

        Logs the version transition (``old -> new``) and swaps the policy in
        atomically via :meth:`reload_policy`.
        """
        policy_dict = self._load_policy_file(policy_file)
        new_policy = self.convert_policy_dict_to_contexts(policy_dict)
        new_version = str(policy_dict.get("policy_version"))
        logger.info(
            "[Reloaded policy: %s -> %s, contexts=%d]",
            self._policy_version,
            new_version,
            len(new_policy),
        )
        self._policy_version = new_version
        self.reload_policy(new_policy)

    @staticmethod
    def convert_policy_dict_to_contexts(
        policy_dict: dict[str, Any],
    ) -> Dict[bytes, CalibrationContext]:
        """Convert a validated policy artifact dict into gatekeeper contexts.

        Every context becomes a :class:`CalibrationContext`:

        * ``context_hash`` -- decoded from the hex ``context_ref``.
        * ``q_hat`` -- per-context non-conformity threshold (absent when the
          context never reached a threshold).
        * ``min_sample_size`` -- the artifact's global default.
        * ``current_sample_size`` -- per-context sample count.
        * ``is_active`` -- ``True`` only for ``ACTIVE`` contexts; ``DRAINING``
          and ``REVOKED`` contexts are present but inactive, so the gatekeeper
          fails closed for them (escalates).
        """
        min_sample_size = int(policy_dict["global"]["min_sample_size_default"])
        contexts: Dict[bytes, CalibrationContext] = {}
        for entry in policy_dict["contexts"]:
            context_hash = unhex_hash(entry["context_ref"])
            contexts[context_hash] = CalibrationContext(
                context_hash=context_hash,
                q_hat=entry.get("q_hat"),
                min_sample_size=min_sample_size,
                current_sample_size=int(entry["sample_size"]),
                is_active=(entry.get("state") == "ACTIVE"),
            )
        return contexts

    def _load_policy_file(self, policy_file: str) -> dict[str, Any]:
        """Load and validate a policy artifact, deferring the import to avoid
        a circular dependency (:mod:`~.policy` imports this module)."""
        from .policy import load_policy

        return load_policy(policy_file)

    def get_metrics(self) -> Dict[str, Any]:
        """Snapshot of evaluation counts and rates per decision type."""
        with self._lock:
            escalate = self._escalate
            delegate = self._delegate
            explore = self._explore
            calls = self._calls
            escalated = self._escalated

        overall = delegate + escalate + explore
        return {
            "delegate": delegate,
            "escalate": escalate,
            "explore": explore,
            "total": overall,
            "escalation_rate": (escalate / overall) if overall else 0.0,
            "exploration_rate": (explore / overall) if overall else 0.0,
            "per_decision_type": {
                _DECISION_NAMES[code]: {
                    "calls": calls[code],
                    "escalations": escalated[code],
                    "escalation_rate": (escalated[code] / calls[code] if calls[code] else 0.0),
                }
                for code in _DECISION_NAMES
            },
        }

    def _record(
        self,
        context_hash: bytes,
        confidence: float,
        code: int,
        action: GateAction,
        start_ns: int,
    ) -> GateAction:
        """Append a telemetry record when a ring buffer is attached."""
        if self.telemetry is None:
            return action
        record = DecisionRecord.from_evaluation(
            context_hash=context_hash,
            decision_id=decision_id(),
            decision_type=_DECISION_NAMES[code],
            action=action.name,
            confidence=confidence,
            latency_us=(time.perf_counter_ns() - start_ns) // 1000,
        )
        self.telemetry.push(record)
        return action
