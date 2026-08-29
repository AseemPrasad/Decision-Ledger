"""Outcome collection and decision-outcome joining.

Outcomes arrive from *independent* sources (human review, task metrics,
model verification, user reports) anywhere from seconds to days after a
decision. The joiner links each outcome back to its decision by
``decision_id`` so the calibration engine can compute empirical risk.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .telemetry import DecisionRecord
from .utils import now_ns


class OutcomeSource:
    """Outcome source identifiers (mirrors the production enum)."""

    HUMAN = 0
    TASK_METRIC = 1
    MODEL_VERIFICATION = 2
    USER_REPORT = 3


@dataclass
class OutcomeRecord:
    """A single outcome observation for a past decision."""

    decision_id: str
    outcome_timestamp_ns: int = 0
    outcome_source: int = OutcomeSource.HUMAN
    outcome_value: float = 1.0
    metadata: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.outcome_timestamp_ns == 0:
            self.outcome_timestamp_ns = now_ns()

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "outcome_timestamp_ns": self.outcome_timestamp_ns,
            "outcome_source": self.outcome_source,
            "outcome_value": self.outcome_value,
            "metadata": self.metadata or {},
        }


@dataclass
class JoinedRecord:
    """A decision linked to its eventual outcome."""

    decision_id: str
    context_hash: bytes
    decision_type: str
    model_confidence: float
    non_conformity: float
    action_taken: str
    outcome_source: int
    outcome_value: float
    decision_timestamp_ns: int
    outcome_timestamp_ns: int

    @property
    def latency_delta_ns(self) -> int:
        return self.outcome_timestamp_ns - self.decision_timestamp_ns

    @property
    def loss(self) -> float:
        """0.0 = correct, 1.0 = incorrect (binary outcome aggregation)."""
        return 0.0 if self.outcome_value >= 0.5 else 1.0


class OutcomeCollector:
    """Collect and persist outcome records."""

    def __init__(self, paths: List[Path] | None = None) -> None:
        self._records: List[OutcomeRecord] = []
        self._paths = paths or []

    def record(
        self,
        decision_id: str,
        outcome_value: float,
        *,
        outcome_source: int = OutcomeSource.HUMAN,
        metadata: Optional[dict[str, Any]] = None,
    ) -> OutcomeRecord:
        outcome = OutcomeRecord(
            decision_id=decision_id,
            outcome_source=outcome_source,
            outcome_value=outcome_value,
            metadata=metadata,
        )
        self._records.append(outcome)
        return outcome

    def iter_records(self) -> Iterable[OutcomeRecord]:
        return iter(self._records)

    def export(self, path: str | Path) -> Path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for record in self._records:
                handle.write(json.dumps(record.to_dict()))
                handle.write("\n")
        return path

    def import_file(self, path: str | Path) -> int:
        imported = 0
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                data = json.loads(line)
                self._records.append(
                    OutcomeRecord(
                        decision_id=data["decision_id"],
                        outcome_timestamp_ns=int(data["outcome_timestamp_ns"]),
                        outcome_source=int(data["outcome_source"]),
                        outcome_value=float(data["outcome_value"]),
                        metadata=data.get("metadata"),
                    )
                )
                imported += 1
        return imported


class DecisionOutcomeJoiner:
    """Join decision records to outcomes by ``decision_id``."""

    def __init__(self, outcomes: Iterable[OutcomeRecord] | None = None) -> None:
        self._by_id: Dict[str, OutcomeRecord] = {}
        for outcome in outcomes or []:
            self._by_id[outcome.decision_id] = outcome

    def add_outcome(self, outcome: OutcomeRecord) -> None:
        self._by_id[outcome.decision_id] = outcome

    def join(self, decisions: Iterable[DecisionRecord]) -> List[JoinedRecord]:
        joined: List[JoinedRecord] = []
        for decision in decisions:
            outcome = self._by_id.get(decision.decision_id)
            if outcome is None:
                continue
            joined.append(
                JoinedRecord(
                    decision_id=decision.decision_id,
                    context_hash=decision.context_hash,
                    decision_type=decision.decision_type,
                    model_confidence=decision.model_confidence,
                    non_conformity=decision.non_conformity,
                    action_taken=decision.action_taken,
                    outcome_source=outcome.outcome_source,
                    outcome_value=outcome.outcome_value,
                    decision_timestamp_ns=decision.timestamp_ns,
                    outcome_timestamp_ns=outcome.outcome_timestamp_ns,
                )
            )
        return joined


def current_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
