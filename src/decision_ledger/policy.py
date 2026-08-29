"""Policy artifacts: the versioned set of calibrated contexts.

Policies are immutable once built, versioned, and swapped atomically into a
:class:`~decision_ledger.gatekeeper.Gatekeeper` via ``reload_policy``. Unknown
contexts are absent from the policy so the gatekeeper fails closed by default.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import yaml

from .calibration import CalibrationResult
from .gatekeeper import CalibrationContext
from .utils import hex_hash, unhex_hash

POLICY_SCHEMA_VERSION = "2.0"


@dataclass
class ServingPolicy:
    """Immutable snapshot of all calibrated contexts at a point in time."""

    schema_version: str = POLICY_SCHEMA_VERSION
    version_id: int = 0
    contexts: Dict[bytes, CalibrationContext] = field(default_factory=dict)


def policy_from_results(
    results: Mapping[bytes, CalibrationResult],
    *,
    version_id: int,
    min_sample_size: int = 100,
) -> ServingPolicy:
    """Build a :class:`ServingPolicy` from per-context calibration results.

    Contexts that did not reach ``min_sample_size`` (or whose empirical risk
    never dropped to the target) stay inactive with ``q_hat=None``, so the
    gatekeeper continues to escalate them.
    """
    contexts: Dict[bytes, CalibrationContext] = {}
    for context_hash, result in results.items():
        active = result.q_hat is not None
        contexts[context_hash] = CalibrationContext(
            context_hash=context_hash,
            q_hat=result.q_hat,
            min_sample_size=min_sample_size,
            current_sample_size=result.sample_size,
            is_active=active,
        )
    return ServingPolicy(version_id=version_id, contexts=contexts)


def to_dict(policy: ServingPolicy) -> dict[str, Any]:
    """Serialize a policy to the YAML-friendly dict representation."""
    contexts = []
    for ctx in policy.contexts.values():
        entry: dict[str, Any] = {
            "context_ref": hex_hash(ctx.context_hash),
            "state": "ACTIVE" if ctx.is_active else "DRAINING",
        }
        if ctx.is_active and ctx.q_hat is not None:
            entry["q_hat"] = ctx.q_hat
        entry["sample_size"] = ctx.current_sample_size
        entry["min_sample_size"] = ctx.min_sample_size
        contexts.append(entry)
    return {
        "schema_version": policy.schema_version,
        "policy_version": f"{policy.version_id:016d}",
        "contexts": contexts,
    }


def from_dict(data: dict[str, Any]) -> ServingPolicy:
    """Parse a policy dict produced by :func:`to_dict`."""
    policy = ServingPolicy(
        schema_version=str(data.get("schema_version", POLICY_SCHEMA_VERSION)),
        version_id=int(str(data.get("policy_version", "0"))),
    )
    for entry in data.get("contexts", []):
        context_hash = unhex_hash(entry["context_ref"])
        is_active = entry.get("state", "DRAINING") == "ACTIVE"
        policy.contexts[context_hash] = CalibrationContext(
            context_hash=context_hash,
            q_hat=entry.get("q_hat"),
            min_sample_size=int(entry.get("min_sample_size", 500)),
            current_sample_size=int(entry.get("sample_size", 0)),
            is_active=is_active,
        )
    return policy


def save_policy(policy: ServingPolicy, path: str | Path) -> Path:
    """Write a policy artifact as YAML, creating parent directories."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(to_dict(policy), sort_keys=False), encoding="utf-8")
    return path


def load_policy(path: str | Path) -> ServingPolicy:
    """Load a policy artifact from YAML."""
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        return from_dict(yaml.safe_load(handle))
