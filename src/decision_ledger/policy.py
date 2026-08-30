"""Policy artifacts: the versioned, validated set of calibrated contexts.

Policies are immutable once built, versioned, and swapped atomically into a
:class:`~decision_ledger.gatekeeper.Gatekeeper` via ``reload_policy``. Unknown
contexts are absent from the policy so the gatekeeper fails closed by default.

Artifact format (``schema_version: "1.0"``), written by
:class:`PolicyGenerator` and consumed by :func:`load_policy`::

    schema_version: "1.0"
    policy_version: "20260830-153000"        # YYYYMMdd-HHMMSS
    generated_at: "2026-08-30T15:30:00Z"
    global:
      default_alpha: 0.05
      fail_closed: true
      exploration_rate: 0.02
      min_sample_size_default: 100
    contexts:
      - context_ref: "<32-hex>"
        state: ACTIVE
        q_hat: 0.05
        sample_size: 500
        min_sample_size: 100

Each context is in one of three states:

* ``ACTIVE``   -- ``q_hat`` is set and ``sample_size >= min_sample_size``;
  the gatekeeper may delegate.
* ``DRAINING`` -- ``q_hat`` is ``None`` or the sample is still too small;
  the gatekeeper escalates (fail closed).
* ``REVOKED``  -- explicitly revoked (set outside ``generate_policy``); the
  context is excluded from the serving policy entirely.

The ``ServingPolicy`` type is the in-memory serving snapshot consumed by the
gatekeeper; :func:`policy_from_dict` converts a validated artifact into one,
dropping ``REVOKED`` contexts. :func:`save_policy` / :func:`load_policy`
round-trip through the same artifact format for the legacy call sites.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, TypeGuard

import yaml

from .calibration import CalibrationResult
from .gatekeeper import CalibrationContext
from .utils import hex_hash, unhex_hash, validate_context_hash

POLICY_SCHEMA_VERSION = "1.0"

ACTIVE = "ACTIVE"
DRAINING = "DRAINING"
REVOKED = "REVOKED"
_POLICY_STATES = (ACTIVE, DRAINING, REVOKED)

_POLICY_VERSION_RE = r"\d{8}-\d{6}"
_POLICY_FILENAME_PREFIX = "policy_"
_POLICY_EXTENSION = ".yaml"
_POLICY_LATEST_FILENAME = "policy_latest.yaml"
_DEFAULT_POLICIES_DIR = "data/policies"
_DEFAULT_ALPHA = 0.05
_DEFAULT_EXPLORATION_RATE = 0.02
_DEFAULT_MIN_SAMPLE_SIZE = 100

logger = logging.getLogger(__name__)


class PolicyValidationError(ValueError):
    """Raised when a policy artifact fails schema validation."""


class PolicyError(RuntimeError):
    """Raised for operational policy failures (missing history, collisions)."""


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
    min_sample_size: int = _DEFAULT_MIN_SAMPLE_SIZE,
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


def to_dict(
    policy: ServingPolicy,
    *,
    policy_version: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> dict[str, Any]:
    """Serialize a policy to the YAML-friendly dict representation.

    Defaults ``policy_version`` to the current UTC timestamp and
    ``generated_at`` to the current UTC time, so the artifact always passes
    :func:`validate_policy`.
    """
    if policy_version is None:
        policy_version = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    elif not re.fullmatch(_POLICY_VERSION_RE, policy_version):
        raise PolicyValidationError(
            f"invalid policy_version {policy_version!r}; expected YYYYMMdd-HHMMSS"
        )
    if generated_at is None:
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    contexts: list[dict[str, Any]] = []
    for ctx in policy.contexts.values():
        entry: dict[str, Any] = {
            "context_ref": hex_hash(ctx.context_hash),
            "state": ACTIVE if ctx.is_active else DRAINING,
        }
        if ctx.q_hat is not None:
            entry["q_hat"] = ctx.q_hat
        entry["sample_size"] = ctx.current_sample_size
        entry["min_sample_size"] = ctx.min_sample_size
        contexts.append(entry)

    return {
        "schema_version": policy.schema_version,
        "policy_version": policy_version,
        "generated_at": generated_at,
        "global": {
            "default_alpha": _DEFAULT_ALPHA,
            "fail_closed": True,
            "exploration_rate": _DEFAULT_EXPLORATION_RATE,
            "min_sample_size_default": _DEFAULT_MIN_SAMPLE_SIZE,
        },
        "contexts": contexts,
    }


def validate_policy(data: Any) -> bool:
    """Validate a policy artifact dict, raising :class:`PolicyValidationError`.

    .. note::
        Returns ``True`` on success; every failure raises instead, so callers
        cannot accidentally ignore a bad artifact.
    """
    if not isinstance(data, dict):
        raise PolicyValidationError(f"policy artifact must be a mapping, got {type(data).__name__}")

    schema = data.get("schema_version")
    if schema != POLICY_SCHEMA_VERSION:
        raise PolicyValidationError(
            f"unsupported schema_version {schema!r}; expected {POLICY_SCHEMA_VERSION!r}"
        )

    version = data.get("policy_version")
    if not isinstance(version, str) or not re.fullmatch(_POLICY_VERSION_RE, version):
        raise PolicyValidationError(f"invalid policy_version {version!r}; expected YYYYMMdd-HHMMSS")

    generated_at = data.get("generated_at")
    if not isinstance(generated_at, str) or _parse_generated_at(generated_at) is None:
        raise PolicyValidationError(
            f"invalid generated_at {generated_at!r}; expected %Y-%m-%dT%H:%M:%SZ"
        )

    _validate_global(data.get("global"))

    contexts = data.get("contexts")
    if not isinstance(contexts, list):
        raise PolicyValidationError(f"contexts must be a list, got {type(contexts).__name__}")
    seen: set[str] = set()
    for index, entry in enumerate(contexts):
        _validate_context_entry(entry, index, seen)

    return True


def policy_from_dict(data: dict[str, Any]) -> ServingPolicy:
    """Materialize a validated artifact into a serving policy for the gatekeeper.

    ``ACTIVE`` contexts become active with ``q_hat``; ``DRAINING`` contexts stay
    inactive; ``REVOKED`` contexts are excluded, so the gatekeeper fails closed
    for them.
    """
    validate_policy(data)
    policy = ServingPolicy(
        schema_version=str(data["schema_version"]),
        version_id=0,
    )
    for entry in data["contexts"]:
        if entry.get("state") == REVOKED:
            continue
        context_hash = unhex_hash(entry["context_ref"])
        is_active = entry.get("state") == ACTIVE
        policy.contexts[context_hash] = CalibrationContext(
            context_hash=context_hash,
            q_hat=entry.get("q_hat"),
            min_sample_size=int(entry["min_sample_size"]),
            current_sample_size=int(entry["sample_size"]),
            is_active=is_active,
        )
    return policy


def load_policy(path: str | Path) -> dict[str, Any]:
    """Load a policy artifact from YAML and validate it.

    Returns:
        The artifact dict (``schema_version: "1.0"``). Convert to a serving
        policy with :func:`policy_from_dict` when feeding a gatekeeper.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise PolicyValidationError(
            f"policy artifact must be a YAML mapping, got {type(data).__name__}"
        )
    validate_policy(data)
    return data


def save_policy(policy: ServingPolicy, path: str | Path) -> Path:
    """Write a serving policy as a validated schema-1.0 artifact."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(to_dict(policy), sort_keys=False), encoding="utf-8")
    return path


class PolicyGenerator:
    """Produce and manage versioned policy artifacts from calibration results.

    Each :meth:`generate_policy` call writes ``policy_<YYYYMMdd-HHMMSS>.yaml``
    into ``policies_dir`` and repoints ``policy_latest.yaml`` at it. Contexts in
    ``revoked_contexts`` are marked ``REVOKED`` and are never served.
    """

    def __init__(
        self,
        policies_dir: str | Path = _DEFAULT_POLICIES_DIR,
        *,
        default_alpha: float = _DEFAULT_ALPHA,
        fail_closed: bool = True,
        exploration_rate: float = _DEFAULT_EXPLORATION_RATE,
        min_sample_size_default: int = _DEFAULT_MIN_SAMPLE_SIZE,
        revoked_contexts: FrozenSet[bytes] = frozenset(),
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Create a policy generator that writes schema-1.0 YAML artifacts.

        Args:
            policies_dir: Directory where versioned artifacts are stored.
            default_alpha: Per-context risk budget carried into artifacts.
            fail_closed: Whether unknown contexts start as ``ESCALATE``.
            exploration_rate: Stratified shadow-sampling rate.
            min_sample_size_default: Minimum samples before a context activates.
            revoked_contexts: Contexts that are never served (``REVOKED``).
            logger: Optional logger override.
        """
        if not 0.0 < default_alpha <= 1.0:
            raise PolicyValidationError(
                f"default_alpha must be within (0.0, 1.0], got {default_alpha!r}"
            )
        if not 0.0 <= exploration_rate <= 1.0:
            raise PolicyValidationError(
                f"exploration_rate must be within [0.0, 1.0], got {exploration_rate!r}"
            )
        if min_sample_size_default < 1:
            raise PolicyValidationError(
                f"min_sample_size_default must be >= 1, got {min_sample_size_default!r}"
            )
        for context_hash in revoked_contexts:
            if not validate_context_hash(context_hash):
                raise PolicyValidationError(
                    f"revoked context {context_hash!r} is not a valid 16-byte hash"
                )
        self.policies_dir = Path(policies_dir)
        self.default_alpha = default_alpha
        self.fail_closed = fail_closed
        self.exploration_rate = exploration_rate
        self.min_sample_size_default = min_sample_size_default
        self.revoked_contexts = frozenset(revoked_contexts)
        self.logger = logger or logging.getLogger(__name__)

    def generate_policy(
        self,
        calibration_results: Mapping[bytes, CalibrationResult],
        *,
        policy_version: Optional[str] = None,
        force: bool = False,
    ) -> str:
        """Generate a policy artifact from per-context calibration results.

        Args:
            calibration_results: context hash to ``CalibrationResult`` mapping.
            policy_version: explicit ``YYYYMMdd-HHMMSS`` version; default is the
                current UTC timestamp.
            force: overwrite an existing artifact with the same version.

        Returns:
            The path of the written artifact.
        """
        if policy_version is None:
            policy_version = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        elif not re.fullmatch(_POLICY_VERSION_RE, policy_version):
            raise PolicyValidationError(
                f"invalid policy_version {policy_version!r}; expected YYYYMMdd-HHMMSS"
            )

        self.policies_dir.mkdir(parents=True, exist_ok=True)
        policy_file = self.policies_dir / _policy_filename(policy_version)
        if policy_file.exists() and not force:
            raise PolicyError(
                f"policy {policy_file} already exists " f"(pass force=True to overwrite it)"
            )

        artifact: dict[str, Any] = {
            "schema_version": POLICY_SCHEMA_VERSION,
            "policy_version": policy_version,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "global": {
                "default_alpha": self.default_alpha,
                "fail_closed": self.fail_closed,
                "exploration_rate": self.exploration_rate,
                "min_sample_size_default": self.min_sample_size_default,
            },
            "contexts": [
                self._entry_for(context_hash, result)
                for context_hash, result in calibration_results.items()
            ],
        }

        policy_file.write_text(yaml.safe_dump(artifact, sort_keys=False), encoding="utf-8")
        self._refresh_latest_link(policy_file)
        self.logger.info("[Generated policy: %s]", policy_file)
        return str(policy_file)

    def load_latest_policy(self) -> dict[str, Any]:
        """Load the artifact currently pointed at by ``policy_latest.yaml``."""
        latest = self.policies_dir / _POLICY_LATEST_FILENAME
        if not latest.exists():
            raise PolicyError(f"no latest policy found at {latest}")
        return load_policy(latest)

    def get_policy_history(self) -> List[str]:
        """Return generated policy versions available on disk, newest first."""
        if not self.policies_dir.exists():
            return []
        versions: List[str] = []
        for path in self.policies_dir.glob(f"{_POLICY_FILENAME_PREFIX}*{_POLICY_EXTENSION}"):
            if path.name == _POLICY_LATEST_FILENAME:
                continue
            if path.is_file():
                versions.append(path.stem[len(_POLICY_FILENAME_PREFIX) :])
        return sorted(versions, reverse=True)

    def rollback_policy(self, target_version: str) -> str:
        """Point ``policy_latest.yaml`` at a previously generated artifact.

        Returns:
            The path of the artifact now published as latest.
        """
        if not re.fullmatch(_POLICY_VERSION_RE, target_version):
            raise PolicyValidationError(
                f"invalid target_version {target_version!r}; expected YYYYMMdd-HHMMSS"
            )
        self.policies_dir.mkdir(parents=True, exist_ok=True)
        target_file = self.policies_dir / _policy_filename(target_version)
        if not target_file.is_file():
            raise PolicyError(f"policy version {target_version!r} not found in {self.policies_dir}")
        load_policy(target_file)  # surface validation errors before republishing
        self._refresh_latest_link(target_file)
        self.logger.info("[Rolled back policy: %s]", target_file)
        return str(target_file)

    def _entry_for(self, context_hash: bytes, result: CalibrationResult) -> dict[str, Any]:
        """Build the schema-1.0 context entry for one calibration result."""
        if not validate_context_hash(context_hash):
            raise PolicyValidationError(f"invalid context_hash {context_hash!r}: must be 16 bytes")
        is_revoked = context_hash in self.revoked_contexts
        active = (
            not is_revoked
            and result.q_hat is not None
            and result.sample_size >= self.min_sample_size_default
        )
        entry: dict[str, Any] = {
            "context_ref": hex_hash(context_hash),
            "state": REVOKED if is_revoked else (ACTIVE if active else DRAINING),
        }
        if result.q_hat is not None and not is_revoked:
            entry["q_hat"] = result.q_hat
        entry["sample_size"] = result.sample_size
        entry["min_sample_size"] = self.min_sample_size_default
        return entry

    def _refresh_latest_link(self, policy_file: Path) -> None:
        """Repoint ``policy_latest.yaml`` at ``policy_file``.

        Prefers a symlink; on Windows without Developer Mode/privileges the OS
        refuses to create one, so the artifact is copied as a plain file
        instead. Both load identically through :func:`load_policy`.
        """
        latest = self.policies_dir / _POLICY_LATEST_FILENAME
        try:
            if latest.exists() or latest.is_symlink():
                latest.unlink()
        except OSError:
            pass
        try:
            os.symlink(policy_file.name, str(latest))
        except (OSError, NotImplementedError):
            latest.write_bytes(policy_file.read_bytes())


def _policy_filename(policy_version: str) -> str:
    """Return the artifact filename for a ``YYYYMMDD-HHMMSS`` version."""
    return f"{_POLICY_FILENAME_PREFIX}{policy_version}{_POLICY_EXTENSION}"


def _parse_generated_at(value: str) -> Optional[datetime]:
    """Parse an ISO-8601 ``...Z`` timestamp, or None when malformed."""
    if not value.endswith("Z"):
        return None
    try:
        return datetime.strptime(value[:-1], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


def _is_number(value: Any) -> TypeGuard[int | float]:
    """True for ints/floats (excluding bools) usable as numeric YAML scalars."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_global(global_block: Any) -> None:
    """Validate the ``global`` block, raising ``PolicyValidationError`` on any mismatch."""
    if not isinstance(global_block, dict):
        raise PolicyValidationError(f"global must be a mapping, got {type(global_block).__name__}")

    default_alpha = global_block.get("default_alpha")
    if not _is_number(default_alpha) or not 0.0 < default_alpha <= 1.0:
        raise PolicyValidationError(
            f"global.default_alpha must be in (0.0, 1.0], got {default_alpha!r}"
        )

    fail_closed = global_block.get("fail_closed")
    if not isinstance(fail_closed, bool):
        raise PolicyValidationError(
            f"global.fail_closed must be a bool, got {type(fail_closed).__name__}"
        )

    exploration_rate = global_block.get("exploration_rate")
    if not _is_number(exploration_rate) or not 0.0 <= exploration_rate <= 1.0:
        raise PolicyValidationError(
            f"global.exploration_rate must be in [0.0, 1.0], got {exploration_rate!r}"
        )

    min_sample_size_default = global_block.get("min_sample_size_default")
    if (
        isinstance(min_sample_size_default, bool)
        or not isinstance(min_sample_size_default, int)
        or min_sample_size_default < 1
    ):
        raise PolicyValidationError(
            "global.min_sample_size_default must be an int >= 1, "
            f"got {min_sample_size_default!r}"
        )


def _validate_context_entry(entry: Any, index: int, seen: set[str]) -> None:
    """Validate one ``contexts[i]`` entry; ``seen`` tracks unique ``context_ref``s."""
    if not isinstance(entry, dict):
        raise PolicyValidationError(f"contexts[{index}] must be a mapping")

    context_ref = entry.get("context_ref")
    if not isinstance(context_ref, str) or not re.fullmatch(r"[0-9a-f]{32}", context_ref):
        raise PolicyValidationError(
            f"contexts[{index}].context_ref must be 32 hex chars, got {context_ref!r}"
        )
    if context_ref in seen:
        raise PolicyValidationError(
            f"contexts[{index}].context_ref {context_ref!r} appears more than once"
        )
    seen.add(context_ref)

    state = entry.get("state")
    if state not in _POLICY_STATES:
        raise PolicyValidationError(
            f"contexts[{index}].state must be one of {_POLICY_STATES}, got {state!r}"
        )

    q_hat = entry.get("q_hat")
    if q_hat is not None and (not _is_number(q_hat) or q_hat < 0.0):
        raise PolicyValidationError(
            f"contexts[{index}].q_hat must be a non-negative number, got {q_hat!r}"
        )
    if state == ACTIVE and q_hat is None:
        raise PolicyValidationError(f"contexts[{index}] is ACTIVE but has no q_hat")

    sample_size = entry.get("sample_size")
    if isinstance(sample_size, bool) or not isinstance(sample_size, int) or sample_size < 0:
        raise PolicyValidationError(
            f"contexts[{index}].sample_size must be an int >= 0, got {sample_size!r}"
        )

    min_sample_size = entry.get("min_sample_size")
    if (
        isinstance(min_sample_size, bool)
        or not isinstance(min_sample_size, int)
        or min_sample_size < 1
    ):
        raise PolicyValidationError(
            f"contexts[{index}].min_sample_size must be an int >= 1, " f"got {min_sample_size!r}"
        )
