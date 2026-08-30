"""Shared low-level utilities: context hashing, identifiers, timestamps,
logging configuration, and input validation.

Two hash entry points serve different jobs:

* :func:`context_hash` -- identifies a *serving context* for the gatekeeper
  (decision type plus prompt/weights/quantization factors). BLAKE2b, 16 bytes.
* :func:`make_context_hash` -- identifies a *model + task factor set* (for
  calibration provenance / cache keys). BLAKE3, 16 bytes, per the canonical
  spec.
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from typing import Callable, Optional, Protocol

CONTEXT_HASH_SIZE = 16

logger = logging.getLogger(__name__)


class _Blake3Hasher(Protocol):
    """The small surface of a BLAKE3 hasher used by :func:`make_context_hash`."""

    def digest(self, size: int = ...) -> bytes:
        """Return ``size`` digest bytes for the input fed to the hasher."""
        ...


def _default_blake3_factory() -> Callable[[bytes], _Blake3Hasher] | None:
    """Return a BLAKE3 constructor (stdlib or ``blake3`` package), or None."""
    factory: Callable[[bytes], _Blake3Hasher] | None = None
    try:  # CPython >= 3.13 with BLAKE3 enabled ships hashlib.blake3
        candidate = getattr(hashlib, "blake3", None)
        if candidate is None:  # fall back to the 'blake3' PyPI package
            from blake3 import blake3 as factory
        else:
            factory = candidate
    except ImportError:  # pragma: no cover - neither backend present
        factory = None
    return factory


_blake3_factory: Callable[[bytes], _Blake3Hasher] | None = _default_blake3_factory()


def now_ns() -> int:
    """Return current wall-clock time in nanoseconds since the epoch.

    Uses :func:`time.time_ns` (Python 3.7+). Nanosecond resolution lets
    decision records be ordered and correlated precisely with producer-side
    timestamps.

    Example::

        >>> now_ns()
        1700000000000000000

    Returns:
        int: nanoseconds since 1970-01-01T00:00:00Z.
    """
    return time.time_ns()


def now_us() -> int:
    """Return current wall-clock time in microseconds since the epoch.

    ``int(time.time() * 1e6)`` per the utility spec; useful sub-millisecond
    granularity for latency budgets in the 1-100us range.

    Example::

        >>> now_us()
        1700000000000

    Returns:
        int: microseconds since 1970-01-01T00:00:00Z.
    """
    return int(time.time() * 1e6)


def make_context_hash(
    model_id: str,
    task_type: str,
    prompt_template_version: str = "default",
    quantization: str = "int8",
    adapter_config: str = "",
) -> bytes:
    """Return a deterministic 128-bit (16-byte) hash of a model + task factor set.

    The same inputs produce the same digest on any machine (BLAKE3 is a
    salted-by-nothing cryptographic hash), and changing *any* factor yields a
    fresh context -- so calibration and caching keyed on this hash can never
    silently mix two different model/task configurations.

    Factor boundaries cannot collide: the tuple is ``repr()``-serialized, so
    ``("a", "bc")`` hashes differently from ``("ab", "c")``. A version prefix
    domain-separates this hash from the gatekeeper's ``context_hash``.

    Example::

        >>> make_context_hash("qwen-7b", "routing").hex()
        '1f810209b58ae19f185af097ca9cf646'

    Args:
        model_id: model name and version, e.g. ``"qwen-7b"``.
        task_type: control-plane task, e.g. ``"routing"``, ``"judge"``.
        prompt_template_version: version of the prompt template in use.
        quantization: (post-)training quantization, e.g. ``"int8"``, ``"fp16"``.
        adapter_config: LoRA/adapter identifier; ``""`` when none is applied.

    Returns:
        bytes: 16-byte BLAKE3 digest of the full context factor set.
    """
    factors = (
        model_id,
        task_type,
        prompt_template_version,
        quantization,
        adapter_config,
    )
    if _blake3_factory is None:  # pragma: no cover - dependency missing
        raise ImportError("context hashing requires BLAKE3 (pip install blake3)")
    payload = ("decision-ledger/context-v1\x00" + repr(factors)).encode("utf-8")
    return _blake3_factory(payload).digest(CONTEXT_HASH_SIZE)


def generate_uuidv7() -> str:
    """Return a time-ordered UUIDv7 as a canonical 36-character string.

    UUIDv7 sorts by creation time: the leading 48 bits are a millisecond
    timestamp, so batched identifiers preserve causal order, which the ledger
    relies on for deterministically replaying decisions in arrival order.

    Prefers the standard-library ``uuid.uuid7`` (Python 3.14+); falls back to
    the ``uuid6`` package on older interpreters; only if neither exists does it
    degrade to a UUIDv4 random string (still unique, no longer time-ordered),
    matching the task's ``uuid.uuid4().hex`` fallback intent while keeping the
    36-character contract.

    Example::

        >>> generate_uuidv7()
        '018d5c13-2b55-7120-a2c5-8d5c13b2f000'

    Returns:
        str: lowercase hyphenated UUID v7.
    """
    if hasattr(uuid, "uuid7"):  # Python >= 3.14
        return str(uuid.uuid7())
    try:
        import uuid6

        return str(uuid6.uuid7())
    except ImportError:  # pragma: no cover - degenerate fallback
        return str(uuid.uuid4())


def context_hash(
    decision_type: str,
    *,
    prompt_template: Optional[str] = None,
    model_id: Optional[str] = None,
    model_weights_sha256: Optional[str] = None,
    quantization_format: Optional[str] = None,
    adapter_config_hash: Optional[str] = None,
    temperature: Optional[float] = None,
) -> bytes:
    """Derive a 128-bit context reference for a gatekeeper serving context.

    Mirrors the production invariant: any change to the prompt template,
    model weights, quantization, adapter, temperature, or decision type
    produces a *new* context whose calibration starts from zero and the
    gatekeeper reverts to fail-closed escalation.

    Uses BLAKE2b truncated to 16 bytes (BLAKE3 is the production target;
    BLAKE2b gives the same 128-bit digest size without an extra dependency).
    """
    parts: list[bytes] = [
        decision_type.encode("utf-8"),
    ]
    for value in (
        prompt_template,
        model_id,
        model_weights_sha256,
        quantization_format,
        adapter_config_hash,
    ):
        parts.append(b"\x00" if value is None else value.encode("utf-8"))
    parts.append(b"none" if temperature is None else repr(float(temperature)).encode("utf-8"))

    digest = hashlib.blake2b(b"\x1f".join(parts), digest_size=CONTEXT_HASH_SIZE)
    return digest.digest()


def decision_id() -> str:
    """Generate a time-ordered UUID v7 identifier.

    Alias of :func:`generate_uuidv7` retained for backward compatibility with
    the gatekeeper/telemetry call sites. Returns the canonical 36-character
    lowercase hyphenated form.
    """
    return generate_uuidv7()


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure the ``decision_ledger`` logger and return it.

    Console output goes to stderr via a ``StreamHandler``; when ``log_file``
    is given, the same formatted records are appended to that file. Repeated
    calls are idempotent: previously attached handlers are closed and removed
    first, so handlers never stack. Records are emitted exactly once (the
    logger does not propagate to the root logger once configured here).

    Format: ``[%(asctime)s] [%(levelname)s] %(message)s``

    Args:
        log_level: level name (``"DEBUG"``, ``"INFO"``, ...) or numeric level.
        log_file: optional path to also append records to.

    Returns:
        The configured ``decision_ledger`` logger.
    """
    if isinstance(log_level, str):
        level = getattr(logging, log_level.upper(), None)
        if not isinstance(level, int):
            level = logging.INFO
    else:
        level = int(log_level)

    log = logging.getLogger("decision_ledger")
    log.setLevel(level)
    for handler in list(log.handlers):
        handler.close()
        log.removeHandler(handler)

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    log.addHandler(console)
    if log_file is not None:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)
    log.propagate = False
    return log


def validate_confidence(confidence: float) -> float:
    """Validate and clamp a model confidence to the unit interval.

    Non-numeric inputs (and ``bool``, which is an ``int`` subclass but never a
    model confidence) raise :class:`TypeError`. Out-of-range values are clamped
    to ``[0.0, 1.0]`` with a warning, so a buggy scorer degrades to the safest
    boundary instead of producing an impossible non-conformity score.

    Example::

        >>> validate_confidence(1.2)
        1.0
        >>> validate_confidence(0.5)
        0.5

    Args:
        confidence: raw model confidence in any sign/scale.

    Returns:
        float: ``confidence`` clamped to ``[0.0, 1.0]``.

    Raises:
        TypeError: if ``confidence`` is not a real number.
    """
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
        raise TypeError(f"confidence must be a real number, got {type(confidence).__name__}")
    value = float(confidence)
    if not 0.0 <= value <= 1.0:
        clamped = min(1.0, max(0.0, value))
        logger.warning("confidence %r out of [0, 1]; clamped to %.4f", confidence, clamped)
        return clamped
    return value


def validate_context_hash(context_hash: bytes) -> bool:
    """Return ``True`` only when ``context_hash`` is exactly 16 bytes (128 bits).

    Context hashes are the cache/calibration keys of the ledger; anything
    shorter or longer would silently partition the space wrong or collide
    trivially, so the length is checked exactly. Non-bytes inputs (``str``,
    ``None``, ...) are rejected rather than coerced.

    Example::

        >>> validate_context_hash(b"\\x00" * 16)
        True
        >>> validate_context_hash(b"\\x00" * 15)
        False

    Args:
        context_hash: candidate hash value.

    Returns:
        bool: whether the value is a valid 16-byte context hash.
    """
    return isinstance(context_hash, (bytes, bytearray)) and len(context_hash) == CONTEXT_HASH_SIZE


def hex_hash(data: bytes) -> str:
    """Hex-encode a context hash for display and YAML keys."""
    return data.hex()


def unhex_hash(value: str) -> bytes:
    """Decode a hex-encoded context hash."""
    return bytes.fromhex(value)
