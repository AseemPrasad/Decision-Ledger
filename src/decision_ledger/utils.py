"""Shared low-level utilities: context hashing, timestamps, identifiers."""

from __future__ import annotations

import hashlib
import os
import time
import uuid
from typing import Optional

CONTEXT_HASH_SIZE = 16

_UUID_V7_VERSION = 0x70
_UUID_VARIANT = 0x80


def now_ns() -> int:
    """Monotonic-epoch wall-clock nanoseconds (system clock)."""
    return time.time_ns()


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
    """Derive a 128-bit context reference.

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
    parts.append(
        b"none" if temperature is None else repr(float(temperature)).encode("utf-8")
    )

    digest = hashlib.blake2b(b"\x1f".join(parts), digest_size=CONTEXT_HASH_SIZE)
    return digest.digest()


def decision_id() -> str:
    """Generate a time-ordered UUID v7 identifier.

    Returns the canonical 36-character lowercase hyphenated form. The first
    six bytes are a 48-bit millisecond timestamp so IDs sort by creation time;
    the remaining bytes carry the version nibble, variant bits, and random
    entropy from the OS CSPRNG.
    """
    millis = int(time.time() * 1000)
    rand = bytearray(os.urandom(10))

    identifier = bytearray(16)
    identifier[0:6] = millis.to_bytes(6, "big")
    identifier[6] = _UUID_V7_VERSION | (rand[0] & 0x0F)
    identifier[8] = _UUID_VARIANT | (rand[1] & 0x3F)
    identifier[7] = rand[2]
    identifier[9:16] = rand[3:10]
    return str(uuid.UUID(bytes=bytes(identifier)))


def hex_hash(data: bytes) -> str:
    """Hex-encode a context hash for display and YAML keys."""
    return data.hex()


def unhex_hash(value: str) -> bytes:
    """Decode a hex-encoded context hash."""
    return bytes.fromhex(value)
