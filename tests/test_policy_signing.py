"""Unit & Integration tests for Ed25519 Cryptographic Policy Signing & Verification."""

from __future__ import annotations

import tempfile
import pytest
from pathlib import Path

from decision_ledger.calibration import CalibrationResult
from decision_ledger.gatekeeper import Gatekeeper
from decision_ledger.policy import (
    PolicyGenerator,
    PolicyValidationError,
    generate_ed25519_key_pair,
    load_policy,
    sign_policy_dict,
    verify_policy_signature,
)
from decision_ledger.utils import make_context_hash


def test_ed25519_key_generation_and_sign_verify_roundtrip():
    """Test Ed25519 key generation and signature roundtrip."""
    priv_key, pub_key = generate_ed25519_key_pair()
    assert len(priv_key) == 64  # 32 bytes hex
    assert len(pub_key) == 64

    data = {
        "schema_version": "1.0",
        "policy_version": "20260830-120000",
        "global": {"default_alpha": 0.05},
    }

    signed = sign_policy_dict(data, priv_key)
    assert "signature" in signed
    assert signed["signature"]["algorithm"] == "Ed25519"
    assert signed["signature"]["public_key"] == pub_key

    # Verification with correct key
    assert verify_policy_signature(signed, pub_key) is True


def test_signed_policy_generator_and_gatekeeper_reload(tmp_path):
    """Test PolicyGenerator producing signed policy and Gatekeeper verifying it."""
    priv_key, pub_key = generate_ed25519_key_pair()
    generator = PolicyGenerator(tmp_path)

    ctx = make_context_hash("qwen-7b", "routing")
    results = {
        ctx: CalibrationResult(
            q_hat=0.08,
            sample_size=200,
        )
    }

    artifact_path = generator.generate_policy(
        results,
        policy_version="20260830-130000",
        private_key_hex=priv_key,
    )

    # Gatekeeper loads with matching public key
    gk = Gatekeeper.from_policy_file(artifact_path, public_key=pub_key)
    assert gk._policy_version == "20260830-130000"
    assert ctx in gk.policy


def test_tampered_policy_signature_rejection(tmp_path):
    """Test that modifying a signed policy artifact causes verification to fail."""
    priv_key, pub_key = generate_ed25519_key_pair()
    generator = PolicyGenerator(tmp_path)

    ctx = make_context_hash("qwen-7b", "routing")
    results = {
        ctx: CalibrationResult(
            q_hat=0.08,
            sample_size=200,
        )
    }

    artifact_path = generator.generate_policy(
        results,
        policy_version="20260830-140000",
        private_key_hex=priv_key,
    )

    # Tamper with the artifact file (change q_hat from 0.08 to 0.99)
    policy_data = load_policy(artifact_path)
    policy_data["contexts"][0]["q_hat"] = 0.99

    with pytest.raises(PolicyValidationError, match="Cryptographic signature verification failed"):
        verify_policy_signature(policy_data, pub_key)


def test_wrong_public_key_rejection():
    """Test verification failure when verifying with an unmatching public key."""
    priv_key1, pub_key1 = generate_ed25519_key_pair()
    _, pub_key2 = generate_ed25519_key_pair()

    data = {"schema_version": "1.0", "policy_version": "20260830-150000"}
    signed = sign_policy_dict(data, priv_key1)

    with pytest.raises(PolicyValidationError, match="Cryptographic signature verification failed"):
        verify_policy_signature(signed, pub_key2)
