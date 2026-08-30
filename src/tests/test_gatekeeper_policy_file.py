"""Gatekeeper policy-file loading, conversion, and reloading."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from decision_ledger import (
    GateAction,
    Gatekeeper,
    PolicyGenerator,
    PolicyValidationError,
)
from decision_ledger.calibration import CalibrationResult
from decision_ledger.gatekeeper import CalibrationContext

CTX_A = bytes(range(16))
CTX_B = bytes.fromhex("11" * 16)
CTX_C = bytes.fromhex("22" * 16)


def _result(q_hat=None, sample_size=500) -> CalibrationResult:
    return CalibrationResult(
        q_hat=q_hat,
        sample_size=sample_size,
        coverage_lower_bound=0.95,
        achieved_empirical_risk=0.05,
    )


def _artifact(
    directory: Path,
    *,
    policy_version: str = "20260830-120000",
    min_sample_size_default: int = 100,
    revoked=frozenset(),
) -> Path:
    generator = PolicyGenerator(
        directory,
        min_sample_size_default=min_sample_size_default,
        revoked_contexts=revoked,
    )
    results = {
        CTX_A: _result(q_hat=0.05, sample_size=500),
        CTX_B: _result(q_hat=None, sample_size=10),
        CTX_C: _result(q_hat=0.05, sample_size=500),
    }
    return Path(generator.generate_policy(results, policy_version=policy_version))


def test_from_policy_file_loads_and_enforces(tmp_path: Path) -> None:
    path = _artifact(tmp_path, revoked=frozenset({CTX_C}))
    gatekeeper = Gatekeeper.from_policy_file(str(path), exploration_rate=0.0)

    assert set(gatekeeper.policy) == {CTX_A, CTX_B, CTX_C}
    assert gatekeeper.policy[CTX_A].context_hash == CTX_A
    assert gatekeeper.policy[CTX_A].is_active
    assert gatekeeper.policy[CTX_A].q_hat == 0.05
    assert gatekeeper.policy[CTX_A].current_sample_size == 500
    assert gatekeeper.policy[CTX_A].min_sample_size == 100  # global default
    assert not gatekeeper.policy[CTX_B].is_active  # DRAINING
    assert not gatekeeper.policy[CTX_C].is_active  # REVOKED stays inactive
    assert gatekeeper._policy_version == "20260830-120000"

    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.DELEGATE
    assert gatekeeper.evaluate(CTX_A, 0.10, "route") == GateAction.ESCALATE
    assert gatekeeper.evaluate(CTX_B, 0.99, "route") == GateAction.ESCALATE
    assert gatekeeper.evaluate(CTX_C, 0.99, "route") == GateAction.ESCALATE


def test_constructor_accepts_policy_file_keyword(tmp_path: Path) -> None:
    path = _artifact(tmp_path)
    gatekeeper = Gatekeeper(policy_file=str(path), exploration_rate=0.0)
    assert set(gatekeeper.policy) == {CTX_A, CTX_B, CTX_C}


def test_policy_file_takes_precedence_over_policy_dict(tmp_path: Path) -> None:
    path = _artifact(tmp_path)
    gatekeeper = Gatekeeper(
        policy={CTX_A: CalibrationContext(CTX_A, q_hat=0.99, is_active=True)},
        policy_file=str(path),
        exploration_rate=0.0,
    )
    assert set(gatekeeper.policy) == {CTX_A, CTX_B, CTX_C}
    assert gatekeeper.policy[CTX_A].q_hat == 0.05


def test_policy_dict_used_directly() -> None:
    policy = {CTX_A: CalibrationContext(CTX_A, q_hat=0.05, current_sample_size=500, is_active=True)}
    gatekeeper = Gatekeeper(policy, exploration_rate=0.0)
    assert gatekeeper.policy is policy
    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.DELEGATE


def test_empty_policy_fails_closed() -> None:
    gatekeeper = Gatekeeper(exploration_rate=0.0)
    assert gatekeeper.policy == {}
    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.ESCALATE


def test_convert_policy_dict_to_contexts_maps_entries(tmp_path: Path) -> None:
    path = _artifact(tmp_path, min_sample_size_default=250)
    from decision_ledger.policy import load_policy

    contexts = Gatekeeper.convert_policy_dict_to_contexts(load_policy(str(path)))

    assert contexts[CTX_A].min_sample_size == 250
    assert contexts[CTX_A].current_sample_size == 500
    assert contexts[CTX_A].q_hat == 0.05
    assert contexts[CTX_A].is_active
    assert contexts[CTX_A].context_hash == CTX_A


def test_convert_policy_dict_to_contexts_handles_entry_without_q_hat() -> None:
    policy_dict = {
        "schema_version": "1.0",
        "policy_version": "20260830-120000",
        "generated_at": "2026-08-30T12:00:00Z",
        "global": {
            "default_alpha": 0.05,
            "fail_closed": True,
            "exploration_rate": 0.02,
            "min_sample_size_default": 250,
        },
        "contexts": [
            {
                "context_ref": CTX_B.hex(),
                "state": "DRAINING",
                "sample_size": 9,
                "min_sample_size": 0,
            }
        ],
    }
    contexts = Gatekeeper.convert_policy_dict_to_contexts(policy_dict)
    assert contexts[CTX_B].q_hat is None
    assert contexts[CTX_B].current_sample_size == 9
    assert contexts[CTX_B].min_sample_size == 250
    assert not contexts[CTX_B].is_active


def test_reload_policy_from_file_swaps_behavior(tmp_path: Path) -> None:
    first = _artifact(tmp_path / "first", policy_version="20260830-010000")
    second = _artifact(tmp_path / "second", policy_version="20260830-020000")

    gatekeeper = Gatekeeper.from_policy_file(str(first), exploration_rate=0.0)
    assert gatekeeper.evaluate(CTX_A, 0.98, "route") == GateAction.DELEGATE

    gatekeeper.reload_policy_from_file(str(second))
    assert gatekeeper.policy[CTX_A].q_hat == 0.05
    assert gatekeeper.evaluate(CTX_A, 0.98, "route") == GateAction.DELEGATE
    assert gatekeeper._policy_version == "20260830-020000"


def test_reload_policy_from_file_logs_version_transition(tmp_path: Path, caplog) -> None:
    first = _artifact(tmp_path / "first", policy_version="20260830-010000")
    gatekeeper = Gatekeeper(policy_file=str(first), exploration_rate=0.0)

    second = _artifact(tmp_path / "second", policy_version="20260830-020000")
    with caplog.at_level(logging.INFO, logger="decision_ledger.gatekeeper"):
        gatekeeper.reload_policy_from_file(str(second))

    assert "[Reloaded policy: 20260830-010000 -> 20260830-020000, contexts=3]" in caplog.text


def test_reload_policy_from_file_logs_none_when_version_unknown(tmp_path: Path, caplog) -> None:
    gatekeeper = Gatekeeper({CTX_A: _result_confident()}, exploration_rate=0.0)
    path = _artifact(tmp_path, policy_version="20260830-030000")
    with caplog.at_level(logging.INFO, logger="decision_ledger.gatekeeper"):
        gatekeeper.reload_policy_from_file(str(path))

    assert "[Reloaded policy: None -> 20260830-030000" in caplog.text


def test_init_with_policy_file_logs_load(tmp_path: Path, caplog) -> None:
    path = _artifact(tmp_path, policy_version="20260830-040000")
    with caplog.at_level(logging.INFO, logger="decision_ledger.gatekeeper"):
        Gatekeeper(policy_file=str(path), exploration_rate=0.0)

    assert "[Loaded policy: version=20260830-040000, contexts=3]" in caplog.text


def test_missing_policy_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Gatekeeper.from_policy_file(str(tmp_path / "missing.yaml"))
    with pytest.raises(FileNotFoundError):
        Gatekeeper(policy_file=str(tmp_path / "missing.yaml"))


def test_invalid_policy_file_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(PolicyValidationError):
        Gatekeeper.from_policy_file(str(bad))
    with pytest.raises(PolicyValidationError):
        Gatekeeper(policy_file=str(bad))


def test_reload_from_missing_file_raises(tmp_path: Path) -> None:
    path = _artifact(tmp_path, policy_version="20260830-050000")
    gatekeeper = Gatekeeper(policy_file=str(path), exploration_rate=0.0)
    with pytest.raises(FileNotFoundError):
        gatekeeper.reload_policy_from_file(str(tmp_path / "nope.yaml"))


def _result_confident() -> CalibrationContext:
    return CalibrationContext(CTX_A, q_hat=0.05, current_sample_size=500, is_active=True)
