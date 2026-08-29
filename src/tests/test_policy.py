"""Tests for the policy artifact generator and lifecycle manager."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

from decision_ledger import (
    PolicyError,
    PolicyGenerator,
    PolicyValidationError,
)
from decision_ledger.calibration import CalibrationResult
from decision_ledger.gatekeeper import GateAction, Gatekeeper
from decision_ledger.policy import (
    ACTIVE,
    DRAINING,
    POLICY_SCHEMA_VERSION,
    REVOKED,
    load_policy,
    policy_from_dict,
    policy_from_results,
    save_policy,
    validate_policy,
)

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


def _generator(tmp_path: Path, **kwargs) -> PolicyGenerator:
    return PolicyGenerator(policies_dir=tmp_path, **kwargs)


def _artifact(**overrides) -> dict:
    data = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_version": "20260830-120000",
        "generated_at": "2026-08-30T12:00:00Z",
        "global": {
            "default_alpha": 0.05,
            "fail_closed": True,
            "exploration_rate": 0.02,
            "min_sample_size_default": 100,
        },
        "contexts": [
            {
                "context_ref": CTX_A.hex(),
                "state": ACTIVE,
                "q_hat": 0.05,
                "sample_size": 500,
                "min_sample_size": 100,
            }
        ],
    }
    data.update(overrides)
    return data


def test_generate_policy_writes_schema_1_0_artifact(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    path = generator.generate_policy(
        {
            CTX_A: _result(q_hat=0.05, sample_size=500),
            CTX_B: _result(q_hat=None, sample_size=10),
        }
    )

    policy_file = Path(path)
    assert policy_file.is_file()
    assert re.fullmatch(
        rf"policy_\d{{8}}-\d{{6}}\.yaml", policy_file.name
    ), policy_file.name
    assert policy_file.parent == tmp_path

    artifact = load_policy(policy_file)
    assert artifact["schema_version"] == POLICY_SCHEMA_VERSION
    assert re.fullmatch(r"\d{8}-\d{6}", artifact["policy_version"])
    assert artifact["generated_at"].endswith("Z")
    assert set(artifact["global"]) == {
        "default_alpha",
        "fail_closed",
        "exploration_rate",
        "min_sample_size_default",
    }
    assert artifact["global"]["min_sample_size_default"] == 100

    by_ref = {entry["context_ref"]: entry for entry in artifact["contexts"]}
    assert set(by_ref) == {CTX_A.hex(), CTX_B.hex()}
    assert by_ref[CTX_A.hex()]["state"] == ACTIVE
    assert by_ref[CTX_A.hex()]["q_hat"] == 0.05
    assert by_ref[CTX_B.hex()]["state"] == DRAINING
    assert "q_hat" not in by_ref[CTX_B.hex()]
    assert by_ref[CTX_B.hex()]["sample_size"] == 10


def test_generate_policy_classifies_states(tmp_path: Path) -> None:
    generator = _generator(tmp_path, revoked_contexts=frozenset({CTX_C}))
    results = {
        CTX_A: _result(q_hat=0.0, sample_size=500),  # threshold zero still counts
        CTX_B: _result(q_hat=0.05, sample_size=1),  # q_hat set, below min sample
        CTX_C: _result(q_hat=0.05, sample_size=500),  # revoked overrides ACTIVE
    }
    artifact = load_policy(generator.generate_policy(results))

    by_ref = {entry["context_ref"]: entry for entry in artifact["contexts"]}
    assert by_ref[CTX_A.hex()]["state"] == ACTIVE
    assert by_ref[CTX_A.hex()]["q_hat"] == 0.0
    assert by_ref[CTX_B.hex()]["state"] == DRAINING
    assert by_ref[CTX_B.hex()]["q_hat"] == 0.05  # recorded, but not served
    assert by_ref[CTX_C.hex()]["state"] == REVOKED


def test_generate_policy_explicit_version_and_force(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    results = {CTX_A: _result(q_hat=0.05, sample_size=500)}

    first = generator.generate_policy(results, policy_version="20260830-120000")
    assert Path(first).name == "policy_20260830-120000.yaml"

    with pytest.raises(PolicyError, match="already exists"):
        generator.generate_policy(results, policy_version="20260830-120000")

    overwritten = generator.generate_policy(
        results, policy_version="20260830-120000", force=True
    )
    assert overwritten == first
    assert load_policy(overwritten)["policy_version"] == "20260830-120000"


def test_generate_policy_invalid_version_raises(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    with pytest.raises(PolicyValidationError, match="expected YYYYMMdd-HHMMSS"):
        generator.generate_policy(
            {CTX_A: _result(q_hat=0.05, sample_size=500)},
            policy_version="not-a-version",
        )


def test_generate_policy_publishes_latest(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    path = generator.generate_policy(
        {CTX_A: _result(q_hat=0.05, sample_size=500)},
        policy_version="20260830-120000",
    )

    latest = tmp_path / "policy_latest.yaml"
    assert latest.exists()
    if latest.is_symlink():
        assert str(latest.resolve()) == str(Path(path).resolve())

    assert generator.load_latest_policy()["policy_version"] == "20260830-120000"
    assert generator.load_latest_policy() == load_policy(path)


def test_load_latest_policy_missing_raises(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    with pytest.raises(PolicyError, match="no latest policy"):
        generator.load_latest_policy()


def test_get_policy_history_newest_first(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    results = {CTX_A: _result(q_hat=0.05, sample_size=500)}
    generator.generate_policy(results, policy_version="20260801-010000")
    generator.generate_policy(results, policy_version="20260815-020000")

    assert generator.get_policy_history() == [
        "20260815-020000",
        "20260801-010000",
    ]
    assert tmp_path / "policy_latest.yaml" in set(tmp_path.iterdir())


def test_get_policy_history_empty_dir(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    assert generator.get_policy_history() == []


def test_get_policy_history_nonexistent_dir(tmp_path: Path) -> None:
    generator = PolicyGenerator(policies_dir=tmp_path / "nope")
    assert generator.get_policy_history() == []


def test_latest_link_falls_back_when_symlink_disallowed(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("decision_ledger.policy.os.symlink", _deny_symlink)
    generator = _generator(tmp_path)
    generator.generate_policy(
        {CTX_A: _result(q_hat=0.05, sample_size=500)},
        policy_version="20260830-120000",
    )
    assert generator.load_latest_policy()["policy_version"] == "20260830-120000"


def _deny_symlink(*args, **kwargs) -> None:
    raise OSError("symlinks not permitted")


def test_rollback_policy_republishes(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    results = {CTX_A: _result(q_hat=0.05, sample_size=500)}
    generator.generate_policy(results, policy_version="20260801-010000")
    generator.generate_policy(results, policy_version="20260830-030000")

    rolled_back = generator.rollback_policy("20260801-010000")
    assert rolled_back == str(tmp_path / "policy_20260801-010000.yaml")
    assert generator.load_latest_policy()["policy_version"] == "20260801-010000"
    assert generator.get_policy_history() == [
        "20260830-030000",
        "20260801-010000",
    ]


def test_rollback_policy_missing_version_raises(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    with pytest.raises(PolicyError, match="not found"):
        generator.rollback_policy("20260830-030000")
    with pytest.raises(PolicyValidationError):
        generator.rollback_policy("bogus")


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda d: d.update({"schema_version": "9.9"}), "unsupported schema_version"),
        (
            lambda d: d.update({"policy_version": "2026-08-30"}),
            "invalid policy_version",
        ),
        (lambda d: d.update({"generated_at": "yesterday"}), "invalid generated_at"),
        (lambda d: d.update({"contexts": "nope"}), "contexts must be a list"),
        (
            lambda d: d["global"].pop("fail_closed"),
            "global.fail_closed must be a bool",
        ),
        (
            lambda d: d["global"].update({"default_alpha": 2.0}),
            "global.default_alpha must be in",
        ),
        (
            lambda d: d["global"].update({"min_sample_size_default": 0}),
            "global.min_sample_size_default must be an int >= 1",
        ),
        (
            lambda d: d.update({"global": []}),
            "global must be a mapping",
        ),
        (
            lambda d: d.update({"generated_at": "not-a-dateZ"}),
            "invalid generated_at",
        ),
        (
            lambda d: d.update({"contexts": ["oops"]}),
            r"contexts\[0\] must be a mapping",
        ),
        (
            lambda d: d["contexts"][0].update({"q_hat": -0.1}),
            "q_hat must be a non-negative number",
        ),
        (
            lambda d: d["global"].update({"exploration_rate": 3.0}),
            "global.exploration_rate must be in",
        ),
        (
            lambda d: d["contexts"][0].update({"state": "FROZEN"}),
            "state must be one of",
        ),
        (
            lambda d: d["contexts"][0].pop("q_hat"),
            "ACTIVE but has no q_hat",
        ),
        (
            lambda d: d["contexts"][0].update({"context_ref": "xyz"}),
            "context_ref must be 32 hex chars",
        ),
        (
            lambda d: d["contexts"][0].update({"sample_size": -1}),
            "sample_size must be an int >= 0",
        ),
        (
            lambda d: d["contexts"][0].update({"min_sample_size": 0}),
            "min_sample_size must be an int >= 1",
        ),
    ],
)
def test_validate_policy_rejects_invalid(mutate, message, tmp_path: Path) -> None:
    data = _artifact()
    mutate(data)
    with pytest.raises(PolicyValidationError, match=message):
        validate_policy(data)


def test_validate_policy_rejects_duplicate_context_ref(tmp_path: Path) -> None:
    data = _artifact()
    data["contexts"].append(dict(data["contexts"][0]))
    with pytest.raises(PolicyValidationError, match="more than once"):
        validate_policy(data)


def test_validate_policy_rejects_non_mapping() -> None:
    with pytest.raises(PolicyValidationError, match="must be a mapping"):
        validate_policy(["not", "a", "dict"])


def test_validate_policy_accepts_valid(tmp_path: Path) -> None:
    assert validate_policy(_artifact()) is True


def test_to_dict_rejects_invalid_policy_version() -> None:
    from decision_ledger.policy import to_dict

    policy = policy_from_results(
        {CTX_A: _result(q_hat=0.05, sample_size=500)}, version_id=1
    )
    with pytest.raises(PolicyValidationError, match="expected YYYYMMdd-HHMMSS"):
        to_dict(policy, policy_version="nope")


def test_generate_policy_rejects_invalid_context_hash(tmp_path: Path) -> None:
    generator = _generator(tmp_path)
    with pytest.raises(PolicyValidationError, match="must be 16 bytes"):
        generator.generate_policy({b"short": _result(q_hat=0.05, sample_size=500)})


def test_policy_from_dict_feeds_gatekeeper(tmp_path: Path) -> None:
    generator = _generator(tmp_path, revoked_contexts=frozenset({CTX_C}))
    results = {
        CTX_A: _result(q_hat=0.05, sample_size=500),
        CTX_B: _result(q_hat=0.05, sample_size=5),
        CTX_C: _result(q_hat=0.05, sample_size=500),
    }
    path = generator.generate_policy(results, policy_version="20260830-120000")

    serving = policy_from_dict(load_policy(path))
    assert set(serving.contexts) == {CTX_A, CTX_B}
    assert serving.contexts[CTX_A].is_active
    assert serving.contexts[CTX_A].q_hat == 0.05
    assert serving.contexts[CTX_A].current_sample_size == 500
    assert not serving.contexts[CTX_B].is_active

    gatekeeper = Gatekeeper(serving.contexts, exploration_rate=0.0)
    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.DELEGATE
    assert gatekeeper.evaluate(CTX_A, 0.10, "route") == GateAction.ESCALATE
    assert gatekeeper.evaluate(CTX_B, 0.99, "route") == GateAction.ESCALATE
    assert gatekeeper.evaluate(CTX_C, 0.99, "route") == GateAction.ESCALATE


def test_save_then_load_round_trip(tmp_path: Path) -> None:
    results = {
        CTX_A: _result(q_hat=0.05, sample_size=500),
        CTX_B: _result(q_hat=None, sample_size=10),
    }
    policy = policy_from_results(results, version_id=2, min_sample_size=100)
    path = save_policy(policy, tmp_path / "policies" / "policy-v2.yaml")
    assert path.exists()

    artifact = load_policy(path)
    assert artifact["schema_version"] == POLICY_SCHEMA_VERSION
    assert validate_policy(artifact) is True

    reloaded = policy_from_dict(artifact)
    assert set(reloaded.contexts) == {CTX_A, CTX_B}
    gatekeeper = Gatekeeper(reloaded.contexts, exploration_rate=0.0)
    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.DELEGATE
    assert gatekeeper.evaluate(CTX_B, 0.99, "route") == GateAction.ESCALATE


def test_serving_policy_is_snapshot_plus_generator_sources_agree(
    tmp_path: Path,
) -> None:
    results = {CTX_A: _result(q_hat=0.05, sample_size=500)}
    legacy = policy_from_results(results, version_id=1, min_sample_size=100)
    generated = _generator(tmp_path).generate_policy(results)

    legacy_entries = {c.context_hash.hex(): c for c in legacy.contexts.values()}
    artifact_entries = {
        entry["context_ref"]: entry for entry in load_policy(generated)["contexts"]
    }
    assert legacy_entries[CTX_A.hex()].q_hat == artifact_entries[CTX_A.hex()]["q_hat"]
    assert artifact_entries[CTX_A.hex()]["state"] == ACTIVE


def test_revocation_is_external_and_reversible(tmp_path: Path) -> None:
    results = {CTX_A: _result(q_hat=0.05, sample_size=500)}

    revoked_generator = _generator(tmp_path, revoked_contexts=frozenset({CTX_A}))
    first = load_policy(revoked_generator.generate_policy(results))
    assert first["contexts"][0]["state"] == REVOKED

    fresh_generator = _generator(tmp_path, revoked_contexts=frozenset())
    second = load_policy(
        fresh_generator.generate_policy(
            results, policy_version="20260830-120000", force=True
        )
    )
    assert second["contexts"][0]["state"] == ACTIVE


def test_generate_policy_logs_message(tmp_path: Path, caplog) -> None:
    generator = _generator(tmp_path)
    with caplog.at_level(logging.INFO, logger="decision_ledger.policy"):
        path = generator.generate_policy({CTX_A: _result(q_hat=0.05, sample_size=500)})
    assert f"[Generated policy: {path}]" in caplog.text


def test_policy_generator_validates_constructor_params(tmp_path: Path) -> None:
    with pytest.raises(PolicyValidationError, match="default_alpha"):
        PolicyGenerator(tmp_path, default_alpha=0.0)
    with pytest.raises(PolicyValidationError, match="exploration_rate"):
        PolicyGenerator(tmp_path, exploration_rate=1.5)
    with pytest.raises(PolicyValidationError, match="min_sample_size_default"):
        PolicyGenerator(tmp_path, min_sample_size_default=0)
    with pytest.raises(PolicyValidationError, match="not a valid 16-byte hash"):
        PolicyGenerator(tmp_path, revoked_contexts=frozenset({b"short"}))


def test_load_policy_rejects_non_yaml_mapping(tmp_path: Path) -> None:
    file_path = tmp_path / "bad.yaml"
    file_path.write_text("- just\n- a\n- list\n", encoding="utf-8")
    with pytest.raises(PolicyValidationError, match="YAML mapping"):
        load_policy(file_path)


def test_policy_types_exported_from_package_root() -> None:
    from decision_ledger import load_policy as root_load_policy
    from decision_ledger import policy_from_dict as root_policy_from_dict

    assert root_load_policy is load_policy
    assert root_policy_from_dict is policy_from_dict
