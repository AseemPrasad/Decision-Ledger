"""Tests for the end-to-end calibration pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

import pytest

from decision_ledger import (
    CalibrationPipeline,
    Database,
    GateAction,
    Gatekeeper,
    PolicyGenerator,
)
from decision_ledger.policy import load_policy

CTX_A = bytes(range(16))
CTX_B = bytes.fromhex("11" * 16)


def decision_row(decision_id: str, *, context: bytes = CTX_A) -> dict:
    return {
        "decision_id": decision_id,
        "timestamp_ns": 1,
        "context_hash": context,
        "decision_type": "route",
        "model_confidence": 0.9,
        "non_conformity": 0.1,
        "action_taken": "DELEGATE",
        "latency_us": 5,
    }


def joined_row(
    decision_id: str,
    *,
    score: float,
    outcome_value: float,
    action: str = "DELEGATE",
    context: bytes = CTX_A,
) -> dict:
    return {
        "joined_id": decision_id,
        "decision_id": decision_id,
        "context_hash": context,
        "decision_type": "route",
        "model_confidence": 1.0 - score,
        "non_conformity": score,
        "action_taken": action,
        "outcome_value": outcome_value,
        "decision_timestamp_ns": 1,
        "outcome_timestamp_ns": 2,
        "latency_delta_ns": 1,
    }


def outcome_row(decision_id: str) -> dict:
    return {
        "outcome_id": f"o-{decision_id}",
        "decision_id": decision_id,
        "timestamp_ns": 2,
        "outcome_value": 1.0,
        "outcome_source": "task_metric",
        "metadata": None,
    }


def seed_context(
    database: Database,
    pairs: list[tuple[str, float, float, str]],
    *,
    context: bytes = CTX_A,
) -> None:
    """Seed decisions, outcomes and joined_records for the pairs."""
    database.batch_insert(
        "decisions",
        [decision_row(decision_id, context=context) for decision_id, *_ in pairs],
    )
    for decision_id, score, value, action in pairs:
        database.batch_insert(
            "joined_records",
            [
                joined_row(
                    decision_id,
                    score=score,
                    outcome_value=value,
                    action=action,
                    context=context,
                )
            ],
        )
        database.batch_insert("outcomes", [outcome_row(decision_id)])


def successful_set(n: int, decision_prefix: str) -> list[tuple[str, float, float, str]]:
    return [(f"{decision_prefix}-{i}", 0.05, 1.0, "DELEGATE") for i in range(n)]


def exploratory_set(
    n: int, decision_prefix: str
) -> list[tuple[str, float, float, str]]:
    return [(f"{decision_prefix}-e{i}", 0.05, 1.0, "EXPLORE_SHADOW") for i in range(n)]


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    database = Database(str(tmp_path / "ledger.db"))
    yield database
    database.close()


@pytest.fixture
def generator(tmp_path: Path) -> PolicyGenerator:
    return PolicyGenerator(str(tmp_path / "policies"), min_sample_size_default=10)


@pytest.fixture
def gatekeeper() -> Gatekeeper:
    return Gatekeeper(exploration_rate=0.0)


def test_constructor_validation(db: Database, generator: PolicyGenerator) -> None:
    with pytest.raises(ValueError):
        CalibrationPipeline(
            db, Gatekeeper(exploration_rate=0.0), generator, target_alpha=0.0
        )
    with pytest.raises(ValueError):
        CalibrationPipeline(
            db, Gatekeeper(exploration_rate=0.0), generator, target_alpha=1.0
        )


def test_calibrator_wired_to_generator_settings(
    db: Database, generator: PolicyGenerator, gatekeeper: Gatekeeper
) -> None:
    pipeline = CalibrationPipeline(db, gatekeeper, generator)
    assert pipeline.target_alpha == 0.05
    assert pipeline.calibrator.min_sample_size == generator.min_sample_size_default


def test_run_calibration_generates_and_reloads_policy(
    db: Database, generator: PolicyGenerator, gatekeeper: Gatekeeper, capsys
) -> None:
    seed_context(db, successful_set(15, "a") + exploratory_set(15, "a"))
    seed_context(db, successful_set(5, "b") + exploratory_set(5, "b"), context=CTX_B)

    pipeline = CalibrationPipeline(db, gatekeeper, generator)
    policy_file = pipeline.run_calibration()

    artifact_path = Path(policy_file)
    assert artifact_path.exists()
    artifact = load_policy(policy_file)
    assert gatekeeper._policy_version == artifact["policy_version"]

    assert set(gatekeeper.policy) == {CTX_A, CTX_B}
    assert gatekeeper.policy[CTX_A].is_active
    assert gatekeeper.policy[CTX_A].q_hat == 0.05
    assert not gatekeeper.policy[CTX_B].is_active
    assert gatekeeper.policy[CTX_B].q_hat is None

    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.DELEGATE
    assert gatekeeper.evaluate(CTX_A, 0.10, "route") == GateAction.ESCALATE
    assert gatekeeper.evaluate(CTX_B, 0.99, "route") == GateAction.ESCALATE

    out = capsys.readouterr().out
    assert "contexts=2" in out
    assert "activated=1" in out
    assert "draining=1" in out
    assert "drift=0" in out


def test_run_calibration_no_data_fails_closed(
    db: Database, generator: PolicyGenerator, gatekeeper: Gatekeeper, capsys
) -> None:
    policy_file = CalibrationPipeline(db, gatekeeper, generator).run_calibration()

    artifact = load_policy(policy_file)
    assert artifact["contexts"] == []
    assert gatekeeper.policy == {}
    assert gatekeeper.evaluate(CTX_A, 0.99, "route") == GateAction.ESCALATE
    assert "contexts=0" in capsys.readouterr().out


def test_run_calibration_logs_and_counts_drift(
    db: Database, generator: PolicyGenerator, gatekeeper: Gatekeeper, caplog
) -> None:
    pairs = [(f"c-{i}", 0.05, 1.0, "EXPLORE_SHADOW") for i in range(15)]
    pairs += [(f"c-{i}", 0.05, 0.0, "DELEGATE") for i in range(15, 30)]
    seed_context(db, pairs, context=CTX_B)

    with caplog.at_level(logging.INFO, logger="decision_ledger.pipeline"):
        CalibrationPipeline(db, gatekeeper, generator).run_calibration()

    expected = "[Pipeline context %s: q_hat=None samples=15 drift=True]"
    assert expected % CTX_B.hex() in caplog.text


def test_get_calibration_stats_snapshot(
    db: Database, generator: PolicyGenerator, gatekeeper: Gatekeeper
) -> None:
    seed_context(db, successful_set(15, "a") + exploratory_set(15, "a"))
    seed_context(db, successful_set(5, "b") + exploratory_set(5, "b"), context=CTX_B)
    db.batch_insert("decisions", [decision_row("unmatched-d")])

    pipeline = CalibrationPipeline(db, gatekeeper, generator)
    stats = pipeline.get_calibration_stats()

    assert stats["total_decisions"] == 41
    assert stats["total_outcomes"] == 40
    assert stats["join_rate"] == pytest.approx(40 / 41)
    assert stats["contexts_calibrated"] == 2
    assert stats["samples_per_context"] == {CTX_A.hex(): 15, CTX_B.hex(): 5}
    distribution = stats["q_hat_distribution"]
    assert distribution["activated_contexts"] == [CTX_A.hex()]
    assert distribution["draining_contexts"] == [CTX_B.hex()]
    assert distribution["q_hats"] == {CTX_A.hex(): 0.05}
    assert stats["drift"]["drift_detected_count"] == 0
    assert stats["drift"]["drifted_contexts"] == []
