"""Tests for the Split Conformal Risk Control calibrator.

Covers the pure-statistics core (``compute_threshold`` / Wilson interval), the
database-backed context calibration (``calibrate_context``) and drift
detection (``detect_drift``), plus every documented edge case.
"""

import pytest

from decision_ledger import CalibrationRecord, ConformalCalibrator, Database

CTX = bytes(range(16))
OTHER_CTX = bytes(reversed(range(16)))

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def record(
    score: float, loss: float, *, exploratory: bool = False
) -> CalibrationRecord:
    return CalibrationRecord(
        context_hash=CTX,
        non_conformity_score=score,
        loss=loss,
        is_independent=True,
        is_exploratory=exploratory,
    )


def decision_row(decision_id: str, *, context: bytes = CTX) -> dict:
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
    context: bytes = CTX,
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


def outcome_row(
    decision_id: str, *, source: str = "task_metric", outcome_id: str | None = None
) -> dict:
    return {
        "outcome_id": outcome_id if outcome_id is not None else f"o-{decision_id}",
        "decision_id": decision_id,
        "timestamp_ns": 2,
        "outcome_value": 1.0,
        "outcome_source": source,
        "metadata": None,
    }


def seed_context(
    database: Database,
    pairs: list[tuple[str, float, float, str]],
    *,
    source: str = "task_metric",
    context: bytes = CTX,
) -> None:
    """Seed decisions, joined_records and outcomes for the pairs."""
    database.batch_insert(
        "decisions",
        [decision_row(decision_id, context=context) for decision_id, *_ in pairs],
    )
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
            for decision_id, score, value, action in pairs
        ],
    )
    database.batch_insert(
        "outcomes",
        [outcome_row(decision_id, source=source) for decision_id, *_ in pairs],
    )


def seed_joined_only(database: Database, rows: list[dict]) -> None:
    """Seed only joined_records (what ``detect_drift`` reads)."""
    database.batch_insert("joined_records", rows)


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "calibration.db"))
    yield database
    database.close()


def calibrator(database=None, **kwargs) -> ConformalCalibrator:
    return ConformalCalibrator(database, **kwargs)


# --------------------------------------------------------------------------- #
# Constructor validation
# --------------------------------------------------------------------------- #


def test_constructor_validation():
    with pytest.raises(ValueError):
        ConformalCalibrator(target_alpha=0.0)
    with pytest.raises(ValueError):
        ConformalCalibrator(target_alpha=1.0)
    with pytest.raises(ValueError):
        ConformalCalibrator(min_sample_size=0)
    with pytest.raises(ValueError):
        ConformalCalibrator(confidence_level=1.0)
    with pytest.raises(ValueError):
        ConformalCalibrator(confidence_level=0.0)


def test_defaults_match_split_conformal_risk_control_spec():
    c = ConformalCalibrator()
    assert c.target_alpha == 0.05
    assert c.min_sample_size == 100
    assert c.confidence_level == 0.95
    assert c.z_score == 1.96


# --------------------------------------------------------------------------- #
# Pure-statistics edge cases
# --------------------------------------------------------------------------- #


def test_empty_records_returns_no_threshold():
    result = calibrator(min_sample_size=100).compute_threshold([])
    assert result.q_hat is None
    assert result.sample_size == 0
    assert result.coverage_lower_bound is None
    assert result.achieved_empirical_risk is None
    assert result.min_observed_loss == 0.0
    assert result.max_observed_loss == 0.0


def test_insufficient_samples_returns_no_threshold():
    result = calibrator(min_sample_size=500).compute_threshold([record(0.1, 0.0)] * 50)
    assert result.q_hat is None
    assert result.sample_size == 50
    assert result.coverage_lower_bound is None
    assert result.achieved_empirical_risk is None
    assert result.min_observed_loss == 0.0
    assert result.max_observed_loss == 0.0


def test_all_successful_outcomes_unrestrict_q_hat():
    scores = [0.05, 0.2, 0.4, 0.6, 0.85]
    result = calibrator(target_alpha=0.05, min_sample_size=5).compute_threshold(
        [record(score, 0.0) for score in scores]
    )
    assert result.q_hat == pytest.approx(max(scores))
    assert result.achieved_empirical_risk == pytest.approx(0.0)
    assert result.min_observed_loss == 0.0
    assert result.max_observed_loss == 0.0


def test_all_failures_returns_no_valid_threshold():
    scores = [0.05, 0.2, 0.4, 0.6, 0.9]
    result = calibrator(target_alpha=0.05, min_sample_size=5).compute_threshold(
        [record(score, 1.0) for score in scores]
    )
    assert result.q_hat is None
    assert result.achieved_empirical_risk == pytest.approx(1.0)
    assert result.min_observed_loss == 1.0
    assert result.max_observed_loss == 1.0


# --------------------------------------------------------------------------- #
# Deterministic quantile / empirical-risk correctness
# --------------------------------------------------------------------------- #

SMALL = [(0.05, 0.0), (0.2, 1.0), (0.4, 0.0), (0.6, 0.0), (0.9, 1.0)]


def _small_records():
    return [record(score, loss) for score, loss in SMALL]


def test_empirical_risk_and_q_hat_calculation():
    result = calibrator(target_alpha=0.5, min_sample_size=5).compute_threshold(
        _small_records()
    )
    # Sorted prefixes: (0.05,0) (0.2,1) (0.4,0) (0.6,0) (0.9,1)
    # empirical risk:   0.0     0.5     1/3    0.25    0.4
    assert result.q_hat == pytest.approx(0.9)  # largest prefix risk <= 0.5
    assert result.achieved_empirical_risk == pytest.approx(0.4)
    assert result.sample_size == 5
    assert result.min_observed_loss == 0.0
    assert result.max_observed_loss == 1.0


def test_no_prefix_leaves_worst_case_assumption():
    # alpha tight enough that only the strictest prefix fits.
    result = calibrator(target_alpha=0.2, min_sample_size=5).compute_threshold(
        _small_records()
    )
    assert result.q_hat == pytest.approx(0.05)
    assert result.achieved_empirical_risk == pytest.approx(0.0)
    assert result.coverage_lower_bound == pytest.approx(
        calibrator()._wilson_interval_lower(1.0, 1), abs=1e-6
    )


# --------------------------------------------------------------------------- #
# Wilson interval
# --------------------------------------------------------------------------- #


def test_wilson_interval_lower_formula():
    c = ConformalCalibrator()
    # p=1, n=1: (center=2.9208, margin=1.9208, denom=4.8416) -> ~0.2065
    assert c._wilson_interval_lower(1.0, 1) == pytest.approx(0.20654, abs=1e-4)
    assert c._wilson_interval_lower(0.6, 5) == pytest.approx(0.23072, abs=1e-4)
    # Finite-sample confidence shrinks with sample size.
    assert c._wilson_interval_lower(0.95, 100) < c._wilson_interval_lower(0.95, 1000)
    assert c._wilson_interval_lower(1.0, 1000) > 0.995


def test_wilson_interval_lower_zero_samples():
    c = ConformalCalibrator()
    assert c._wilson_interval_lower(0.0, 0) == 0.0
    assert c._wilson_interval_lower(0.5, -1) == 0.0
    assert c._wilson_interval_lower(0.95, 400) == c.wilson_lower_bound(0.95, 400)


# --------------------------------------------------------------------------- #
# Group calibration (offline path)
# --------------------------------------------------------------------------- #


def test_exploratory_and_non_independent_records_are_excluded():
    all_inclusive = calibrator(
        target_alpha=0.05, min_sample_size=100
    ).compute_threshold([record(s, 0.0) for s in (0.05, 0.06)] * 500)
    assert all_inclusive.q_hat is not None

    mixed = [record(0.05, 0.0, exploratory=(index % 2 == 0)) for index in range(1000)]
    reduced = calibrator(target_alpha=0.05, min_sample_size=100).compute_threshold(
        mixed
    )
    assert reduced.sample_size == 500  # half excluded


def test_calibrate_by_context_partitions_independently():
    results = calibrator(target_alpha=0.05, min_sample_size=100).calibrate_by_context(
        [
            CalibrationRecord(ctx, score, 1.0 if score > 0.1 else 0.0)
            for ctx, score in [(b"a" * 16, 0.05), (b"a" * 16, 0.06), (b"b" * 16, 0.35)]
            for _ in range(500)
        ]
    )
    assert set(results) == {b"a" * 16, b"b" * 16}
    assert results[b"a" * 16].q_hat is not None
    assert results[b"b" * 16].q_hat is None  # 100% losses at score 0.35


def test_monotonicity_with_stricter_alpha():
    loose = calibrator(target_alpha=0.10, min_sample_size=100)
    strict = calibrator(target_alpha=0.01, min_sample_size=100)
    records = [record(s, 1.0 if s > 0.1 else 0.0) for s in _synthetic_scores(2000)]
    loose_result = loose.compute_threshold(records)
    strict_result = strict.compute_threshold(records)
    assert (
        loose_result.q_hat is None
        or strict_result.q_hat is None
        or strict_result.q_hat <= loose_result.q_hat
    )


def _synthetic_scores(n: int) -> list[float]:
    # Deterministic pseudo-random spread across [0, 1] (no import of random
    # needed in the module under test; keep the generator in the test).
    import random

    rng = random.Random(7)
    return [rng.random() for _ in range(n)]


def test_computes_threshold_within_risk_budget():
    result = calibrator(target_alpha=0.05, min_sample_size=100).compute_threshold(
        [record(s, 1.0 if s > 0.1 else 0.0) for s in _synthetic_scores(1000)]
    )
    assert result.q_hat is not None
    assert 0.0 < result.q_hat < 1.0
    assert result.achieved_empirical_risk is not None
    assert result.achieved_empirical_risk <= 0.05
    assert result.coverage_lower_bound is not None
    assert 0.0 < result.coverage_lower_bound <= (1.0 - result.achieved_empirical_risk)


# --------------------------------------------------------------------------- #
# Database-backed calibrate_context
# --------------------------------------------------------------------------- #


def test_calibrate_context_requires_database():
    with pytest.raises(ValueError):
        ConformalCalibrator().calibrate_context(CTX)
    with pytest.raises(ValueError):
        ConformalCalibrator().detect_drift(CTX)


def test_calibrate_context_rejects_invalid_hash(db):
    with pytest.raises(ValueError):
        calibrator(db, min_sample_size=1).calibrate_context(b"too short")
    with pytest.raises(ValueError):
        calibrator(db, min_sample_size=1).detect_drift(b"too short")


def test_calibrate_context_computes_q_hat_from_sqlite(db):
    pairs = [
        ("d-1", 0.05, 1.0, "DELEGATE"),
        ("d-2", 0.2, 0.0, "DELEGATE"),
        ("d-3", 0.4, 1.0, "DELEGATE"),
        ("d-4", 0.6, 1.0, "DELEGATE"),
        ("d-5", 0.9, 0.0, "DELEGATE"),
    ]
    seed_context(db, pairs)

    c = calibrator(db, target_alpha=0.5, min_sample_size=5)
    result = c.calibrate_context(CTX)

    assert result.sample_size == 5
    assert result.q_hat == pytest.approx(0.9)
    assert result.achieved_empirical_risk == pytest.approx(0.4)
    assert result.min_observed_loss == 0.0
    assert result.max_observed_loss == 1.0
    assert result.coverage_lower_bound == pytest.approx(
        c._wilson_interval_lower(0.6, 5), abs=1e-6
    )


def test_calibrate_context_matches_pure_path(db):
    pairs = [
        ("d-1", 0.05, 1.0, "DELEGATE"),
        ("d-2", 0.2, 0.0, "DELEGATE"),
        ("d-3", 0.4, 1.0, "DELEGATE"),
        ("d-4", 0.6, 1.0, "DELEGATE"),
        ("d-5", 0.9, 0.0, "DELEGATE"),
    ]
    seed_context(db, pairs)

    c = calibrator(db, target_alpha=0.5, min_sample_size=5)
    from_db = c.calibrate_context(CTX)
    offline = c.compute_threshold(
        [record(score, 1.0 if value < 0.5 else 0.0) for (_, score, value, _) in pairs]
    )
    assert from_db == offline


def test_calibrate_context_excludes_explore_shadow(db):
    pairs = []
    for index in range(1000):
        action = "EXPLORE_SHADOW" if index % 2 == 0 else "DELEGATE"
        pairs.append((f"d-{index}", 0.3, 1.0, action))
    seed_context(db, pairs)

    result = calibrator(db, target_alpha=0.4, min_sample_size=100).calibrate_context(
        CTX
    )
    assert result.sample_size == 500  # exploratory half excluded


def test_calibrate_context_excludes_non_independent_outcomes(db):
    seed_context(
        db,
        [(f"x-{i}", 0.3, 1.0, "DELEGATE") for i in range(100)],
        source="model_verification",
    )
    seed_context(
        db,
        [(f"y-{i}", 0.3, 1.0, "DELEGATE") for i in range(100)],
        source="task_metric",
    )

    result = calibrator(db, target_alpha=0.4, min_sample_size=100).calibrate_context(
        CTX
    )
    assert result.sample_size == 100  # only the independent (task_metric) records


def test_calibrate_context_includes_decision_with_mixed_outcomes(db):
    seed_context(db, [("d-mixed", 0.3, 1.0, "DELEGATE")], source="task_metric")
    db.batch_insert(
        "outcomes",
        [
            outcome_row(
                "d-mixed",
                source="model_verification",
                outcome_id="o-d-mixed-model",
            )
        ],
    )

    result = calibrator(db, target_alpha=0.4, min_sample_size=1).calibrate_context(CTX)
    assert result.sample_size == 1  # an independent outcome exists


def test_calibrate_context_all_success_q_hat_is_max_score(db):
    seed_context(
        db,
        [("d-1", 0.5, 1.0, "DELEGATE"), ("d-2", 0.9, 1.0, "DELEGATE")],
    )
    result = calibrator(db, target_alpha=0.05, min_sample_size=2).calibrate_context(CTX)
    assert result.q_hat == pytest.approx(0.9)


def test_calibrate_context_no_valid_threshold(db):
    seed_context(
        db,
        [("d-1", 0.5, 0.0, "DELEGATE"), ("d-2", 0.9, 0.0, "DELEGATE")],
    )
    result = calibrator(db, target_alpha=0.05, min_sample_size=2).calibrate_context(CTX)
    assert result.q_hat is None
    assert result.achieved_empirical_risk == pytest.approx(1.0)


def test_calibrate_context_insufficient_data(db):
    seed_context(db, [("d-1", 0.3, 1.0, "DELEGATE")])
    result = calibrator(db, target_alpha=0.05, min_sample_size=100).calibrate_context(
        CTX
    )
    assert result.q_hat is None
    assert result.sample_size == 1


def test_calibrate_context_empty_context_returns_empty_result(db):
    result = calibrator(db, target_alpha=0.05, min_sample_size=1).calibrate_context(
        OTHER_CTX
    )
    assert result.q_hat is None
    assert result.sample_size == 0


def test_calibrate_context_is_scoped_to_context(db):
    seed_context(db, [("d-1", 0.3, 1.0, "DELEGATE")], context=OTHER_CTX)
    result = calibrator(db, target_alpha=0.4, min_sample_size=1).calibrate_context(CTX)
    assert result.sample_size == 0


# --------------------------------------------------------------------------- #
# Drift detection
# --------------------------------------------------------------------------- #


def test_detect_drift_flags_wide_divergence(db):
    seed_joined_only(
        db,
        [
            joined_row(f"e-{i}", score=0.9, outcome_value=1.0, action="EXPLORE_SHADOW")
            for i in range(10)
        ]
        + [
            joined_row(f"e-{i}", score=0.9, outcome_value=0.0, action="EXPLORE_SHADOW")
            for i in range(10, 20)
        ]
        + [
            joined_row(f"d-{i}", score=0.3, outcome_value=1.0, action="DELEGATE")
            for i in range(20)
        ],
    )

    report = calibrator(db).detect_drift(CTX)
    assert report["drift_detected"] is True
    assert report["full_range_accuracy"] == pytest.approx(0.5)
    assert report["active_range_accuracy"] == pytest.approx(1.0)
    assert report["divergence"] == pytest.approx(0.5)


def test_detect_drift_quiet_when_ranges_agree(db):
    rows = [
        joined_row(f"r-{a}-{i}", score=0.5, outcome_value=1.0, action=a)
        for a in ("DELEGATE", "EXPLORE_SHADOW")
        for i in range(20)
    ]
    seed_joined_only(db, rows)

    report = calibrator(db).detect_drift(CTX)
    assert report["drift_detected"] is False
    assert report["full_range_accuracy"] == pytest.approx(1.0)
    assert report["active_range_accuracy"] == pytest.approx(1.0)
    assert report["divergence"] == pytest.approx(0.0)


def test_detect_drift_empty_context_reports_zeroes(db):
    report = calibrator(db).detect_drift(OTHER_CTX)
    assert report == {
        "drift_detected": False,
        "full_range_accuracy": 0.0,
        "active_range_accuracy": 0.0,
        "divergence": 0.0,
    }


def test_detect_drift_missing_range_counts_as_zero_accuracy(db):
    seed_joined_only(
        db,
        [
            joined_row(f"d-{i}", score=0.3, outcome_value=1.0, action="DELEGATE")
            for i in range(20)
        ],
    )
    report = calibrator(db).detect_drift(CTX)
    assert report["full_range_accuracy"] == 0.0  # no exploration records
    assert report["active_range_accuracy"] == 1.0
    assert report["drift_detected"] is True
