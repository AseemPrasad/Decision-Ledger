import json
import pytest
from unittest.mock import MagicMock
from decision_ledger.webhooks import WebhookNotifier, WebhookTarget, WebhookFormat
from decision_ledger.pipeline import DriftMonitor, AutoRecalibrationPipeline, CalibrationPipeline

def test_drift_monitor_detection():
    monitor = DriftMonitor(target_alpha=0.05)
    
    # 1. Low empirical loss -> No drift
    normal_data = [{"loss": 0.02}, {"loss": 0.01}, {"loss": 0.03}]
    res = monitor.check_drift(b"ctx1234567890123", normal_data)
    assert res["drift_detected"] is False
    assert res["observed_risk"] == 0.02

    # 2. High empirical loss -> Drift detected
    drifted_data = [{"loss": 0.10}, {"loss": 0.12}, {"loss": 0.08}]
    res_drift = monitor.check_drift(b"ctx1234567890123", drifted_data)
    assert res_drift["drift_detected"] is True
    assert res_drift["observed_risk"] == pytest.approx(0.10)

def test_webhook_notifier_formatting():
    notifier = WebhookNotifier()
    
    # Generic format
    generic_payload = notifier._format_payload(
        WebhookFormat.GENERIC_JSON,
        "SLA_BREACH",
        "hash123",
        0.12,
        0.05,
        "TRIGGERED",
        {"count": 100}
    )
    assert generic_payload["event"] == "SLA_BREACH"
    assert generic_payload["metrics"]["observed_risk"] == 0.12

    # Slack format
    slack_payload = notifier._format_payload(
        WebhookFormat.SLACK,
        "SLA_BREACH",
        "hash123",
        0.12,
        0.05,
        "TRIGGERED",
        {"count": 100}
    )
    assert "Decision Ledger Alert" in slack_payload["text"]

    # PagerDuty format
    pd_payload = notifier._format_payload(
        WebhookFormat.PAGERDUTY,
        "SLA_BREACH",
        "hash123",
        0.12,
        0.05,
        "TRIGGERED",
        {"count": 100}
    )
    assert pd_payload["event_action"] == "trigger"

def test_auto_recalibration_pipeline_trigger():
    mock_pipeline = MagicMock(spec=CalibrationPipeline)
    mock_pipeline.target_alpha = 0.05
    mock_pipeline._context_hashes.return_value = [b"ctx1234567890123"]
    mock_pipeline.run_calibration.return_value = "data/policies/policy_new.yaml"

    mock_notifier = MagicMock(spec=WebhookNotifier)

    auto_pipeline = AutoRecalibrationPipeline(
        pipeline=mock_pipeline,
        webhook_notifier=mock_notifier,
        auto_trigger_on_drift=True
    )

    drifted_records = {
        b"ctx1234567890123": [{"loss": 0.15}, {"loss": 0.20}]
    }

    res = auto_pipeline.evaluate_and_recalibrate_if_needed(drifted_records)
    assert res["drift_detected_count"] == 1
    assert res["recalibration_executed"] is True
    assert res["policy_file"] == "data/policies/policy_new.yaml"

    assert mock_notifier.dispatch_alert.called is True
    assert mock_pipeline.run_calibration.called is True
