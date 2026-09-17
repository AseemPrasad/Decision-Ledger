import pytest
from unittest.mock import MagicMock
from decision_ledger_server.clickhouse import ClickHouseTelemetryStore, CLICKHOUSE_AVAILABLE

def test_clickhouse_telemetry_store_initialization():
    store = ClickHouseTelemetryStore(host="localhost", port=8123)
    assert store.table_name == "decision_telemetry"
    assert store._client is None

def test_clickhouse_mock_operations():
    mock_client = MagicMock()
    store = ClickHouseTelemetryStore(client=mock_client)
    
    # 1. Schema Init
    store.init_schema()
    assert mock_client.command.called is True
    
    # 2. Insert Batch
    records = [
        {
            "decision_id": "dec-1",
            "timestamp": "2026-09-18 03:00:00",
            "context_hash": "a1b2c3d4e5f60708090a0b0c0d0e0f10",
            "decision_type": "route",
            "model_confidence": 0.95,
            "non_conformity": 0.05,
            "action_taken": "DELEGATE",
            "latency_us": 1200,
            "loss": 0.0,
        }
    ]
    count = store.insert_batch(records)
    assert count == 1
    assert mock_client.insert.called is True

    # 3. Compute Quantile
    mock_query_res = MagicMock()
    mock_query_res.result_rows = [[0.12]]
    mock_client.query.return_value = mock_query_res

    q_hat = store.compute_quantile("a1b2c3d4e5f60708090a0b0c0d0e0f10", target_alpha=0.05)
    assert q_hat == 0.12
    assert mock_client.query.called is True
