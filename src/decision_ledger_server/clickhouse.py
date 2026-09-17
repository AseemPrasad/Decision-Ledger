"""ClickHouse Column-Oriented Telemetry Data Lake Client for Decision Ledger."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False


class ClickHouseTelemetryStore:
    """High-Throughput Column-Oriented ClickHouse Telemetry Store."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "default",
        table_name: str = "decision_telemetry",
        client: Optional[Any] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table_name = table_name
        self._client = client

    def connect(self) -> bool:
        """Establish connection to ClickHouse server if clickhouse-connect package is installed."""
        if self._client is not None:
            return True

        if not CLICKHOUSE_AVAILABLE:
            logger.warning("clickhouse-connect is not installed; ClickHouse features disabled.")
            return False

        try:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
            self.init_schema()
            return True
        except Exception as err:
            logger.error("Failed to connect to ClickHouse server at %s:%d: %s", self.host, self.port, err)
            return False

    def init_schema(self) -> None:
        """Initialize high-performance MergeTree table schema for analytical telemetry."""
        if self._client is None:
            return

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            decision_id String,
            timestamp DateTime64(6, 'UTC'),
            context_hash String,
            decision_type String,
            model_confidence Float64,
            non_conformity Float64,
            action_taken String,
            latency_us UInt64,
            loss Nullable(Float64)
        ) ENGINE = MergeTree()
        PRIMARY KEY (context_hash, timestamp)
        ORDER BY (context_hash, timestamp);
        """
        try:
            self._client.command(create_table_sql)
            logger.info("ClickHouse table '%s' schema verified.", self.table_name)
        except Exception as err:
            logger.error("Error creating ClickHouse table schema: %s", err)

    def insert_batch(self, records: List[Dict[str, Any]]) -> int:
        """High-throughput batch insertion into ClickHouse."""
        if self._client is None or not records:
            return 0

        data = []
        for r in records:
            data.append([
                str(r.get("decision_id", "")),
                r.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
                str(r.get("context_hash", "")),
                str(r.get("decision_type", "route")),
                float(r.get("model_confidence", 0.0)),
                float(r.get("non_conformity", 0.0)),
                str(r.get("action_taken", "DELEGATE")),
                int(r.get("latency_us", 0)),
                r.get("loss"),
            ])

        column_names = [
            "decision_id", "timestamp", "context_hash", "decision_type",
            "model_confidence", "non_conformity", "action_taken", "latency_us", "loss"
        ]

        try:
            self._client.insert(self.table_name, data, column_names=column_names)
            return len(records)
        except Exception as err:
            logger.error("Failed to insert batch into ClickHouse: %s", err)
            return 0

    def compute_quantile(self, context_hash_hex: str, target_alpha: float = 0.05) -> Optional[float]:
        """Sub-second column-oriented analytical quantile computation over billions of rows."""
        if self._client is None:
            return None

        quantile_level = 1.0 - target_alpha
        query = f"""
        SELECT quantileExact({quantile_level})(non_conformity) AS q_hat
        FROM {self.table_name}
        WHERE context_hash = %(context_hash)s AND loss IS NOT NULL;
        """
        try:
            result = self._client.query(query, parameters={"context_hash": context_hash_hex})
            if result.result_rows and result.result_rows[0][0] is not None:
                return float(result.result_rows[0][0])
            return None
        except Exception as err:
            logger.error("ClickHouse quantile query failed for context %s: %s", context_hash_hex, err)
            return None
