"""High-Throughput gRPC Telemetry Consumer for Decision Ledger Client."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterator, List, Optional

import grpc

from decision_ledger.proto import decision_ledger_pb2 as pb2
from decision_ledger.proto import decision_ledger_pb2_grpc as pb2_grpc
from decision_ledger.telemetry import RingBuffer

logger = logging.getLogger(__name__)


class GrpcRemoteSyncConsumer:
    """High-Throughput gRPC binary telemetry consumer for client microservices."""

    def __init__(
        self,
        ring_buffer: RingBuffer,
        server_target: str = "localhost:50051",
        batch_size: int = 100,
        client_id: str = "client-default",
    ) -> None:
        self.ring_buffer = ring_buffer
        self.server_target = server_target
        self.batch_size = batch_size
        self.client_id = client_id
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[pb2_grpc.DecisionLedgerServiceStub] = None

    def connect(self) -> None:
        """Establish gRPC channel connection to server."""
        self.channel = grpc.insecure_channel(self.server_target)
        self.stub = pb2_grpc.DecisionLedgerServiceStub(self.channel)

    def close(self) -> None:
        """Close gRPC channel connection."""
        if self.channel is not None:
            self.channel.close()
            self.channel = None
            self.stub = None

    def send_batch(self) -> Optional[pb2.TelemetryResponse]:
        """Drain local ring buffer records and stream over gRPC."""
        if self.stub is None:
            self.connect()

        records = self.ring_buffer.pop_batch(max_records=self.batch_size)
        if not records:
            return None

        def _record_generator() -> Iterator[pb2.TelemetryRecord]:
            for r in records:
                yield pb2.TelemetryRecord(
                    decision_id=getattr(r, "decision_id", ""),
                    context_hash=getattr(r, "context_hash", b""),
                    model_confidence=float(getattr(r, "model_confidence", 0.0)),
                    action=int(getattr(r, "action", 0)),
                    decision_type=str(getattr(r, "decision_type", "route")),
                    latency_ns=int(getattr(r, "latency_us", 0) * 1000) if hasattr(r, "latency_us") else int(getattr(r, "latency_ns", 0)),
                    timestamp_ns=int(getattr(r, "timestamp_ns", time.time_ns())),
                    loss=float(r.loss) if getattr(r, "loss", None) is not None else None,
                )

        try:
            assert self.stub is not None
            response = self.stub.StreamTelemetry(_record_generator())
            return response
        except grpc.RpcError as err:
            logger.error("gRPC telemetry streaming failed: %s", err)
            return None
