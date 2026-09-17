"""High-Throughput gRPC Control-Plane Server for Decision Ledger."""

from __future__ import annotations

import logging
import time
from concurrent import futures
from typing import Any, Dict, Iterator, Optional

import grpc

from decision_ledger.proto import decision_ledger_pb2 as pb2
from decision_ledger.proto import decision_ledger_pb2_grpc as pb2_grpc

logger = logging.getLogger(__name__)


class DecisionLedgerServicer(pb2_grpc.DecisionLedgerServiceServicer):
    """gRPC Servicer for high-throughput binary telemetry and policy streaming."""

    def __init__(self, current_policy_dict: Optional[Dict[str, Any]] = None) -> None:
        self.current_policy_dict = current_policy_dict or {}
        self.received_records: list[Any] = []

    def StreamTelemetry(
        self, request_iterator: Iterator[pb2.TelemetryRecord], context: grpc.ServicerContext
    ) -> pb2.TelemetryResponse:
        """Receive stream of binary TelemetryRecord messages from client microservices."""
        count = 0
        try:
            for record in request_iterator:
                self.received_records.append(record)
                count += 1
            return pb2.TelemetryResponse(
                processed_count=count,
                success=True,
                error_message="",
            )
        except Exception as err:
            logger.error("Error in StreamTelemetry gRPC handler: %s", err)
            return pb2.TelemetryResponse(
                processed_count=count,
                success=False,
                error_message=str(err),
            )

    def StreamPolicyUpdates(
        self, request: pb2.PolicyRequest, context: grpc.ServicerContext
    ) -> Iterator[pb2.PolicyUpdate]:
        """Stream policy updates from server to client nodes."""
        policy_version = str(self.current_policy_dict.get("policy_version", "v1.0.0"))
        
        contexts_proto = {}
        raw_contexts = self.current_policy_dict.get("contexts", {})
        for ctx_hex, ctx_val in raw_contexts.items():
            if isinstance(ctx_val, dict):
                contexts_proto[ctx_hex] = pb2.ContextEnvelope(
                    q_hat=float(ctx_val.get("q_hat", 0.1)),
                    min_sample_size=int(ctx_val.get("min_sample_size", 100)),
                    current_sample_size=int(ctx_val.get("current_sample_size", 100)),
                    is_active=bool(ctx_val.get("is_active", True)),
                )

        yield pb2.PolicyUpdate(
            policy_version=policy_version,
            timestamp_ns=time.time_ns(),
            contexts=contexts_proto,
            ed25519_signature=b"",
        )

    def BiDirectionalSync(
        self, request_iterator: Iterator[pb2.TelemetryRecord], context: grpc.ServicerContext
    ) -> Iterator[pb2.PolicyUpdate]:
        """Bi-directional full-duplex stream for telemetry ingestion and policy push."""
        # Process incoming telemetry
        for record in request_iterator:
            self.received_records.append(record)

        # Yield current policy update
        policy_version = str(self.current_policy_dict.get("policy_version", "v1.0.0"))
        yield pb2.PolicyUpdate(
            policy_version=policy_version,
            timestamp_ns=time.time_ns(),
            contexts={},
            ed25519_signature=b"",
        )


def create_grpc_server(
    servicer: Optional[DecisionLedgerServicer] = None,
    host: str = "[::]",
    port: int = 50051,
    max_workers: int = 10,
) -> tuple[grpc.Server, DecisionLedgerServicer, int]:
    """Factory creating a started or unstarted gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    if servicer is None:
        servicer = DecisionLedgerServicer()
    pb2_grpc.add_DecisionLedgerServiceServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    return server, servicer, bound_port
