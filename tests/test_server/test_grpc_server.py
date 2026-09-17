import time
import pytest
import grpc
from decision_ledger.telemetry import RingBuffer, DecisionRecord
from decision_ledger.client.grpc_consumer import GrpcRemoteSyncConsumer
from decision_ledger_server.grpc_server import create_grpc_server, DecisionLedgerServicer
from decision_ledger.proto import decision_ledger_pb2 as pb2
from decision_ledger.proto import decision_ledger_pb2_grpc as pb2_grpc

def test_grpc_server_and_consumer_streaming():
    # 1. Start gRPC server on dynamic port
    policy_dict = {
        "policy_version": "v2.0.0-grpc",
        "contexts": {
            "a1b2c3d4e5f60708090a0b0c0d0e0f10": {
                "q_hat": 0.15,
                "min_sample_size": 100,
                "current_sample_size": 500,
                "is_active": True,
            }
        }
    }
    servicer = DecisionLedgerServicer(current_policy_dict=policy_dict)
    server, servicer, port = create_grpc_server(servicer=servicer, port=0)
    server.start()

    try:
        target = f"localhost:{port}"
        rb = RingBuffer(capacity=1000)

        # 2. Push telemetry records to RingBuffer
        for i in range(10):
            rb.push(DecisionRecord(
                decision_id=f"dec-{i}",
                timestamp_ns=time.time_ns(),
                context_hash=b"1234567890123456",
                decision_type="route",
                model_confidence=0.92,
                non_conformity=0.08,
                action_taken="DELEGATE",
                latency_us=1500,
            ))

        # 3. Stream over gRPC consumer
        consumer = GrpcRemoteSyncConsumer(ring_buffer=rb, server_target=target, batch_size=10)
        resp = consumer.send_batch()

        assert resp is not None
        assert resp.success is True
        assert resp.processed_count == 10
        assert len(servicer.received_records) == 10
        assert servicer.received_records[0].decision_id == "dec-0"

        # 4. Test StreamPolicyUpdates RPC
        channel = grpc.insecure_channel(target)
        stub = pb2_grpc.DecisionLedgerServiceStub(channel)
        updates = list(stub.StreamPolicyUpdates(pb2.PolicyRequest(client_id="test-client")))
        assert len(updates) == 1
        assert updates[0].policy_version == "v2.0.0-grpc"
        assert "a1b2c3d4e5f60708090a0b0c0d0e0f10" in updates[0].contexts

        consumer.close()
        channel.close()

    finally:
        server.stop(grace=None)
