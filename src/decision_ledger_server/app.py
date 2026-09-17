"""Control Plane SaaS Application Server using built-in WSGI/HTTP standards."""

from __future__ import annotations

import json
import logging
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional, Tuple
import urllib.parse

from .auth import AuthManager
from .metering import MeteringEngine
from .policy_service import CentralPolicyService
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class ControlPlaneRequestHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for B2B SaaS Control Plane endpoints."""

    auth_manager = AuthManager()
    rate_limiter = RateLimiter(requests_per_minute=10000, burst_capacity=500)
    metering_engine = MeteringEngine(credits_per_1k_decisions=1.0)
    policy_service = CentralPolicyService()

    def do_POST(self) -> None:
        """Handle POST requests (telemetry ingestion, key registration)."""
        path = urllib.parse.urlparse(self.path).path

        if path == "/api/v1/ingest":
            self._handle_ingest()
        elif path == "/api/v1/auth/keys":
            self._handle_create_key()
        else:
            self._send_json({"error": "Endpoint not found"}, status=HTTPStatus.NOT_FOUND)

    def do_GET(self) -> None:
        """Handle GET requests (policy sync, metering stats, health check)."""
        path = urllib.parse.urlparse(self.path).path

        if path == "/healthz":
            self._send_json({"status": "ok", "service": "decision-ledger-control-plane"})
        elif path == "/api/v1/policy/active":
            self._handle_get_policy()
        elif path == "/api/v1/tenants/metering":
            self._handle_get_metering()
        else:
            self._send_json({"error": "Endpoint not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_ingest(self) -> None:
        """Process batch telemetry ingestion with auth, rate limiting & credit metering."""
        api_key = self.headers.get("X-API-Key")
        tenant_ctx = self.auth_manager.authenticate(api_key)
        if not tenant_ctx:
            self._send_json({"error": "Unauthorized / Invalid API Key"}, status=HTTPStatus.UNAUTHORIZED)
            return

        tenant_id = tenant_ctx["tenant_id"]

        # Check Rate Limit
        if not self.rate_limiter.is_allowed(tenant_id):
            self._send_json({"error": "Rate limit exceeded"}, status=HTTPStatus.TOO_MANY_REQUESTS)
            return

        body = self._read_json_body()
        if not body or "records" not in body:
            self._send_json({"error": "Invalid payload format"}, status=HTTPStatus.BAD_REQUEST)
            return

        records = body.get("records", [])
        record_count = len(records)

        # Record usage & credit deduction
        allowed, remaining_credits = self.metering_engine.record_usage(tenant_id, record_count)
        if not allowed:
            self._send_json(
                {"error": "Insufficient credits", "remaining_credits": remaining_credits},
                status=HTTPStatus.PAYMENT_REQUIRED,
            )
            return

        self._send_json(
            {
                "status": "accepted",
                "processed_count": record_count,
                "remaining_credits": remaining_credits,
            },
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_get_policy(self) -> None:
        """Serve active serving policy for tenant."""
        api_key = self.headers.get("X-API-Key")
        tenant_ctx = self.auth_manager.authenticate(api_key)
        if not tenant_ctx:
            self._send_json({"error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return

        tenant_id = tenant_ctx["tenant_id"]
        policy = self.policy_service.get_policy(tenant_id)
        if not policy:
            self._send_json({"error": "No active policy found for tenant"}, status=HTTPStatus.NOT_FOUND)
            return

        self._send_json(policy, status=HTTPStatus.OK)

    def _handle_get_metering(self) -> None:
        """Serve tenant metering snapshot."""
        api_key = self.headers.get("X-API-Key")
        tenant_ctx = self.auth_manager.authenticate(api_key)
        if not tenant_ctx:
            self._send_json({"error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
            return

        tenant_id = tenant_ctx["tenant_id"]
        balance = self.metering_engine.get_balance(tenant_id)
        history = self.metering_engine.get_tenant_history(tenant_id)

        self._send_json(
            {
                "tenant_id": tenant_id,
                "credit_balance": balance,
                "total_ingested_batches": len(history),
            },
            status=HTTPStatus.OK,
        )

    def _handle_create_key(self) -> None:
        """Register a new API Key for a tenant."""
        body = self._read_json_body()
        if not body or "org_id" not in body:
            self._send_json({"error": "org_id required"}, status=HTTPStatus.BAD_REQUEST)
            return

        org_id = body["org_id"]
        tenant_id = body.get("tenant_id", "default")
        new_key = self.auth_manager.generate_key(org_id, tenant_id)

        self._send_json(
            {
                "api_key": new_key,
                "org_id": org_id,
                "tenant_id": tenant_id,
            },
            status=HTTPStatus.CREATED,
        )

    def _read_json_body(self) -> Optional[Dict[str, Any]]:
        try:
            content_len = int(self.headers.get("Content-Length", 0))
            if content_len == 0:
                return None
            raw = self.rfile.read(content_len)
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def _send_json(self, data: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress verbose default HTTP logging."""
        logger.debug(format, *args)


def create_server(host: str = "127.0.0.1", port: int = 8080) -> HTTPServer:
    """Create a control plane HTTPServer instance."""
    return HTTPServer((host, port), ControlPlaneRequestHandler)
