"""OpenTelemetry (OTel) Distributed Tracing & Metrics instrumentation for Decision Ledger."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    from opentelemetry import metrics, trace
    from opentelemetry.metrics import Counter, Histogram
    _HAS_OTEL = True
except ImportError:
    metrics = None
    trace = None
    _HAS_OTEL = False


class ObservabilityManager:
    """Manages OpenTelemetry metric instruments and distributed trace propagation."""

    def __init__(self, meter_name: str = "decision_ledger") -> None:
        self.enabled = _HAS_OTEL
        self._meter = None
        self._tracer = None

        self._evaluations_counter = None
        self._latency_histogram = None
        self._q_hat_values: Dict[str, float] = {}

        if self.enabled and metrics is not None and trace is not None:
            try:
                self._meter = metrics.get_meter(meter_name)
                self._tracer = trace.get_tracer(meter_name)

                self._evaluations_counter = self._meter.create_counter(
                    name="decision_ledger.gate_evaluations_total",
                    description="Total number of gatekeeper evaluations",
                    unit="1",
                )

                self._latency_histogram = self._meter.create_histogram(
                    name="decision_ledger.evaluation_latency_microseconds",
                    description="Hot-path gatekeeper evaluation latency distribution in microseconds",
                    unit="us",
                )

                def _q_hat_callback(options: Any) -> Any:
                    if metrics is None:
                        return []
                    from opentelemetry.metrics import Observation
                    return [
                        Observation(val, {"context_ref": ctx_ref})
                        for ctx_ref, val in self._q_hat_values.items()
                    ]

                self._meter.create_observable_gauge(
                    name="decision_ledger.q_hat_gauge",
                    description="Current active q_hat conformal quantile thresholds per context",
                    callbacks=[_q_hat_callback],
                    unit="1",
                )
                logger.info("Initialized OpenTelemetry instrumentation for namespace %r", meter_name)
            except Exception as err:
                logger.warning("Failed to initialize OpenTelemetry instruments: %s", err)
                self.enabled = False

    def record_evaluation(
        self,
        action: str,
        decision_type: str,
        context_hash: bytes,
        latency_us: int,
    ) -> None:
        """Record evaluation metrics (counter + latency histogram)."""
        if not self.enabled:
            return

        ctx_ref = context_hash.hex()
        attrs = {
            "action": action,
            "decision_type": decision_type,
            "context_ref": ctx_ref,
        }

        try:
            if self._evaluations_counter is not None:
                self._evaluations_counter.add(1, attrs)

            if self._latency_histogram is not None:
                self._latency_histogram.record(float(latency_us), attrs)
        except Exception as err:
            logger.debug("Error recording OTel metric: %s", err)

    def update_q_hat(self, context_hash: bytes, q_hat: Optional[float]) -> None:
        """Update active q_hat gauge value for context."""
        ctx_ref = context_hash.hex()
        if q_hat is not None:
            self._q_hat_values[ctx_ref] = float(q_hat)
        else:
            self._q_hat_values.pop(ctx_ref, None)
