"""Anthropic SDK Drop-in Wrapper for Decision Ledger."""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from decision_ledger.gatekeeper import GateAction, Gatekeeper
from decision_ledger.utils import make_context_hash

logger = logging.getLogger(__name__)


class AutoLedgerAnthropicMessages:
    """Interceptor for client.messages.create()."""

    def __init__(
        self,
        target_messages: Any,
        gatekeeper: Gatekeeper,
        small_model: str,
        frontier_model: str,
        decision_type: str = "route",
        default_confidence: float = 0.95,
    ) -> None:
        self._messages = target_messages
        self.gatekeeper = gatekeeper
        self.small_model = small_model
        self.frontier_model = frontier_model
        self.decision_type = decision_type
        self.default_confidence = default_confidence
        self.shadow_logs: List[Dict[str, Any]] = []

    def create(self, *args: Any, **kwargs: Any) -> Any:
        """Intercept Anthropic messages create call and apply conformal gating."""
        requested_model = kwargs.get("model", self.small_model)
        confidence = kwargs.pop("confidence", self.default_confidence)

        # Compute 16-byte BLAKE3 context hash for model + task
        ctx_hash = make_context_hash(model_id=self.small_model, task_type=self.decision_type)

        # Evaluate gatekeeper decision
        action = self.gatekeeper.evaluate(ctx_hash, confidence, self.decision_type)
        self.shadow_logs.append({
            "context_hash": ctx_hash.hex(),
            "confidence": confidence,
            "action": action,
            "messages": kwargs.get("messages", []),
        })

        if action == GateAction.DELEGATE:
            logger.debug("[AutoLedgerAnthropic] DELEGATE -> %s", self.small_model)
            kwargs["model"] = self.small_model
            return self._messages.create(*args, **kwargs)

        elif action == GateAction.ESCALATE:
            logger.debug("[AutoLedgerAnthropic] ESCALATE -> %s", self.frontier_model)
            kwargs["model"] = self.frontier_model
            return self._messages.create(*args, **kwargs)

        else:  # EXPLORE_SHADOW
            logger.debug("[AutoLedgerAnthropic] EXPLORE_SHADOW -> Serving %s", self.frontier_model)
            kwargs_frontier = dict(kwargs)
            kwargs_frontier["model"] = self.frontier_model
            frontier_response = self._messages.create(*args, **kwargs_frontier)

            def _log_shadow() -> None:
                try:
                    kwargs_small = dict(kwargs)
                    kwargs_small["model"] = self.small_model
                    self._messages.create(*args, **kwargs_small)
                except Exception as err:
                    logger.debug("Anthropic shadow evaluation error: %s", err)

            threading.Thread(target=_log_shadow, daemon=True).start()
            return frontier_response


class AutoLedgerAnthropic:
    """Drop-in wrapper for Anthropic client with zero-code conformal delegation."""

    def __init__(
        self,
        anthropic_client: Optional[Any] = None,
        gatekeeper: Optional[Gatekeeper] = None,
        small_model: str = "claude-3-haiku-20240307",
        frontier_model: str = "claude-3-5-sonnet-20240620",
        decision_type: str = "route",
        default_confidence: float = 0.95,
    ) -> None:
        if gatekeeper is None:
            gatekeeper = Gatekeeper()
        if anthropic_client is None:
            anthropic_client = _MockAnthropicClient()

        self._client = anthropic_client
        self.gatekeeper = gatekeeper
        self.messages = AutoLedgerAnthropicMessages(
            anthropic_client.messages,
            gatekeeper=gatekeeper,
            small_model=small_model,
            frontier_model=frontier_model,
            decision_type=decision_type,
            default_confidence=default_confidence,
        )

    @property
    def shadow_logs(self) -> List[Dict[str, Any]]:
        return self.messages.shadow_logs

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _MockAnthropicMessages:
    def create(self, *args: Any, **kwargs: Any) -> Any:
        model = kwargs.get("model", "mock-claude")
        return {
            "id": "msg_mock123",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{"type": "text", "text": f"Anthropic response from {model}"}],
        }


class _MockAnthropicClient:
    def __init__(self) -> None:
        self.messages = _MockAnthropicMessages()
