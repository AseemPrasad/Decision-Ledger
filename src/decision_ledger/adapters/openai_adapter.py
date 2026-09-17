"""OpenAI SDK Drop-in Wrapper for Decision Ledger."""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from decision_ledger.gatekeeper import GateAction, Gatekeeper
from decision_ledger.utils import make_context_hash

logger = logging.getLogger(__name__)


from collections import deque
from concurrent.futures import ThreadPoolExecutor

_SHADOW_EXECUTOR = ThreadPoolExecutor(max_workers=10, thread_name_prefix="AutoLedgerOpenAIShadow")


class AutoLedgerChatCompletions:
    """Interceptor for client.chat.completions.create()."""

    def __init__(
        self,
        target_completions: Any,
        gatekeeper: Gatekeeper,
        small_model: str,
        frontier_model: str,
        decision_type: str = "route",
        default_confidence: float = 0.95,
        max_log_capacity: int = 1000,
    ) -> None:
        self._completions = target_completions
        self.gatekeeper = gatekeeper
        self.small_model = small_model
        self.frontier_model = frontier_model
        self.decision_type = decision_type
        self.default_confidence = default_confidence
        self._shadow_logs: deque[Dict[str, Any]] = deque(maxlen=max_log_capacity)

    @property
    def shadow_logs(self) -> List[Dict[str, Any]]:
        return list(self._shadow_logs)

    def clear_shadow_logs(self) -> None:
        self._shadow_logs.clear()

    def create(self, *args: Any, **kwargs: Any) -> Any:
        """Intercept chat completions create call and apply conformal gating."""
        messages = kwargs.get("messages", [])
        requested_model = kwargs.get("model", self.small_model)
        confidence = kwargs.pop("confidence", self.default_confidence)

        # Compute 16-byte BLAKE3 context hash for model + task
        ctx_hash = make_context_hash(model_id=self.small_model, task_type=self.decision_type)

        # Evaluate gatekeeper decision
        action = self.gatekeeper.evaluate(ctx_hash, confidence, self.decision_type)
        self._shadow_logs.append({
            "context_hash": ctx_hash.hex(),
            "confidence": confidence,
            "action": action,
            "messages": messages,
        })

        if action == GateAction.DELEGATE:
            logger.debug("[AutoLedgerOpenAI] DELEGATE -> %s", self.small_model)
            kwargs["model"] = self.small_model
            return self._completions.create(*args, **kwargs)

        elif action == GateAction.ESCALATE:
            logger.debug("[AutoLedgerOpenAI] ESCALATE -> %s", self.frontier_model)
            kwargs["model"] = self.frontier_model
            return self._completions.create(*args, **kwargs)

        else:  # EXPLORE_SHADOW
            logger.debug("[AutoLedgerOpenAI] EXPLORE_SHADOW -> Serving %s, logging counterfactual %s", self.frontier_model, self.small_model)
            # Serve frontier model response to caller
            kwargs_frontier = dict(kwargs)
            kwargs_frontier["model"] = self.frontier_model
            frontier_response = self._completions.create(*args, **kwargs_frontier)

            # Asynchronously invoke small model for counterfactual shadow logging via thread pool
            def _log_shadow() -> None:
                try:
                    kwargs_small = dict(kwargs)
                    kwargs_small["model"] = self.small_model
                    self._completions.create(*args, **kwargs_small)
                except Exception as err:
                    logger.debug("Shadow evaluation error: %s", err)

            _SHADOW_EXECUTOR.submit(_log_shadow)
            return frontier_response


class AutoLedgerChat:
    """Wrapper for client.chat namespace."""

    def __init__(self, target_chat: Any, **kwargs: Any) -> None:
        self.completions = AutoLedgerChatCompletions(target_chat.completions, **kwargs)


class AutoLedgerOpenAI:
    """Drop-in wrapper for OpenAI client with zero-code conformal delegation."""

    def __init__(
        self,
        openai_client: Optional[Any] = None,
        gatekeeper: Optional[Gatekeeper] = None,
        small_model: str = "gpt-4o-mini",
        frontier_model: str = "gpt-4o",
        decision_type: str = "route",
        default_confidence: float = 0.95,
    ) -> None:
        if gatekeeper is None:
            gatekeeper = Gatekeeper()
        if openai_client is None:
            # Fallback mock client if real openai client is not passed
            openai_client = _MockOpenAIClient()

        self._client = openai_client
        self.gatekeeper = gatekeeper
        self.chat = AutoLedgerChat(
            openai_client.chat,
            gatekeeper=gatekeeper,
            small_model=small_model,
            frontier_model=frontier_model,
            decision_type=decision_type,
            default_confidence=default_confidence,
        )

    @property
    def shadow_logs(self) -> List[Dict[str, Any]]:
        return self.chat.completions.shadow_logs

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


class _MockOpenAICompletions:
    def create(self, *args: Any, **kwargs: Any) -> Any:
        model = kwargs.get("model", "mock-model")
        return {
            "id": "chatcmpl-mock123",
            "object": "chat.completion",
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": f"Response from {model}"},
                    "finish_reason": "stop",
                }
            ],
        }


class _MockOpenAIChat:
    def __init__(self) -> None:
        self.completions = _MockOpenAICompletions()


class _MockOpenAIClient:
    def __init__(self) -> None:
        self.chat = _MockOpenAIChat()
