"""Redis Event Bus & Distributed Caching helper for Decision Ledger Control Plane."""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

try:
    import redis
    _HAS_REDIS = True
except ImportError:
    redis = None
    _HAS_REDIS = False


class RedisPolicyBus:
    """Pub/Sub channel manager for broadcasting policy updates across distributed edge nodes."""

    CHANNEL_NAME = "decision_ledger:policy_updates"

    def __init__(self, redis_url: Optional[str] = None) -> None:
        self.redis_url = redis_url or os.getenv("REDIS_URL")
        self._client: Optional[Any] = None
        self._pubsub: Optional[Any] = None
        self._listener_thread: Optional[threading.Thread] = None

        if _HAS_REDIS and self.redis_url:
            try:
                self._client = redis.Redis.from_url(self.redis_url, decode_responses=True)
                self._client.ping()
                logger.info("Connected to Redis Policy Bus at %s", self.redis_url)
            except Exception as err:
                logger.warning("Could not connect to Redis at %s (%s); operating in standalone mode", self.redis_url, err)
                self._client = None

    @property
    def is_connected(self) -> bool:
        """True if connected to a live Redis cluster."""
        return self._client is not None

    def publish_policy_update(self, tenant_id: str, policy_version: str) -> bool:
        """Broadcast policy update notification across cluster."""
        if not self._client:
            return False

        payload = {
            "event": "policy_updated",
            "tenant_id": tenant_id,
            "policy_version": policy_version,
        }
        try:
            self._client.publish(self.CHANNEL_NAME, json.dumps(payload))
            return True
        except Exception as err:
            logger.warning("Failed to publish policy update to Redis: %s", err)
            return False

    def subscribe(self, callback: Callable[[str, str], None]) -> None:
        """Subscribe background thread to policy update notifications."""
        if not self._client or self._pubsub is not None:
            return

        def _listen_loop() -> None:
            pubsub = self._client.pubsub()
            pubsub.subscribe(self.CHANNEL_NAME)
            self._pubsub = pubsub

            for message in pubsub.listen():
                if message and message.get("type") == "message":
                    try:
                        data = json.loads(message["data"])
                        if data.get("event") == "policy_updated":
                            callback(data["tenant_id"], data["policy_version"])
                    except Exception as err:
                        logger.debug("Error processing Redis pubsub message: %s", err)

        self._listener_thread = threading.Thread(target=_listen_loop, daemon=True)
        self._listener_thread.start()
