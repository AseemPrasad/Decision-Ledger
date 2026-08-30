"""Zero-allocation telemetry capture: the decision record + ring buffer.

The production design uses a cache-line-aligned 64-byte slot in shared
memory. This MVP uses a ``collections.deque(maxlen=capacity)`` with the same
semantics:

* ``push`` never blocks and never raises: at capacity the buffer wraps,
  the oldest record is evicted and accounted in ``dropped_count``;
* explicit backpressure tiers warn (throttled) as fill rises and, above
  95%, drop exploratory records first (then the oldest) to make room;
* ``pop_batch`` drains up to N records in one FIFO call;
* ``deque`` methods are O(1) under the GIL, so no lock is needed for the
  single-producer / multi-consumer contract.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, List, Optional

from .utils import now_ns

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    """Immutable record of a single gatekeeper evaluation."""

    decision_id: str
    timestamp_ns: int
    context_hash: bytes
    decision_type: str
    model_confidence: float
    non_conformity: float
    action_taken: str
    latency_us: int

    @classmethod
    def from_evaluation(
        cls,
        *,
        context_hash: bytes,
        decision_id: str,
        decision_type: str,
        action: str,
        confidence: float,
        latency_us: int,
        timestamp_ns: Optional[int] = None,
    ) -> DecisionRecord:
        """Build a record from a gatekeeper evaluation result.

        ``action`` is one of ``DELEGATE``, ``ESCALATE``, ``EXPLORE_SHADOW``.
        ``non_conformity`` is derived as ``1 - confidence``, matching the
        calibration score.
        """
        return cls(
            decision_id=decision_id,
            timestamp_ns=now_ns() if timestamp_ns is None else timestamp_ns,
            context_hash=context_hash,
            decision_type=decision_type,
            model_confidence=confidence,
            non_conformity=1.0 - confidence,
            action_taken=action,
            latency_us=latency_us,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "timestamp_ns": self.timestamp_ns,
            "context_hash": self.context_hash.hex(),
            "decision_type": self.decision_type,
            "model_confidence": self.model_confidence,
            "non_conformity": self.non_conformity,
            "action_taken": self.action_taken,
            "latency_us": self.latency_us,
        }


EXPLORATORY = "EXPLORE_SHADOW"


class RingBuffer:
    """Bounded single-producer ring buffer for decision records.

    Backpressure tiers on fill level (``level`` = ``size / capacity``):

    * ``< 0.50`` quiet;
    * ``0.50-0.75`` warn once per minute;
    * ``0.75-0.90`` warn every ten seconds;
    * ``0.90-0.95`` critical warning every few seconds;
    * ``>= 0.95`` (and full) drop -- exploratory records first, then the
      oldest, so escalation/delegation records survive overload longer.

    All drops (evicted-at-wrap, dropped-to-make-room, and rejected inputs)
    are counted in ``dropped_count``; ``total_pushed`` counts every valid
    record submitted. Under the GIL the ``deque`` is append/appendleft safe
    across threads without a lock.
    """

    _WARN1_LEVEL = 0.50
    _WARN2_LEVEL = 0.75
    _WARN3_LEVEL = 0.90
    _DROP_LEVEL = 0.95

    _WARN1_EVERY_S = 60.0
    _WARN2_EVERY_S = 10.0
    _WARN3_EVERY_S = 5.0

    def __init__(self, capacity: int = 100_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._deque: deque[DecisionRecord] = deque(maxlen=capacity)
        self._total_pushed = 0
        self._dropped = 0
        self._warn1_ts = -1e9
        self._warn2_ts = -1e9
        self._warn3_ts = -1e9
        self._warn4_ts = -1e9

    @property
    def total_pushed(self) -> int:
        return self._total_pushed

    @property
    def dropped_count(self) -> int:
        return self._dropped

    def size(self) -> int:
        return len(self._deque)

    def fill_level(self) -> float:
        return len(self._deque) / self.capacity

    def push(self, record: Any) -> bool:
        """Append a record without blocking; returns ``False`` on drop.

        Returns ``False`` (and counts a drop) when the input is not a
        :class:`DecisionRecord`, or when overloaded and the record is
        sacrificed. Otherwise the record is stored, evicting the oldest at
        capacity (a wrap, also counted as a drop).
        """
        if not isinstance(record, DecisionRecord):
            self._dropped += 1
            return False

        self._total_pushed += 1
        level = len(self._deque) / self.capacity
        self._maybe_warn(level)

        if level >= self._DROP_LEVEL and len(self._deque) == self.capacity:
            # Truly full: sacrificing an exploratory record (or dropping the
            # incoming one) beats losing an escalation/delegation record.
            if record.action_taken == EXPLORATORY:
                self._dropped += 1
                return False
            self._dropped += self._evict_low_priority()

        self._deque.append(record)
        return True

    def pop_batch(self, max_records: int = 1000) -> List[DecisionRecord]:
        """Drain up to ``max_records`` records (FIFO, O(1) per record).

        Safe against concurrent pops from another thread: each record is
        popped atomically and an empty deque ends the batch early, so two
        drainers at worst split the buffer between them instead of raising.
        """
        if max_records <= 0:
            return []
        count = min(max_records, len(self._deque))
        batch: List[DecisionRecord] = []
        for _ in range(count):
            try:
                batch.append(self._deque.popleft())
            except IndexError:
                break
        return batch

    def _evict_low_priority(self) -> int:
        """Discard one record, preferring an exploratory one; else oldest.

        Only ever called while the buffer is full, so a popleft is always
        safe when no exploratory record is found.
        """
        for i, candidate in enumerate(self._deque):
            if candidate.action_taken == EXPLORATORY:
                del self._deque[i]
                return 1
        self._deque.popleft()
        return 1

    def _maybe_warn(self, level: float) -> None:
        """Throttled backpressure warnings keyed by fill tier."""
        now = time.monotonic()
        if level < self._WARN1_LEVEL:
            return
        if level < self._WARN2_LEVEL:
            if now - self._warn1_ts >= self._WARN1_EVERY_S:
                self._warn1_ts = now
                logger.warning("ring buffer %.0f%% full (50-75%% tier)", level * 100)
            return
        if level < self._WARN3_LEVEL:
            if now - self._warn2_ts >= self._WARN2_EVERY_S:
                self._warn2_ts = now
                logger.warning("ring buffer %.0f%% full (75-90%% tier)", level * 100)
            return
        if level < self._DROP_LEVEL:
            if now - self._warn3_ts >= self._WARN3_EVERY_S:
                self._warn3_ts = now
                logger.critical("ring buffer %.0f%% full; drops imminent", level * 100)
            return
        if now - self._warn4_ts >= self._WARN3_EVERY_S:
            self._warn4_ts = now
            logger.critical("ring buffer %.0f%% full; dropping records", level * 100)
