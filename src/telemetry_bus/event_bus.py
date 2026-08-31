"""
governance/src/telemetry_bus/event_bus.py

In-memory asyncio.Queue Event Bus — Plane 3 Telemetry Bus.

Simulates Apache Kafka's ATLAS_HOOK and governance topics using
Python's built-in asyncio.Queue. No external dependencies required.

Architecture mapping
--------------------
  Kafka Broker          → EventBus instance (singleton per runtime)
  Kafka Topic           → per-topic asyncio.Queue inside the bus
  Kafka Producer.send() → EventBus.publish()
  Kafka Consumer.poll() → EventBus.subscribe() async generator
  Kafka Consumer Group  → named subscribers tracked per topic
  Kafka Offset          → GovernanceEvent.sequence (monotonic int)

Design
------
  - EventBus is an async context manager. Use `async with EventBus() as bus:`.
  - publish() is fire-and-forget (puts to queue, never awaits consumers).
    This mirrors Kafka's async producer semantics — the caller is NOT blocked.
  - subscribe() returns an async generator that yields events from a
    dedicated per-subscriber asyncio.Queue. Each subscriber gets its own
    copy of every event on that topic (fan-out, not competing consumers).
  - close() sends a STOP sentinel to all subscriber queues so they exit
    their async-for loop cleanly.
  - Thread-safe: all operations are coroutine-safe; no threading.Lock needed.
  - Backpressure: maxsize=0 (unlimited) by default — mirrors Kafka's
    at-rest durability without disk. Set maxsize > 0 to test backpressure.

Pattern: Event-Driven Architecture (Pub/Sub) — Plane 3 of GaC architecture.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from asyncio import Queue
from collections import defaultdict
from datetime import datetime, timezone
from typing import AsyncGenerator

from telemetry_bus.events import GovernanceEvent, Topic

log = logging.getLogger(__name__)

# Sentinel object used to signal subscriber shutdown
_STOP = object()


class EventBus:
    """
    In-memory async Pub/Sub event bus.

    Usage
    -----
    async with EventBus() as bus:
        # Publisher (Plane 2 — gac_compiler)
        await bus.publish(event)

        # Subscriber (Plane 4 — recon_operator)
        async for event in bus.subscribe(Topic.POLICY_COMPILED, subscriber_id="recon"):
            await handle(event)

    Multiple concurrent publishers and subscribers are supported.
    """

    def __init__(self, *, maxsize: int = 0) -> None:
        """
        Parameters
        ----------
        maxsize:
            Maximum events per subscriber queue (0 = unlimited).
            Set to a positive integer to enable backpressure testing.
        """
        self._maxsize = maxsize
        # topic → {subscriber_id → Queue}
        self._queues: dict[Topic, dict[str, Queue]] = defaultdict(dict)
        # Monotonic sequence counter per topic (Kafka offset equivalent)
        self._sequences: dict[Topic, int] = defaultdict(int)
        self._running = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "EventBus":
        self._running = True
        log.info("EventBus started — %d topics available", len(Topic))
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Publisher API  (Plane 2, Plane 3, Plane 4 emit here)
    # ------------------------------------------------------------------

    async def publish(self, event: GovernanceEvent) -> int:
        """
        Publish an event to its topic.

        Assigns a monotonic sequence number to the event (Kafka offset).
        Fan-out: every registered subscriber on this topic receives a copy.

        Parameters
        ----------
        event:
            Any concrete GovernanceEvent subtype.

        Returns
        -------
        int
            The sequence number assigned to this event.
        """
        async with self._lock:
            seq = self._sequences[event.topic] + 1
            self._sequences[event.topic] = seq
            # Stamp the sequence onto the (frozen) event via dataclasses.replace
            stamped: GovernanceEvent = dataclasses.replace(event, sequence=seq)  # type: ignore[misc]

        topic_queues = self._queues.get(event.topic, {})
        for subscriber_id, q in topic_queues.items():
            await q.put(stamped)
            log.debug(
                "→ PUBLISH  topic=%-20s seq=%04d  subscriber='%s'  %s",
                event.topic.value,
                seq,
                subscriber_id,
                type(stamped).__name__,
            )

        if not topic_queues:
            log.debug(
                "→ PUBLISH  topic=%-20s seq=%04d  (no subscribers — event dropped)",
                event.topic.value,
                seq,
            )

        return seq

    # ------------------------------------------------------------------
    # Subscriber API  (Plane 4 reads here)
    # ------------------------------------------------------------------

    async def subscribe(
        self,
        topic: Topic,
        *,
        subscriber_id: str,
        timeout: float | None = None,
    ) -> AsyncGenerator[GovernanceEvent, None]:
        """
        Subscribe to a topic and yield events as they arrive.

        This is an async generator — use it in an ``async for`` loop.
        The loop exits cleanly when the bus is closed or ``timeout``
        elapses with no new event.

        Parameters
        ----------
        topic:
            The Topic enum value to subscribe to (e.g., Topic.ATLAS_HOOK).
        subscriber_id:
            A unique string identifying this subscriber.
            Multiple subscribers with different IDs each receive all events
            (fan-out / broadcast semantics, like separate Kafka consumer groups).
        timeout:
            Optional float seconds. If set, the generator yields None-free
            events until no event arrives within ``timeout`` seconds, then stops.
            If None, the generator runs until the bus is closed.

        Yields
        ------
        GovernanceEvent
            Each event published on the topic after subscription starts.
        """
        q: Queue = Queue(maxsize=self._maxsize)
        async with self._lock:
            self._queues[topic][subscriber_id] = q

        log.info(
            "← SUBSCRIBE topic=%-20s subscriber='%s'",
            topic.value,
            subscriber_id,
        )

        try:
            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    log.debug(
                        "← TIMEOUT   topic=%-20s subscriber='%s' — exiting",
                        topic.value,
                        subscriber_id,
                    )
                    return

                if item is _STOP:
                    log.debug(
                        "← STOP      topic=%-20s subscriber='%s' — bus closed",
                        topic.value,
                        subscriber_id,
                    )
                    return

                log.debug(
                    "← CONSUME   topic=%-20s seq=%04d  subscriber='%s'  %s",
                    topic.value,
                    item.sequence,  # type: ignore[attr-defined]
                    subscriber_id,
                    type(item).__name__,
                )
                yield item  # type: ignore[misc]
        finally:
            async with self._lock:
                self._queues[topic].pop(subscriber_id, None)

    # ------------------------------------------------------------------
    # Bus lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """
        Shut down the bus. Sends a STOP sentinel to every subscriber queue
        so all async-for loops exit gracefully.
        """
        self._running = False
        async with self._lock:
            for topic, subscribers in self._queues.items():
                for sub_id, q in subscribers.items():
                    await q.put(_STOP)
                    log.debug("STOP → topic=%s subscriber='%s'", topic.value, sub_id)
        log.info("EventBus closed.")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, object]:
        """Return a snapshot of current bus state (topic → sequence + subscriber count)."""
        return {
            topic.value: {
                "sequence": self._sequences[topic],
                "subscribers": list(self._queues.get(topic, {}).keys()),
                "pending_per_subscriber": {
                    sub_id: q.qsize()
                    for sub_id, q in self._queues.get(topic, {}).items()
                },
            }
            for topic in Topic
        }

    def __repr__(self) -> str:
        total_events = sum(self._sequences.values())
        return f"EventBus(topics={len(Topic)}, total_published={total_events}, running={self._running})"
