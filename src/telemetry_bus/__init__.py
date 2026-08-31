"""
governance/src/telemetry_bus/__init__.py

Public API for the Telemetry Bus (Plane 3 — In-Memory Event Plane).

Import surface:

    from telemetry_bus import EventBus, Topic
    from telemetry_bus import (
        PolicyCompiledEvent, LineageEvent, EntityRegisteredEvent,
        DriftDetectedEvent, RemediationDoneEvent,
    )
"""

from telemetry_bus.event_bus import EventBus
from telemetry_bus.events import (
    DriftDetectedEvent,
    EntityRegisteredEvent,
    GovernanceEvent,
    LineageEvent,
    PolicyCompiledEvent,
    RemediationDoneEvent,
    Topic,
)

__all__ = [
    "EventBus",
    "Topic",
    "GovernanceEvent",
    "PolicyCompiledEvent",
    "LineageEvent",
    "EntityRegisteredEvent",
    "DriftDetectedEvent",
    "RemediationDoneEvent",
]
