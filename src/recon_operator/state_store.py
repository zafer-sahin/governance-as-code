"""
governance/src/recon_operator/state_store.py

In-memory Actual State Store for the Reconciliation Operator (Plane 4).

Responsibility:
  Build and maintain the "actual state" dictionary from events received
  via the Telemetry Bus (Plane 3). This is the live runtime state that
  the Control Loop compares against the desired state from Git.

Design:
  - StateStore is an async context manager; run it inside the control loop.
  - It subscribes to POLICY_COMPILED and ATLAS_HOOK topics on the bus.
  - Each incoming event updates the internal state dict (keyed by resource_id).
  - actual_state() returns a frozen snapshot suitable for diff().
  - Thread-safe: all mutations happen inside the asyncio event loop (single-threaded).

State structure produced
------------------------
{
  "ranger_policies": {
    "<policy_name>": { ...policy dict from PolicyCompiledEvent... }
  },
  "atlas_entities": {
    "<qualified_name>": { ...entity dict from LineageEvent or EntityRegisteredEvent... }
  },
  "lineage_edges": [
    { "source": ..., "target": ..., "process": ..., "source_system": ... }
  ]
}
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections import defaultdict
from typing import Any

from telemetry_bus.events import (
    EntityRegisteredEvent,
    LineageEvent,
    PolicyCompiledEvent,
    Topic,
)

log = logging.getLogger(__name__)


class StateStore:
    """
    Mutable in-memory actual-state registry, populated from bus events.

    Usage
    -----
    store = StateStore(bus)
    asyncio.create_task(store.run())   # starts listening; call stop() to halt
    snapshot = store.actual_state()   # returns a deep-copied snapshot
    """

    def __init__(self, bus: Any) -> None:  # bus: EventBus (avoid circular import)
        self._bus = bus
        self._ranger_policies: dict[str, dict[str, Any]] = {}
        self._atlas_entities:  dict[str, dict[str, Any]] = {}
        self._lineage_edges:   list[dict[str, Any]] = []
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start background subscription tasks."""
        self._stop_event.clear()
        self._task = asyncio.create_task(self._listen_all(), name="StateStore.listen")
        log.info("[StateStore] Started — listening on POLICY_COMPILED + ATLAS_HOOK + ENTITY_REGISTERED")

    async def stop(self) -> None:
        """Signal background tasks to stop and await completion."""
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("[StateStore] Stopped.")

    # ------------------------------------------------------------------
    # Snapshot (read path — zero mutation)
    # ------------------------------------------------------------------

    def actual_state(self) -> dict[str, Any]:
        """
        Return a deep copy of the current actual state.
        Safe to call at any time; does not block.

        Returns
        -------
        dict with keys:
          "ranger_policies" : dict[policy_name → policy_dict]
          "atlas_entities"  : dict[qualified_name → entity_dict]
          "lineage_edges"   : list of edge dicts
        """
        return {
            "ranger_policies": copy.deepcopy(self._ranger_policies),
            "atlas_entities":  copy.deepcopy(self._atlas_entities),
            "lineage_edges":   copy.deepcopy(self._lineage_edges),
        }

    # ------------------------------------------------------------------
    # Event handlers (write path)
    # ------------------------------------------------------------------

    def _apply_policy_compiled(self, event: PolicyCompiledEvent) -> None:
        """
        Update ranger_policies section from a PolicyCompiledEvent.
        Treats each policy_name as a key — records that it was compiled.
        """
        for name in event.policy_names:
            self._ranger_policies[name] = {
                "name":        name,
                "git_sha":     event.git_sha,
                "plan_id":     event.plan_id,
                "compiled_by": event.requesting_principal,
                "output_dir":  event.output_dir,
                "status":      "COMPILED",
            }
        for name in event.entity_names:
            # Stub: mark entity as known in actual state
            if name not in self._atlas_entities:
                self._atlas_entities[name] = {
                    "qualified_name": name,
                    "source_system":  "gac_compiler",
                    "git_sha":        event.git_sha,
                    "status":         "REGISTERED",
                }
        log.debug(
            "[StateStore] PolicyCompiledEvent applied — "
            "%d policies, %d entities now in actual state",
            len(event.policy_names), len(event.entity_names),
        )

    def _apply_lineage_event(self, event: LineageEvent) -> None:
        """Append a lineage edge to the actual state."""
        edge = {
            "source":       event.source_table,
            "target":       event.target_table,
            "process":      event.process_name,
            "source_system": event.source_system,
            "job_id":       event.job_id,
            "tags":         list(event.classification_tags),
        }
        self._lineage_edges.append(edge)
        # Also mark target entity as known
        if event.target_table not in self._atlas_entities:
            self._atlas_entities[event.target_table] = {
                "qualified_name": event.target_table,
                "source_system":  event.source_system,
                "status":         "LINEAGE_OBSERVED",
            }
        log.debug(
            "[StateStore] LineageEvent applied — %s → %s via %s",
            event.source_table, event.target_table, event.source_system,
        )

    def _apply_entity_registered(self, event: EntityRegisteredEvent) -> None:
        """Update atlas_entities with a confirmed registration."""
        self._atlas_entities[event.qualified_name] = {
            "qualified_name": event.qualified_name,
            "type_name":      event.type_name,
            "guid":           event.guid,
            "source_system":  event.source_system,
            "status":         "REGISTERED",
        }
        log.debug("[StateStore] EntityRegisteredEvent applied — %s", event.qualified_name)

    # ------------------------------------------------------------------
    # Background listener
    # ------------------------------------------------------------------

    async def _listen_all(self) -> None:
        """Run all topic listeners concurrently."""
        await asyncio.gather(
            self._listen_policy_compiled(),
            self._listen_atlas_hook(),
            self._listen_entity_registered(),
        )

    async def _listen_policy_compiled(self) -> None:
        async for event in self._bus.subscribe(
            Topic.POLICY_COMPILED,
            subscriber_id="state-store-policy",
            timeout=0.5,
        ):
            if self._stop_event.is_set():
                return
            if isinstance(event, PolicyCompiledEvent):
                self._apply_policy_compiled(event)

    async def _listen_atlas_hook(self) -> None:
        async for event in self._bus.subscribe(
            Topic.ATLAS_HOOK,
            subscriber_id="state-store-atlas-hook",
            timeout=0.5,
        ):
            if self._stop_event.is_set():
                return
            if isinstance(event, LineageEvent):
                self._apply_lineage_event(event)

    async def _listen_entity_registered(self) -> None:
        async for event in self._bus.subscribe(
            Topic.ENTITY_REGISTERED,
            subscriber_id="state-store-entity",
            timeout=0.5,
        ):
            if self._stop_event.is_set():
                return
            if isinstance(event, EntityRegisteredEvent):
                self._apply_entity_registered(event)
