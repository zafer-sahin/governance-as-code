"""
governance/src/telemetry_bus/events.py

Governance event types for the in-memory Telemetry Bus (Plane 3).

Design:
  - All events are frozen dataclasses — immutable value objects.
  - GovernanceEvent is the sealed base; concrete variants represent
    distinct event categories that flow through the bus.
  - topic field mirrors Kafka topic names from the architecture docs
    (e.g., ATLAS_HOOK, POLICY_COMPILED, DRIFT_DETECTED).
  - Each event carries a monotonically increasing sequence_id for
    ordering guarantees (Kafka offset equivalent) assigned by the bus.

Kafka topic equivalents
-----------------------
ATLAS_HOOK         → lineage/metadata events from Spark hooks, Trino hooks, DiscoveryEngine
POLICY_COMPILED    → emitted by Plane 2 (gac_compiler) after compile_plan()
ENTITY_REGISTERED  → emitted by Plane 3 after Atlas entity ingest
DRIFT_DETECTED     → emitted by Plane 4 (recon_operator) when drift found
REMEDIATION_DONE   → emitted by Plane 4 after auto-remediation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto, unique
from typing import Any
import uuid


# ---------------------------------------------------------------------------
# Topic enum — mirrors Kafka topic names
# ---------------------------------------------------------------------------

@unique
class Topic(str, Enum):
    """Named event channels. Equivalent to Kafka topic names."""
    ATLAS_HOOK         = "ATLAS_HOOK"           # Plane 3 → Atlas
    POLICY_COMPILED    = "POLICY_COMPILED"       # Plane 2 → Plane 4
    ENTITY_REGISTERED  = "ENTITY_REGISTERED"     # Plane 3 → Plane 4
    DRIFT_DETECTED     = "DRIFT_DETECTED"        # Plane 4 internal
    REMEDIATION_DONE   = "REMEDIATION_DONE"      # Plane 4 → observers


# ---------------------------------------------------------------------------
# Sealed base
# ---------------------------------------------------------------------------

_EVENT_SEALED: frozenset[str] = frozenset({
    "PolicyCompiledEvent",
    "LineageEvent",
    "EntityRegisteredEvent",
    "DriftDetectedEvent",
    "RemediationDoneEvent",
})


class GovernanceEvent:
    """
    Abstract sealed base for all bus events.

    Concrete variants must be in _EVENT_SEALED.
    Every event is assigned:
      event_id   : unique UUID (set at construction)
      emitted_at : UTC timestamp (set at construction)
      sequence   : monotonic offset assigned by the bus on publish
    """

    def __init_subclass__(cls, **kwargs: object) -> None:
        if cls.__name__ not in _EVENT_SEALED:
            raise TypeError(
                f"GovernanceEvent is sealed. '{cls.__name__}' not permitted. "
                f"Allowed: {sorted(_EVENT_SEALED)}"
            )
        super().__init_subclass__(**kwargs)

    @property
    def topic(self) -> Topic:  # pragma: no cover
        raise NotImplementedError

    def summary(self) -> str:  # pragma: no cover
        return repr(self)


# ---------------------------------------------------------------------------
# Concrete event variants
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PolicyCompiledEvent(GovernanceEvent):
    """
    Emitted by Plane 2 (gac_compiler) after a successful compile_plan().

    Consumed by Plane 4 (recon_operator) to trigger state reconciliation.

    Fields
    ------
    plan_id         : The Trino plan query ID that was compiled.
    git_sha         : The Git commit SHA of the triggering PR.
    policy_names    : Names of all RangerPolicy objects compiled.
    typedef_names   : Names of all AtlasTypeDef objects compiled.
    entity_names    : Qualified names of all AtlasEntity objects compiled.
    output_dir      : Path to the output directory written by the Outbound Port.
    requesting_principal : CI/CD service account that triggered compilation.
    event_id        : Unique event UUID.
    emitted_at      : UTC emission timestamp.
    sequence        : Monotonic offset assigned by the bus (0 = unassigned).
    """
    plan_id: str
    git_sha: str
    policy_names: tuple[str, ...]
    typedef_names: tuple[str, ...]
    entity_names: tuple[str, ...]
    output_dir: str
    requesting_principal: str = "ci-cd-runner"
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    @property
    def topic(self) -> Topic:
        return Topic.POLICY_COMPILED

    def summary(self) -> str:
        return (
            f"PolicyCompiledEvent[seq={self.sequence}] "
            f"git={self.git_sha[:8]} "
            f"policies={len(self.policy_names)} "
            f"typedefs={len(self.typedef_names)}"
        )


@dataclass(frozen=True, slots=True)
class LineageEvent(GovernanceEvent):
    """
    Emitted by Plane 3 actors (native Spark/Trino hook or discovery scanner).
    Published to the ATLAS_HOOK topic — Atlas consumes at its own pace.

    Fields
    ------
    source_table    : Upstream table qualified name.
    target_table    : Downstream table qualified name.
    process_name    : Job or query name that produced the lineage edge.
    source_system   : "spark-hook" | "trino-hook" | "discovery-engine"
    job_id          : Job/session/scan identifier.
    classification_tags : Tags carried by this lineage event (e.g., ["PII"]).
    event_id        : Unique event UUID.
    emitted_at      : UTC emission timestamp.
    sequence        : Monotonic offset assigned by the bus (0 = unassigned).
    """
    source_table: str
    target_table: str
    process_name: str
    source_system: str
    job_id: str
    classification_tags: tuple[str, ...] = field(default_factory=tuple)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    @property
    def topic(self) -> Topic:
        return Topic.ATLAS_HOOK

    def summary(self) -> str:
        return (
            f"LineageEvent[seq={self.sequence}] "
            f"{self.source_table} → {self.target_table} "
            f"via={self.process_name} "
            f"src={self.source_system} "
            f"tags={self.classification_tags}"
        )


@dataclass(frozen=True, slots=True)
class EntityRegisteredEvent(GovernanceEvent):
    """
    Emitted after an Atlas entity is persisted (from Kafka consumer or direct ingest).

    Fields
    ------
    qualified_name  : The entity that was registered.
    type_name       : The Atlas type name of the entity.
    guid            : The Atlas-assigned GUID (or deterministic stub UUID).
    source_system   : System that triggered registration.
    event_id, emitted_at, sequence: standard event envelope.
    """
    qualified_name: str
    type_name: str
    guid: str
    source_system: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    @property
    def topic(self) -> Topic:
        return Topic.ENTITY_REGISTERED

    def summary(self) -> str:
        return (
            f"EntityRegisteredEvent[seq={self.sequence}] "
            f"{self.qualified_name} ({self.type_name}) "
            f"guid={self.guid[:8]}… src={self.source_system}"
        )


@dataclass(frozen=True, slots=True)
class DriftDetectedEvent(GovernanceEvent):
    """
    Emitted by Plane 4 (recon_operator) when desired state ≠ actual state.

    Fields
    ------
    dimension       : "ranger" | "atlas" | "model-registry"
    resource_id     : Policy name, entity qualified_name, or model ID.
    desired_hash    : Hash of the desired-state payload.
    actual_hash     : Hash of the actual-state payload.
    severity        : "LOW" | "MEDIUM" | "HIGH" — extent of the drift.
    event_id, emitted_at, sequence: standard event envelope.
    """
    dimension: str
    resource_id: str
    desired_hash: str
    actual_hash: str
    severity: str = "MEDIUM"
    verbose_report: str = ""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    @property
    def topic(self) -> Topic:
        return Topic.DRIFT_DETECTED

    def summary(self) -> str:
        return (
            f"DriftDetectedEvent[seq={self.sequence}] "
            f"dim={self.dimension} resource={self.resource_id} "
            f"severity={self.severity}"
        )


@dataclass(frozen=True, slots=True)
class RemediationDoneEvent(GovernanceEvent):
    """
    Emitted by Plane 4 after a successful auto-remediation patch.

    Fields
    ------
    dimension       : "ranger" | "atlas" | "model-registry"
    resource_id     : The resource that was patched.
    drift_event_id  : event_id of the DriftDetectedEvent that triggered this.
    patch_applied   : Human-readable description of the patch applied.
    event_id, emitted_at, sequence: standard event envelope.
    """
    dimension: str
    resource_id: str
    drift_event_id: str
    patch_applied: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sequence: int = 0

    @property
    def topic(self) -> Topic:
        return Topic.REMEDIATION_DONE

    def summary(self) -> str:
        return (
            f"RemediationDoneEvent[seq={self.sequence}] "
            f"dim={self.dimension} resource={self.resource_id} "
            f"patch='{self.patch_applied}'"
        )
