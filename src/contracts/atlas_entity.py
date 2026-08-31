"""
governance/src/contracts/atlas_entity.py

Immutable domain model for Apache Atlas entities.

Design:
  - All types are frozen dataclasses (hashable, zero mutation surface).
  - Uses tuple for ordered collections, frozenset for unordered unique sets.
  - EntityStatus, ClassificationPropagation are exhaustive enums.
  - AtlasEntity is the aggregate root. AtlasLineage composes two roots.
  - AtlasTypeDef models type system declarations separately from instances.

Relationship to GaC architecture:
  - Plane 2 (PolicyCompiler Outbound Port) produces AtlasTypeDef instances.
  - Plane 3 (DiscoveryEngine + native hooks) produces AtlasEntity + AtlasLineage via Kafka.
  - Plane 4 (ReconOperator) diffs AtlasEntity state against desired Git state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto, unique
from typing import Any, Optional
from uuid import UUID


# ---------------------------------------------------------------------------
# Value Enumerations
# ---------------------------------------------------------------------------


@unique
class EntityStatus(Enum):
    """Lifecycle status of an Atlas entity."""

    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


@unique
class ClassificationPropagation(Enum):
    """Controls whether a classification propagates through lineage edges."""

    ENABLED = auto()
    DISABLED = auto()
    TO_PROPAGATED_ENTITIES = auto()


@unique
class LineageDirection(Enum):
    """Direction of a lineage query or edge."""

    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    BOTH = "BOTH"


@unique
class CardinalityType(Enum):
    """Attribute cardinality in Atlas type definitions."""

    SINGLE = "SINGLE"
    LIST = "LIST"
    SET = "SET"
    MAP = "MAP"


# ---------------------------------------------------------------------------
# Classification (Tag) — applied to entities and propagated through lineage
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AtlasClassification:
    """
    An immutable tag applied to an Atlas entity.

    name:             The registered classification type name (e.g., "PII", "Regulated").
    attributes:       Attribute key-value pairs defined by the classification's TypeDef.
    propagate:        Whether this tag propagates along lineage edges.
    validity_period:  Optional ISO-8601 duration string for time-bounded tags.
    """

    name: str
    attributes: tuple[tuple[str, Any], ...] = field(default_factory=tuple)
    propagate: ClassificationPropagation = ClassificationPropagation.ENABLED
    validity_period: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("AtlasClassification.name must not be empty.")

    def attributes_as_dict(self) -> dict[str, Any]:
        """Return attributes as a plain dict for serialisation."""
        return dict(self.attributes)


# ---------------------------------------------------------------------------
# Business Metadata — structured annotations attached to an entity
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AtlasBusinessMetadata:
    """
    A single business metadata namespace applied to an entity.

    namespace:   The registered BusinessMetadataDef name.
    attributes:  Attribute key-value pairs defined by the namespace.
    """

    namespace: str
    attributes: tuple[tuple[str, Any], ...]

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("AtlasBusinessMetadata.namespace must not be empty.")
        if not self.attributes:
            raise ValueError(
                f"AtlasBusinessMetadata '{self.namespace}' must have at least one attribute."
            )

    def attributes_as_dict(self) -> dict[str, Any]:
        return dict(self.attributes)


# ---------------------------------------------------------------------------
# Type System — TypeDef hierarchy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AtlasAttributeDef:
    """
    Definition of a single attribute within an Atlas type.

    name:            Attribute name.
    type_name:       Atlas primitive or composite type (e.g., "string", "int", "array<string>").
    cardinality:     Multiplicity of the attribute value.
    is_optional:     If False, the attribute is mandatory on entity creation.
    is_unique:       If True, Atlas enforces uniqueness across entities of this type.
    is_indexable:    If True, Atlas creates a search index on this attribute.
    description:     Free-text documentation for tooling.
    """

    name: str
    type_name: str
    cardinality: CardinalityType = CardinalityType.SINGLE
    is_optional: bool = True
    is_unique: bool = False
    is_indexable: bool = False
    description: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("AtlasAttributeDef.name must not be empty.")
        if not self.type_name:
            raise ValueError("AtlasAttributeDef.type_name must not be empty.")


@dataclass(frozen=True, slots=True)
class AtlasTypeDef:
    """
    Immutable declaration of an Atlas entity type.

    Produced by the PolicyCompiler Outbound Port (Plane 2) and registered
    via Atlas REST API. The ReconOperator (Plane 4) diffs registered TypeDefs
    against this desired-state object.

    type_name:        Unique type name in the Atlas type system.
    super_types:      Parent types this type inherits from (e.g., "DataSet", "Process").
    attribute_defs:   Ordered tuple of attribute definitions.
    description:      Human-readable documentation.
    service_type:     The Ranger/Atlas service that owns this type.
    git_sha:          Git commit SHA from which this typedef was compiled.
    """

    type_name: str
    super_types: tuple[str, ...]
    attribute_defs: tuple[AtlasAttributeDef, ...]
    description: str = ""
    service_type: str = "gac-governance"
    git_sha: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.type_name:
            raise ValueError("AtlasTypeDef.type_name must not be empty.")


# ---------------------------------------------------------------------------
# Entity — aggregate root
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AtlasEntity:
    """
    Immutable aggregate root representing a single Apache Atlas entity instance.

    An entity is a typed, identifiable metadata object (e.g., a Hive table,
    a Spark process, a Kafka topic) with classifications, business metadata,
    and relationships to other entities.

    Fields:
        guid:                Stable UUID assigned by Atlas on creation.
        type_name:           Must correspond to a registered AtlasTypeDef.
        qualified_name:      Globally unique identifier string (Atlas uniqueness key).
                             Convention: <service>.<db>.<table>@<cluster>
        attributes:          Entity attributes as immutable key-value pairs.
        status:              ACTIVE or DELETED lifecycle state.
        classifications:     Zero or more applied classification tags.
        business_metadata:   Zero or more structured business metadata blocks.
        relationship_guids:  Tuple of GUIDs of related entities (foreign keys).
        created_by:          Username or service account that created this entity.
        updated_by:          Username or service account of last update.
        create_time:         UTC creation timestamp.
        update_time:         UTC last-update timestamp.
        version:             Atlas-internal optimistic locking version.
        source_system:       Originating system (e.g., "discovery-engine", "spark-hook", "trino-hook").
        git_sha:             Git commit SHA if the entity was declared as IaC.
    """

    type_name: str
    qualified_name: str
    attributes: tuple[tuple[str, Any], ...]
    guid: Optional[UUID] = None
    status: EntityStatus = EntityStatus.ACTIVE
    classifications: tuple[AtlasClassification, ...] = field(default_factory=tuple)
    business_metadata: tuple[AtlasBusinessMetadata, ...] = field(default_factory=tuple)
    relationship_guids: tuple[UUID, ...] = field(default_factory=tuple)
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    version: int = 0
    source_system: Optional[str] = None
    git_sha: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.type_name:
            raise ValueError("AtlasEntity.type_name must not be empty.")
        if not self.qualified_name:
            raise ValueError("AtlasEntity.qualified_name must not be empty.")

    # ------------------------------------------------------------------
    # Derived queries (no mutation)
    # ------------------------------------------------------------------

    def attributes_as_dict(self) -> dict[str, Any]:
        """Return entity attributes as a plain dict for serialisation."""
        return dict(self.attributes)

    def classification_names(self) -> frozenset[str]:
        """Return the set of applied classification names."""
        return frozenset(c.name for c in self.classifications)

    def has_classification(self, name: str) -> bool:
        """Check whether a specific classification is applied."""
        return name in self.classification_names()

    def with_status(self, status: EntityStatus) -> "AtlasEntity":
        """Return a new AtlasEntity with updated lifecycle status."""
        import dataclasses
        return dataclasses.replace(self, status=status)

    def with_git_sha(self, sha: str) -> "AtlasEntity":
        """Return a new AtlasEntity stamped with a Git commit SHA."""
        import dataclasses
        return dataclasses.replace(self, git_sha=sha)

    def with_guid(self, guid: UUID) -> "AtlasEntity":
        """Return a new AtlasEntity stamped with the Atlas-assigned GUID."""
        import dataclasses
        return dataclasses.replace(self, guid=guid)


# ---------------------------------------------------------------------------
# Lineage — directed edge between two AtlasEntity aggregates
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AtlasLineageEdge:
    """
    A single directed lineage edge between two Atlas entity GUIDs.

    from_entity_guid: Source entity (input / upstream).
    to_entity_guid:   Sink entity (output / downstream).
    process_guid:     Optional GUID of the Process entity (Spark job, Trino query)
                      that produced this edge.
    """

    from_entity_guid: UUID
    to_entity_guid: UUID
    process_guid: Optional[UUID] = None


@dataclass(frozen=True, slots=True)
class AtlasLineage:
    """
    Immutable lineage graph snapshot rooted at a given entity.

    Produced by Plane 3 (native hooks + DiscoveryEngine) and persisted to Atlas.
    Read by Plane 4 (ReconOperator) for drift comparison.

    root_entity_guid:  The entity at the centre of this lineage snapshot.
    direction:         INPUT (upstream), OUTPUT (downstream), or BOTH.
    edges:             All edges in the captured lineage sub-graph.
    depth:             Number of hops captured from the root entity.
    source_system:     System that emitted this lineage (e.g., "spark-hook", "discovery-engine").
    captured_at:       UTC timestamp of lineage capture.
    """

    root_entity_guid: UUID
    direction: LineageDirection
    edges: tuple[AtlasLineageEdge, ...]
    depth: int = 1
    source_system: Optional[str] = None
    captured_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.depth < 1:
            raise ValueError(f"AtlasLineage.depth must be >= 1, got {self.depth}.")

    def upstream_guids(self) -> frozenset[UUID]:
        """Return the set of all upstream (input) entity GUIDs."""
        return frozenset(e.from_entity_guid for e in self.edges)

    def downstream_guids(self) -> frozenset[UUID]:
        """Return the set of all downstream (output) entity GUIDs."""
        return frozenset(e.to_entity_guid for e in self.edges)
