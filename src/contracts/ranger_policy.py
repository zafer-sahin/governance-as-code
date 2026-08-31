"""
governance/src/contracts/ranger_policy.py

Immutable domain model for Apache Ranger policies.

Design:
  - All types are frozen dataclasses (hashable, immutable, no __dict__ overhead).
  - Uses tuple (not list) for all collections — preserves immutability guarantee.
  - Enums for closed value sets (PolicyEffect, ResourceType, AuditMode).
  - No ORM annotations, no I/O logic — pure domain objects.
  - RangerPolicy is the aggregate root.

Relationship to GaC architecture:
  - Plane 2 (PolicyCompiler Outbound Port) produces RangerPolicy instances.
  - Plane 1 (LocalProxy CQRS) reads RangerPolicy as query result.
  - Plane 4 (ReconOperator) diffs RangerPolicy trees against actual state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto, unique
from typing import Optional
from uuid import UUID


# ---------------------------------------------------------------------------
# Value Enumerations — closed, exhaustive sets
# ---------------------------------------------------------------------------


@unique
class PolicyEffect(Enum):
    """The terminal decision of a policy evaluation."""

    ALLOW = auto()
    DENY = auto()


@unique
class ResourceType(Enum):
    """Ranger resource hierarchy discriminant."""

    DATABASE = "database"
    TABLE = "table"
    COLUMN = "column"
    HDFS_PATH = "hdfs-path"
    KAFKA_TOPIC = "kafka-topic"
    HIVE_SERVICE = "hive-service"
    TRINO_CATALOG = "trino-catalog"


@unique
class AuditMode(Enum):
    """Controls whether access events are written to the audit log."""

    ENABLED = auto()
    DISABLED = auto()


@unique
class MaskType(Enum):
    """Column-level masking strategy — maps to Ranger masking conditions."""

    NONE = "NONE"
    MASK = "MASK"                            # Replace with X's
    MASK_SHOW_LAST_4 = "MASK_SHOW_LAST_4"   # e.g., ****-1234
    MASK_SHOW_FIRST_4 = "MASK_SHOW_FIRST_4"
    HASH = "HASH"                            # SHA-256 of original value
    NULLIFY = "NULLIFY"                      # Return NULL
    REDACT = "REDACT"                        # Replace with fixed string
    CUSTOM = "CUSTOM"                        # Expression-based


# ---------------------------------------------------------------------------
# Component value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RangerResource:
    """
    A single resource specifier within a Ranger policy.

    Fields:
        resource_type: The resource hierarchy level.
        values:        One or more specific resource names or glob patterns.
        is_recursive:  If True, applies to all child resources (e.g., all tables in DB).
        is_exclusion:  If True, this resource is negated (exclude-semantics).
    """

    resource_type: ResourceType
    values: tuple[str, ...]
    is_recursive: bool = False
    is_exclusion: bool = False

    def __post_init__(self) -> None:
        if not self.values:
            raise ValueError(
                f"RangerResource of type {self.resource_type} must have at least one value."
            )


@dataclass(frozen=True, slots=True)
class RangerPrincipal:
    """
    A subject to whom a policy condition applies.

    Exactly one of (users, groups, roles) must be non-empty.
    """

    users: tuple[str, ...] = field(default_factory=tuple)
    groups: tuple[str, ...] = field(default_factory=tuple)
    roles: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not (self.users or self.groups or self.roles):
            raise ValueError(
                "RangerPrincipal must define at least one of: users, groups, roles."
            )


@dataclass(frozen=True, slots=True)
class RangerCondition:
    """
    An optional custom condition expression evaluated during policy matching.

    condition_type: Ranger-registered condition evaluator name.
    values:         Arguments passed to the condition evaluator.
    """

    condition_type: str
    values: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RangerMaskingSpec:
    """
    Column-level masking specification attached to a policy item.

    Only valid on policies of type MASKING.
    mask_value is only populated when mask_type == MaskType.CUSTOM.
    """

    mask_type: MaskType
    mask_value: Optional[str] = None  # custom expression when mask_type == CUSTOM

    def __post_init__(self) -> None:
        if self.mask_type != MaskType.CUSTOM and self.mask_value is not None:
            raise ValueError(
                "mask_value may only be set when mask_type is CUSTOM."
            )


@dataclass(frozen=True, slots=True)
class RangerPolicyItem:
    """
    A single (principal, permissions, conditions, effect) tuple within a policy.

    accesses:   Tuple of permission names (e.g., "select", "update", "read").
    principal:  The subjects this item applies to.
    conditions: Optional evaluator conditions (e.g., IP-range, time-window).
    effect:     ALLOW or DENY.
    delegate:   If True, the principal may delegate this permission.
    masking:    Column-level masking spec; only set on masking policy items.
    """

    accesses: tuple[str, ...]
    principal: RangerPrincipal
    conditions: tuple[RangerCondition, ...] = field(default_factory=tuple)
    effect: PolicyEffect = PolicyEffect.ALLOW
    delegate: bool = False
    masking: Optional[RangerMaskingSpec] = None

    def __post_init__(self) -> None:
        if not self.accesses:
            raise ValueError("RangerPolicyItem must declare at least one access permission.")


# ---------------------------------------------------------------------------
# Aggregate Root
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RangerPolicy:
    """
    Immutable aggregate root representing a complete Apache Ranger policy.

    This is the canonical domain object produced by the PolicyCompiler
    (Plane 2 Outbound Port) and read by the LocalProxy (Plane 1 CQRS)
    and the ReconOperator (Plane 4 Control Loop).

    Fields:
        policy_id:      Stable UUID assigned at creation; None for drafts.
        name:           Human-readable policy name (unique per service).
        service:        Ranger service name (e.g., "cm_hive", "trino_prod").
        resources:      Tuple of RangerResource objects forming the resource path.
        policy_items:   Ordered tuple of RangerPolicyItem grant/deny rules.
        is_enabled:     Whether the policy is active.
        is_audit_enabled: Whether access events are sent to the audit log.
        description:    Free-text documentation string.
        labels:         Tuple of governance labels (e.g., "pii", "regulated").
        version:        Monotonically increasing version counter (0 = unpublished).
        created_at:     UTC timestamp of policy creation.
        updated_at:     UTC timestamp of last policy update.
        git_sha:        The Git commit SHA from which this policy was compiled.
    """

    name: str
    service: str
    resources: tuple[RangerResource, ...]
    policy_items: tuple[RangerPolicyItem, ...]
    is_enabled: bool = True
    is_audit_enabled: bool = True
    description: str = ""
    labels: tuple[str, ...] = field(default_factory=tuple)
    version: int = 0
    policy_id: Optional[UUID] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    git_sha: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("RangerPolicy.name must not be empty.")
        if not self.service:
            raise ValueError("RangerPolicy.service must not be empty.")
        if not self.resources:
            raise ValueError(
                f"RangerPolicy '{self.name}' must define at least one resource."
            )
        if not self.policy_items:
            raise ValueError(
                f"RangerPolicy '{self.name}' must define at least one policy item."
            )

    # ------------------------------------------------------------------
    # Derived queries (no mutation)
    # ------------------------------------------------------------------

    def with_git_sha(self, sha: str) -> "RangerPolicy":
        """Return a new RangerPolicy stamped with the given Git commit SHA."""
        import dataclasses
        return dataclasses.replace(self, git_sha=sha)

    def with_version(self, version: int) -> "RangerPolicy":
        """Return a new RangerPolicy with an incremented version counter."""
        import dataclasses
        return dataclasses.replace(self, version=version)

    def allow_items(self) -> tuple[RangerPolicyItem, ...]:
        """Return only the ALLOW-effect policy items."""
        return tuple(i for i in self.policy_items if i.effect == PolicyEffect.ALLOW)

    def deny_items(self) -> tuple[RangerPolicyItem, ...]:
        """Return only the DENY-effect policy items."""
        return tuple(i for i in self.policy_items if i.effect == PolicyEffect.DENY)

    def masking_items(self) -> tuple[RangerPolicyItem, ...]:
        """Return policy items that carry a masking specification."""
        return tuple(i for i in self.policy_items if i.masking is not None)
