"""
governance/src/contracts/failures.py

Structured failure descriptors for the GaC contract layer.

Design:
  - Sealed hierarchy: GovernanceFailure is the abstract base.
    Subclasses represent distinct failure categories — callers
    pattern-match exhaustively using fold() on Result[S, GovernanceFailure].
  - All failures are frozen dataclasses (immutable value objects).
  - No exceptions are raised here; failures are returned as values.

These failure types are the F type-parameter in Result[S, F].

Failure taxonomy aligned to the 4-plane GaC architecture:
  - ValidationFailure   → Plane 1 (LocalProxy dry-run)
  - PolicyFailure       → Plane 2 (PolicyCompiler provisioning)
  - MetadataFailure     → Plane 2 / Plane 3 (Atlas registration / lineage ingest)
  - ReconciliationFailure → Plane 4 (ReconOperator drift remediation)
  - TransportFailure    → Any plane (network/HTTP/Kafka errors)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto, unique
from typing import Optional


# ---------------------------------------------------------------------------
# Sealed base
# ---------------------------------------------------------------------------

_FAILURE_SEALED: frozenset[str] = frozenset(
    {
        "ValidationFailure",
        "PolicyFailure",
        "MetadataFailure",
        "ReconciliationFailure",
        "TransportFailure",
    }
)


class GovernanceFailure:
    """
    Abstract sealed base for all GaC failure descriptors.

    Use as the F type-parameter: Result[S, GovernanceFailure]
    """

    def __init_subclass__(cls, **kwargs: object) -> None:
        if cls.__name__ not in _FAILURE_SEALED:
            raise TypeError(
                f"GovernanceFailure is sealed. '{cls.__name__}' is not permitted. "
                f"Allowed subtypes: {sorted(_FAILURE_SEALED)}"
            )
        super().__init_subclass__(**kwargs)

    def human_readable(self) -> str:  # pragma: no cover
        """Override in subclasses to provide a log-friendly message."""
        return repr(self)


# ---------------------------------------------------------------------------
# Plane 1 — Validation failures (LocalProxy CQRS dry-run)
# ---------------------------------------------------------------------------


@unique
class ValidationCode(Enum):
    """Exhaustive set of dry-run validation failure reasons."""

    SCHEMA_NOT_FOUND = auto()
    COLUMN_NOT_FOUND = auto()
    ACCESS_DENIED = auto()
    MASKING_REQUIRED = auto()
    MODEL_RESTRICT = auto()          # Agentic AI / Private AI model constraint
    INVALID_QUERY_SYNTAX = auto()
    CLASSIFICATION_MISSING = auto()  # Required PII tag absent


@dataclass(frozen=True, slots=True)
class ValidationFailure(GovernanceFailure):
    """
    Returned by LocalProxy when a dry-run check fails.

    code:            Machine-readable failure discriminant.
    resource:        The resource path that triggered the failure.
    principal:       The identity that attempted the operation.
    reason:          Human-readable explanation.
    violated_rule:   The Ranger policy or Atlas constraint that was violated.
    constraint_id:   For MODEL_RESTRICT failures: the model registry constraint identifier.
    masked_columns:  For MASKING_REQUIRED failures: columns requiring masking.
    """

    code: ValidationCode
    resource: str
    principal: str
    reason: str
    violated_rule: Optional[str] = None
    constraint_id: Optional[str] = None
    masked_columns: tuple[str, ...] = field(default_factory=tuple)

    def human_readable(self) -> str:
        return (
            f"[ValidationFailure/{self.code.name}] "
            f"principal='{self.principal}' resource='{self.resource}' "
            f"reason='{self.reason}'"
        )


# ---------------------------------------------------------------------------
# Plane 2 — Policy provisioning failures (PolicyCompiler Outbound Port)
# ---------------------------------------------------------------------------


@unique
class PolicyFailureCode(Enum):
    """Failure codes from Ranger / Model Registry provisioning operations."""

    RANGER_API_ERROR = auto()
    RANGER_CONFLICT = auto()        # Policy name collision
    RANGER_VALIDATION_ERROR = auto()
    MODEL_REGISTRY_API_ERROR = auto()
    MODEL_CONTRACT_CONFLICT = auto()
    COMPILATION_ERROR = auto()      # Diff → policy inference failed
    STATE_MACHINE_VIOLATION = auto()  # Invalid PR lifecycle transition


@dataclass(frozen=True, slots=True)
class PolicyFailure(GovernanceFailure):
    """
    Returned by the PolicyCompiler or ModelGovCompiler outbound ports.

    code:        Machine-readable failure discriminant.
    policy_name: The policy name involved (if applicable).
    http_status: The HTTP status code returned by the remote API.
    message:     Raw error message from the upstream system.
    pr_id:       The GitHub PR ID that triggered this compilation attempt.
    """

    code: PolicyFailureCode
    message: str
    policy_name: Optional[str] = None
    http_status: Optional[int] = None
    pr_id: Optional[str] = None

    def human_readable(self) -> str:
        return (
            f"[PolicyFailure/{self.code.name}] "
            f"policy='{self.policy_name}' http={self.http_status} "
            f"message='{self.message}'"
        )


# ---------------------------------------------------------------------------
# Plane 2 / Plane 3 — Metadata failures (Atlas registration, lineage ingest)
# ---------------------------------------------------------------------------


@unique
class MetadataFailureCode(Enum):
    """Failure codes for Atlas entity and lineage operations."""

    ATLAS_API_ERROR = auto()
    TYPEDEF_CONFLICT = auto()      # TypeDef already exists with incompatible schema
    ENTITY_NOT_FOUND = auto()
    LINEAGE_CYCLE_DETECTED = auto()
    GUID_COLLISION = auto()
    KAFKA_PUBLISH_ERROR = auto()   # Telemetry bus write failure
    DISCOVERY_SCAN_ERROR = auto()    # Discovery engine scan failure


@dataclass(frozen=True, slots=True)
class MetadataFailure(GovernanceFailure):
    """
    Returned when Atlas entity registration, TypeDef creation, or lineage
    ingest fails — from any of Plane 2, Plane 3 (native hooks, DiscoveryEngine).

    code:           Machine-readable failure discriminant.
    qualified_name: The entity qualified_name involved (if applicable).
    type_name:      The entity type name involved (if applicable).
    http_status:    HTTP status code from Atlas REST API.
    message:        Raw error from upstream.
    source_system:  Originating system (e.g., "discovery-engine", "spark-hook").
    """

    code: MetadataFailureCode
    message: str
    qualified_name: Optional[str] = None
    type_name: Optional[str] = None
    http_status: Optional[int] = None
    source_system: Optional[str] = None

    def human_readable(self) -> str:
        return (
            f"[MetadataFailure/{self.code.name}] "
            f"entity='{self.qualified_name}' type='{self.type_name}' "
            f"http={self.http_status} source='{self.source_system}' "
            f"message='{self.message}'"
        )


# ---------------------------------------------------------------------------
# Plane 4 — Reconciliation failures (ReconOperator Control Loop)
# ---------------------------------------------------------------------------


@unique
class ReconciliationFailureCode(Enum):
    """Failure codes for drift detection and auto-remediation."""

    DESIRED_STATE_FETCH_ERROR = auto()   # Git API unreachable
    ACTUAL_STATE_FETCH_ERROR = auto()    # Ranger/Atlas/Model Registry API unreachable
    REMEDIATION_FAILED = auto()          # Patch attempt rejected
    DIFF_PARSE_ERROR = auto()            # State comparison produced invalid output
    MAX_RETRIES_EXCEEDED = auto()


@dataclass(frozen=True, slots=True)
class ReconciliationFailure(GovernanceFailure):
    """
    Returned by the ReconOperator when drift detection or remediation fails.

    code:            Machine-readable failure discriminant.
    dimension:       Which governance dimension failed: "ranger", "atlas", "model-registry".
    resource_id:     The policy name, entity qualified_name, or model ID involved.
    message:         Descriptive error message.
    retry_count:     Number of remediation attempts made before giving up.
    """

    code: ReconciliationFailureCode
    dimension: str
    message: str
    resource_id: Optional[str] = None
    retry_count: int = 0

    def __post_init__(self) -> None:
        allowed = {"ranger", "atlas", "model-registry", "unknown"}
        if self.dimension not in allowed:
            raise ValueError(
                f"ReconciliationFailure.dimension must be one of {allowed}, "
                f"got '{self.dimension}'."
            )

    def human_readable(self) -> str:
        return (
            f"[ReconciliationFailure/{self.code.name}] "
            f"dimension='{self.dimension}' resource='{self.resource_id}' "
            f"retries={self.retry_count} message='{self.message}'"
        )


# ---------------------------------------------------------------------------
# Cross-cutting — Transport / network failures
# ---------------------------------------------------------------------------


@unique
class TransportFailureCode(Enum):
    """Generic transport-layer failure codes."""

    CONNECTION_TIMEOUT = auto()
    CONNECTION_REFUSED = auto()
    TLS_ERROR = auto()
    HTTP_5XX = auto()
    HTTP_4XX = auto()
    KAFKA_BROKER_UNAVAILABLE = auto()
    DNS_RESOLUTION_FAILED = auto()


@dataclass(frozen=True, slots=True)
class TransportFailure(GovernanceFailure):
    """
    Returned when a network call fails at the transport layer, before
    any application-level response is available.

    Applies to any plane making outbound calls (REST, Kafka producer).

    code:       Machine-readable transport failure discriminant.
    endpoint:   The URL or Kafka topic that was unreachable.
    message:    Raw exception or system message.
    """

    code: TransportFailureCode
    endpoint: str
    message: str

    def human_readable(self) -> str:
        return (
            f"[TransportFailure/{self.code.name}] "
            f"endpoint='{self.endpoint}' message='{self.message}'"
        )
