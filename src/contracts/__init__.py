"""
governance/src/contracts/__init__.py

Public API of the governance contracts package.

All types intended for use by other modules are re-exported here.
Import from this module — not from submodules — to maintain a stable public interface.

    from contracts import (
        Result, Success, Failure, ok, err,
        RangerPolicy, AtlasEntity, AtlasTypeDef, AtlasLineage,
        GovernanceFailure, ValidationFailure, PolicyFailure,
        MetadataFailure, ReconciliationFailure, TransportFailure,
    )
"""

from contracts.atlas_entity import (
    AtlasAttributeDef,
    AtlasBusinessMetadata,
    AtlasClassification,
    AtlasEntity,
    AtlasLineage,
    AtlasLineageEdge,
    AtlasTypeDef,
    CardinalityType,
    ClassificationPropagation,
    EntityStatus,
    LineageDirection,
)
from contracts.failures import (
    GovernanceFailure,
    MetadataFailure,
    MetadataFailureCode,
    PolicyFailure,
    PolicyFailureCode,
    ReconciliationFailure,
    ReconciliationFailureCode,
    TransportFailure,
    TransportFailureCode,
    ValidationFailure,
    ValidationCode,
)
from contracts.ranger_policy import (
    AuditMode,
    MaskType,
    PolicyEffect,
    RangerCondition,
    RangerMaskingSpec,
    RangerPolicy,
    RangerPolicyItem,
    RangerPrincipal,
    RangerResource,
    ResourceType,
)
from contracts.result import Failure, Result, Success, err, ok

__all__ = [
    # Result ADT
    "Result",
    "Success",
    "Failure",
    "ok",
    "err",
    # Ranger
    "RangerPolicy",
    "RangerPolicyItem",
    "RangerPrincipal",
    "RangerResource",
    "RangerCondition",
    "RangerMaskingSpec",
    "PolicyEffect",
    "ResourceType",
    "AuditMode",
    "MaskType",
    # Atlas
    "AtlasEntity",
    "AtlasTypeDef",
    "AtlasAttributeDef",
    "AtlasClassification",
    "AtlasBusinessMetadata",
    "AtlasLineage",
    "AtlasLineageEdge",
    "EntityStatus",
    "ClassificationPropagation",
    "LineageDirection",
    "CardinalityType",
    # Failures
    "GovernanceFailure",
    "ValidationFailure",
    "ValidationCode",
    "PolicyFailure",
    "PolicyFailureCode",
    "MetadataFailure",
    "MetadataFailureCode",
    "ReconciliationFailure",
    "ReconciliationFailureCode",
    "TransportFailure",
    "TransportFailureCode",
]
