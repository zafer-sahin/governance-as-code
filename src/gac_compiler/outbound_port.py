"""
governance/src/gac_compiler/outbound_port.py

GaC State Compiler — Hexagonal Outbound Port (Plane 2).

Responsibility:
  Serialise a CompilationResult (produced by the Core Domain) into raw JSON
  files on the local filesystem under the ``outputs/`` directory.

  This module IS the Outbound Port in Hexagonal Architecture terms.
  It translates Core Domain value objects into the format required by
  the downstream infrastructure (Ranger REST API, Atlas REST API).

  In the "Vibe Coding" stage (no live SDK), the JSON files written here
  are the contract artefacts that a real adapter would POST to the
  Ranger and Atlas endpoints. They are fully self-contained and can be
  replayed against a live cluster without modification.

File layout produced
--------------------
outputs/
  ranger_policies/
    <policy_name>.json          ← one file per RangerPolicy
  atlas_typedefs/
    <type_name>.json            ← one file per AtlasTypeDef
  atlas_entities/
    <safe_qualified_name>.json  ← one file per AtlasEntity
  manifest.json                 ← index of all written artefacts

Design constraints:
  - No external libraries. stdlib only (json, pathlib, datetime).
  - Each write returns Result[Path, GovernanceFailure] — never raises.
  - The manifest.json is written last; its presence signals a complete run.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from contracts import (
    AtlasEntity,
    AtlasTypeDef,
    CardinalityType,
    ClassificationPropagation,
    EntityStatus,
    Failure,
    GovernanceFailure,
    MaskType,
    MetadataFailure,
    MetadataFailureCode,
    PolicyEffect,
    PolicyFailure,
    PolicyFailureCode,
    RangerMaskingSpec,
    RangerPolicy,
    Result,
    Success,
    err,
    ok,
)
from gac_compiler.core_domain import CompilationResult

# ---------------------------------------------------------------------------
# Default output directory (relative to repo root)
# ---------------------------------------------------------------------------

_DEFAULT_OUTPUTS = Path(__file__).parent / "outputs"


# ---------------------------------------------------------------------------
# WriteReport — summary of what was persisted
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WriteReport:
    """
    Immutable summary returned after a full CompilationResult is written.

    Fields
    ------
    manifest_path   : Path to the written manifest.json
    ranger_paths    : Paths of all written Ranger policy JSON files
    typedef_paths   : Paths of all written Atlas TypeDef JSON files
    entity_paths    : Paths of all written Atlas entity JSON files
    warnings        : Non-fatal write-time advisory messages
    written_at      : UTC timestamp of the write operation
    """

    manifest_path: Path
    ranger_paths: tuple[Path, ...]
    typedef_paths: tuple[Path, ...]
    entity_paths: tuple[Path, ...]
    warnings: tuple[str, ...]
    written_at: datetime


# ---------------------------------------------------------------------------
# Public Outbound Port entry point
# ---------------------------------------------------------------------------


def write_compilation_result(
    result: CompilationResult,
    *,
    outputs_dir: Path = _DEFAULT_OUTPUTS,
) -> Result[WriteReport, GovernanceFailure]:
    """
    Serialise a CompilationResult to the outputs/ directory.

    Parameters
    ----------
    result:
        The CompilationResult produced by ``compile_plan``.
    outputs_dir:
        Root directory for all output artefacts.
        Defaults to ``src/gac_compiler/outputs/``.

    Returns
    -------
    Result[WriteReport, GovernanceFailure]
        Success → WriteReport listing every written path.
        Failure → PolicyFailure or MetadataFailure describing what failed.
    """
    try:
        _ensure_dirs(outputs_dir)
    except OSError as exc:
        return err(
            PolicyFailure(
                code=PolicyFailureCode.COMPILATION_ERROR,
                message=f"Cannot create output directories under {outputs_dir}: {exc}",
            )
        )

    ranger_paths: list[Path] = []
    typedef_paths: list[Path] = []
    entity_paths: list[Path] = []
    warnings: list[str] = list(result.warnings)

    # ------------------------------------------------------------------
    # 1. Write Ranger policies
    # ------------------------------------------------------------------
    for policy in result.ranger_policies:
        write_res = write_ranger_policy(policy, outputs_dir / "ranger_policies")
        if isinstance(write_res, Failure):
            return write_res  # type: ignore[return-value]
        ranger_paths.append(write_res.value)

    # ------------------------------------------------------------------
    # 2. Write Atlas TypeDefs
    # ------------------------------------------------------------------
    for typedef in result.atlas_typedefs:
        write_res = write_atlas_typedef(typedef, outputs_dir / "atlas_typedefs")
        if isinstance(write_res, Failure):
            return write_res  # type: ignore[return-value]
        typedef_paths.append(write_res.value)

    # ------------------------------------------------------------------
    # 3. Write Atlas Entities
    # ------------------------------------------------------------------
    for entity in result.atlas_entities:
        write_res = write_atlas_entity(entity, outputs_dir / "atlas_entities")
        if isinstance(write_res, Failure):
            return write_res  # type: ignore[return-value]
        entity_paths.append(write_res.value)

    # ------------------------------------------------------------------
    # 4. Write manifest (signals a complete run)
    # ------------------------------------------------------------------
    written_at = datetime.now(timezone.utc)
    manifest = _build_manifest(
        result=result,
        ranger_paths=ranger_paths,
        typedef_paths=typedef_paths,
        entity_paths=entity_paths,
        warnings=warnings,
        written_at=written_at,
    )
    manifest_path = outputs_dir / "manifest.json"
    try:
        _write_json(manifest_path, manifest)
    except OSError as exc:
        return err(
            PolicyFailure(
                code=PolicyFailureCode.COMPILATION_ERROR,
                message=f"Failed to write manifest.json: {exc}",
            )
        )

    return ok(
        WriteReport(
            manifest_path=manifest_path,
            ranger_paths=tuple(ranger_paths),
            typedef_paths=tuple(typedef_paths),
            entity_paths=tuple(entity_paths),
            warnings=tuple(warnings),
            written_at=written_at,
        )
    )


# ---------------------------------------------------------------------------
# Individual write functions (also usable standalone)
# ---------------------------------------------------------------------------


def write_ranger_policy(
    policy: RangerPolicy,
    directory: Path,
) -> Result[Path, GovernanceFailure]:
    """
    Serialise a single RangerPolicy to ``<directory>/<policy_name>.json``.

    The JSON structure mirrors the Ranger REST API request body for
    ``PUT /service/public/v2/api/policy`` — ready to replay against a
    live Ranger endpoint without transformation.
    """
    payload = _ranger_policy_to_dict(policy)
    path = directory / f"{_safe_filename(policy.name)}.json"
    try:
        _write_json(path, payload)
        return ok(path)
    except OSError as exc:
        return err(
            PolicyFailure(
                code=PolicyFailureCode.RANGER_API_ERROR,
                message=f"Failed to write Ranger policy '{policy.name}': {exc}",
                policy_name=policy.name,
            )
        )


def write_atlas_typedef(
    typedef: AtlasTypeDef,
    directory: Path,
) -> Result[Path, GovernanceFailure]:
    """
    Serialise a single AtlasTypeDef to ``<directory>/<type_name>.json``.

    The JSON structure mirrors the Atlas REST API request body for
    ``PUT /api/atlas/v2/types/typedefs`` — ready to replay.
    """
    payload = _atlas_typedef_to_dict(typedef)
    path = directory / f"{_safe_filename(typedef.type_name)}.json"
    try:
        _write_json(path, payload)
        return ok(path)
    except OSError as exc:
        return err(
            MetadataFailure(
                code=MetadataFailureCode.ATLAS_API_ERROR,
                message=f"Failed to write Atlas TypeDef '{typedef.type_name}': {exc}",
                type_name=typedef.type_name,
                source_system="gac_compiler",
            )
        )


def write_atlas_entity(
    entity: AtlasEntity,
    directory: Path,
) -> Result[Path, GovernanceFailure]:
    """
    Serialise a single AtlasEntity to ``<directory>/<safe_qualified_name>.json``.

    The JSON structure mirrors the Atlas REST API request body for
    ``POST /api/atlas/v2/entity`` — ready to replay.
    """
    payload = _atlas_entity_to_dict(entity)
    path = directory / f"{_safe_filename(entity.qualified_name)}.json"
    try:
        _write_json(path, payload)
        return ok(path)
    except OSError as exc:
        return err(
            MetadataFailure(
                code=MetadataFailureCode.ATLAS_API_ERROR,
                message=f"Failed to write Atlas entity '{entity.qualified_name}': {exc}",
                qualified_name=entity.qualified_name,
                source_system="gac_compiler",
            )
        )


# ---------------------------------------------------------------------------
# Serialisers — domain objects → plain dicts (Ranger / Atlas REST format)
# ---------------------------------------------------------------------------


def _ranger_policy_to_dict(policy: RangerPolicy) -> dict[str, Any]:
    """
    Serialise a RangerPolicy to a dict matching the Ranger REST API schema.

    Reference: Apache Ranger REST API v2 — RangerPolicy JSON structure.
    """
    policy_items = []
    for item in policy.policy_items:
        pi: dict[str, Any] = {
            "accesses": [
                {"type": acc, "isAllowed": item.effect == PolicyEffect.ALLOW}
                for acc in item.accesses
            ],
            "users": list(item.principal.users),
            "groups": list(item.principal.groups),
            "roles": list(item.principal.roles),
            "conditions": [
                {"type": c.condition_type, "values": list(c.values)}
                for c in item.conditions
            ],
            "delegateAdmin": item.delegate,
        }
        if item.masking:
            pi["dataMaskInfo"] = {
                "dataMaskType": item.masking.mask_type.value,
                "valueExpr": item.masking.mask_value or "",
            }
        policy_items.append(pi)

    resources: dict[str, Any] = {}
    for res in policy.resources:
        resources[res.resource_type.value] = {
            "values": list(res.values),
            "isRecursive": res.is_recursive,
            "isExcludes": res.is_exclusion,
        }

    return {
        # Ranger API fields
        "id": str(policy.policy_id) if policy.policy_id else None,
        "name": policy.name,
        "service": policy.service,
        "description": policy.description,
        "isEnabled": policy.is_enabled,
        "isAuditEnabled": policy.is_audit_enabled,
        "resources": resources,
        "policyItems": [pi for pi in policy_items if not any(
            a.get("dataMaskInfo") for a in [pi] if "dataMaskInfo" not in pi
        )],
        "dataMaskPolicyItems": [pi for pi in policy_items if "dataMaskInfo" in pi],
        "denyPolicyItems": [],
        "labels": list(policy.labels),
        "version": policy.version,
        # GaC metadata
        "_gac": {
            "gitSha": policy.git_sha,
            "compiledBy": "gac_compiler/core_domain",
            "schema": "ranger-policy-v2",
        },
    }


def _atlas_typedef_to_dict(typedef: AtlasTypeDef) -> dict[str, Any]:
    """
    Serialise an AtlasTypeDef to a dict matching the Atlas REST API schema.

    Reference: Apache Atlas REST API v2 — TypesDef JSON structure.
    """
    entity_def: dict[str, Any] = {
        "category": "ENTITY",
        "name": typedef.type_name,
        "description": typedef.description,
        "superTypes": list(typedef.super_types),
        "serviceType": typedef.service_type,
        "attributeDefs": [
            {
                "name": attr.name,
                "typeName": attr.type_name,
                "cardinality": attr.cardinality.value,
                "isOptional": attr.is_optional,
                "isUnique": attr.is_unique,
                "isIndexable": attr.is_indexable,
                "description": attr.description,
                "includeInNotification": False,
                "searchWeight": -1,
            }
            for attr in typedef.attribute_defs
        ],
    }

    return {
        "enumDefs": [],
        "structDefs": [],
        "classificationDefs": [],
        "entityDefs": [entity_def],
        "relationshipDefs": [],
        # GaC metadata
        "_gac": {
            "gitSha": typedef.git_sha,
            "compiledBy": "gac_compiler/core_domain",
            "schema": "atlas-typedef-v2",
        },
    }


def _atlas_entity_to_dict(entity: AtlasEntity) -> dict[str, Any]:
    """
    Serialise an AtlasEntity to a dict matching the Atlas REST API schema.

    Reference: Apache Atlas REST API v2 — EntityMutationResponse JSON structure.
    """
    classifications = [
        {
            "typeName": clf.name,
            "attributes": clf.attributes_as_dict(),
            "propagate": clf.propagate != ClassificationPropagation.DISABLED,
            "removePropagationsOnEntityDelete": False,
        }
        for clf in entity.classifications
    ]

    business_attrs: dict[str, Any] = {}
    for bm in entity.business_metadata:
        business_attrs[bm.namespace] = bm.attributes_as_dict()

    return {
        "entity": {
            "typeName": entity.type_name,
            "guid": str(entity.guid) if entity.guid else None,
            "status": entity.status.value,
            "attributes": entity.attributes_as_dict(),
            "classifications": classifications,
            "businessAttributes": business_attrs,
            "relationshipAttributes": {},
            "version": entity.version,
            "createdBy": entity.created_by,
            "updatedBy": entity.updated_by,
        },
        # GaC metadata
        "_gac": {
            "gitSha": entity.git_sha,
            "sourceSystem": entity.source_system,
            "compiledBy": "gac_compiler/core_domain",
            "schema": "atlas-entity-v2",
        },
    }


# ---------------------------------------------------------------------------
# Manifest builder
# ---------------------------------------------------------------------------


def _build_manifest(
    *,
    result: CompilationResult,
    ranger_paths: list[Path],
    typedef_paths: list[Path],
    entity_paths: list[Path],
    warnings: list[str],
    written_at: datetime,
) -> dict[str, Any]:
    return {
        "schemaVersion": "1.0",
        "planId": result.plan_id,
        "gitSha": result.git_sha,
        "compiledAt": result.compiled_at.isoformat(),
        "writtenAt": written_at.isoformat(),
        "summary": {
            "rangerPolicies": len(ranger_paths),
            "atlasTypeDefs": len(typedef_paths),
            "atlasEntities": len(entity_paths),
        },
        "artefacts": {
            "rangerPolicies": [str(p) for p in ranger_paths],
            "atlasTypeDefs": [str(p) for p in typedef_paths],
            "atlasEntities": [str(p) for p in entity_paths],
        },
        "warnings": warnings,
        "isComplete": True,
    }


# ---------------------------------------------------------------------------
# I/O utilities
# ---------------------------------------------------------------------------


def _ensure_dirs(base: Path) -> None:
    for sub in ("ranger_policies", "atlas_typedefs", "atlas_entities"):
        (base / sub).mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _safe_filename(name: str) -> str:
    """Convert an arbitrary string to a safe filesystem filename (no slashes, no spaces)."""
    return name.replace("/", "_").replace(".", "_").replace(" ", "_").replace(":", "_")
