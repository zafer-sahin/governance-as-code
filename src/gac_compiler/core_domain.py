"""
governance/src/gac_compiler/core_domain.py

GaC State Compiler — Hexagonal Architecture Core Domain (Plane 2).

Responsibility:
  Accept an execution plan dict (produced by LocalProxy or a real Trino
  EXPLAIN response) and deterministically compile it into:
    1. RangerPolicy objects  — data-access ACL policies
    2. AtlasTypeDef objects  — metadata type declarations
    3. AtlasEntity objects   — dataset entity registrations

Design constraints (Hexagonal pattern):
  - This module is pure domain logic. Zero I/O, zero network calls.
  - No external SDKs (no Atlas client, no Ranger client).
  - All outputs are values (contracts types). Serialisation is the
    responsibility of the Outbound Port (outbound_port.py).
  - The module is the single source of policy-inference truth.
    The State Machine (state_machine.py) gates when this runs.

Inbound side:
  - Input: raw plan dict from LocalProxy (trino_plan_stub or live Trino).
  - Called by the CI/CD runner after a webhook arrives (GitOps).

Outbound side:
  - Returns CompilationResult — a frozen aggregate of all compiled objects.
  - The Outbound Port serialises CompilationResult to JSON files.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from contracts import (
    AtlasAttributeDef,
    AtlasClassification,
    AtlasEntity,
    AtlasTypeDef,
    CardinalityType,
    ClassificationPropagation,
    EntityStatus,
    Failure,
    GovernanceFailure,
    MaskType,
    PolicyEffect,
    PolicyFailure,
    PolicyFailureCode,
    RangerMaskingSpec,
    RangerPolicy,
    RangerPolicyItem,
    RangerPrincipal,
    RangerResource,
    ResourceType,
    Result,
    Success,
    err,
    ok,
)


# ---------------------------------------------------------------------------
# CompilationResult — aggregate output of the Core Domain (immutable)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CompilationResult:
    """
    The full output of one compilation pass over a Trino execution plan.

    Produced by compile_plan() and consumed by the Outbound Port.

    Fields
    ------
    plan_id       : The query ID from the execution plan (deterministic UUID).
    git_sha       : Git commit SHA from which this compilation was triggered.
    ranger_policies : All Ranger data-access policies inferred from the plan.
    atlas_typedefs  : All Atlas TypeDef declarations inferred from the plan.
    atlas_entities  : All Atlas entity (dataset) registrations inferred.
    compiled_at     : UTC timestamp of compilation.
    warnings        : Non-fatal advisory messages (e.g., missing column types).
    """

    plan_id: str
    git_sha: str
    ranger_policies: tuple[RangerPolicy, ...]
    atlas_typedefs: tuple[AtlasTypeDef, ...]
    atlas_entities: tuple[AtlasEntity, ...]
    compiled_at: datetime
    warnings: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Public Core Domain entry point
# ---------------------------------------------------------------------------


def compile_plan(
    plan: dict[str, Any],
    *,
    git_sha: str = "unknown",
    requesting_principal: str = "ci-cd-runner",
) -> Result[CompilationResult, GovernanceFailure]:
    """
    Hexagonal Core Domain: compile a Trino execution plan into governance objects.

    Parameters
    ----------
    plan:
        The execution plan dict as produced by ``generate_trino_execution_plan``
        or a real Trino ``EXPLAIN (FORMAT JSON)`` response.
    git_sha:
        The Git commit SHA of the PR/commit that triggered this compilation.
    requesting_principal:
        The CI/CD service account requesting compilation (audit trail).

    Returns
    -------
    Result[CompilationResult, GovernanceFailure]
        Success  → CompilationResult with all compiled governance objects.
        Failure  → PolicyFailure describing what went wrong.
    """
    try:
        plan_id = plan.get("id", "unknown")
        input_tables: list[str] = plan.get("inputTables", [])
        pii_tables: list[str] = plan.get("piiTables", [])
        has_select_star: bool = plan.get("hasSelectStar", False)
        plan_root: dict[str, Any] = plan.get("plan", {})

        # Extract TableScan nodes from the plan tree
        table_scans = _collect_table_scans(plan_root)

        warnings: list[str] = []

        # ------------------------------------------------------------------
        # 1. Infer Ranger policies
        # ------------------------------------------------------------------
        ranger_policies: list[RangerPolicy] = []

        for scan in table_scans:
            descriptor = scan.get("descriptor", {})
            qualified_name: str = descriptor.get("qualifiedName", "")
            is_pii: bool = descriptor.get("isPiiSource", False)
            columns: list[dict[str, Any]] = scan.get("outputs", [])

            if not qualified_name:
                warnings.append(f"TableScan missing qualifiedName — skipped: {scan.get('id')}")
                continue

            # Build one Ranger policy per table (full-access allow + masking per PII column)
            policies = _infer_ranger_policies(
                qualified_name=qualified_name,
                is_pii=is_pii,
                has_select_star=has_select_star,
                columns=columns,
                principal=requesting_principal,
                git_sha=git_sha,
            )
            ranger_policies.extend(policies)

        # ------------------------------------------------------------------
        # 2. Infer Atlas TypeDefs
        # ------------------------------------------------------------------
        atlas_typedefs: list[AtlasTypeDef] = []
        seen_type_names: set[str] = set()

        for scan in table_scans:
            descriptor = scan.get("descriptor", {})
            qualified_name = descriptor.get("qualifiedName", "")
            columns = scan.get("outputs", [])

            # Derive a TypeDef name from the table (e.g., "prod.pii_table" → "TrinoTable_prod_pii_table")
            type_name = _to_typedef_name(qualified_name)
            if type_name in seen_type_names:
                continue
            seen_type_names.add(type_name)

            typedef = _infer_atlas_typedef(
                type_name=type_name,
                qualified_name=qualified_name,
                columns=columns,
                git_sha=git_sha,
            )
            atlas_typedefs.append(typedef)

        # ------------------------------------------------------------------
        # 3. Infer Atlas Entities (dataset registrations)
        # ------------------------------------------------------------------
        atlas_entities: list[AtlasEntity] = []

        for scan in table_scans:
            descriptor = scan.get("descriptor", {})
            qualified_name = descriptor.get("qualifiedName", "")
            is_pii = descriptor.get("isPiiSource", False)
            columns = scan.get("outputs", [])

            if not qualified_name:
                continue

            entity = _infer_atlas_entity(
                qualified_name=qualified_name,
                is_pii=is_pii,
                columns=columns,
                descriptor=descriptor,
                git_sha=git_sha,
            )
            atlas_entities.append(entity)

        if not ranger_policies and not atlas_typedefs:
            return err(
                PolicyFailure(
                    code=PolicyFailureCode.COMPILATION_ERROR,
                    message=(
                        f"Core Domain produced no policies from plan '{plan_id}'. "
                        "Verify that the plan contains at least one TableScan node."
                    ),
                    policy_name=git_sha if git_sha != "unknown" else None,
                )
            )

        result = CompilationResult(
            plan_id=plan_id,
            git_sha=git_sha,
            ranger_policies=tuple(ranger_policies),
            atlas_typedefs=tuple(atlas_typedefs),
            atlas_entities=tuple(atlas_entities),
            compiled_at=datetime.now(timezone.utc),
            warnings=tuple(warnings),
        )
        return ok(result)

    except Exception as exc:  # pragma: no cover — unexpected domain errors
        return err(
            PolicyFailure(
                code=PolicyFailureCode.COMPILATION_ERROR,
                message=f"Unexpected Core Domain error: {exc}",
            )
        )


# ---------------------------------------------------------------------------
# Ranger policy inference
# ---------------------------------------------------------------------------


def _infer_ranger_policies(
    *,
    qualified_name: str,
    is_pii: bool,
    has_select_star: bool,
    columns: list[dict[str, Any]],
    principal: str,
    git_sha: str,
) -> list[RangerPolicy]:
    """
    Produce Ranger policies for one table based on its PII classification
    and the type of access requested (SELECT * vs explicit columns).

    Policy matrix
    -------------
    Non-PII table   → One ALLOW policy (read-only) for the CI principal.
    PII + explicit  → ALLOW on non-PII columns + MASKING policy per PII column.
    PII + SELECT *  → DENY full SELECT * + MASKING policy (force explicit columns).
    """
    policies: list[RangerPolicy] = []
    schema, _, table = qualified_name.partition(".")

    base_resource = RangerResource(
        resource_type=ResourceType.TABLE,
        values=(qualified_name,),
        is_recursive=False,
    )
    ci_principal = RangerPrincipal(roles=(principal,))

    if not is_pii:
        # Simple read-allow policy
        policies.append(
            RangerPolicy(
                name=f"gac-allow-read-{schema}-{table}",
                service="trino_prod",
                resources=(base_resource,),
                policy_items=(
                    RangerPolicyItem(
                        accesses=("select",),
                        principal=ci_principal,
                        effect=PolicyEffect.ALLOW,
                    ),
                ),
                description=f"GaC compiled: read access to {qualified_name}.",
                labels=("gac-compiled",),
                git_sha=git_sha,
            )
        )
        return policies

    # PII table — build DENY on SELECT * and MASKING per PII column
    if has_select_star:
        # DENY policy: prevents unmasked SELECT * on PII tables
        deny_resource = RangerResource(
            resource_type=ResourceType.TABLE,
            values=(qualified_name,),
        )
        policies.append(
            RangerPolicy(
                name=f"gac-deny-select-star-{schema}-{table}",
                service="trino_prod",
                resources=(deny_resource,),
                policy_items=(
                    RangerPolicyItem(
                        accesses=("select",),
                        principal=RangerPrincipal(groups=("*",)),
                        effect=PolicyEffect.DENY,
                    ),
                ),
                description=(
                    f"GaC compiled: deny SELECT * on PII table {qualified_name}. "
                    "Use explicit columns with masking policy applied."
                ),
                labels=("gac-compiled", "pii", "deny-select-star"),
                git_sha=git_sha,
            )
        )

    # MASKING policy: one policy covering all PII columns
    pii_columns = [c for c in columns if c.get("isPii", False)]
    for col in pii_columns:
        col_name = col["name"]
        mask_type_str = col.get("maskType", "HASH")
        try:
            mask_type = MaskType[mask_type_str]
        except KeyError:
            mask_type = MaskType.HASH

        col_resource = RangerResource(
            resource_type=ResourceType.COLUMN,
            values=(col_name,),
        )
        table_resource = RangerResource(
            resource_type=ResourceType.TABLE,
            values=(qualified_name,),
        )
        policies.append(
            RangerPolicy(
                name=f"gac-mask-{schema}-{table}-{col_name}",
                service="trino_prod",
                resources=(table_resource, col_resource),
                policy_items=(
                    RangerPolicyItem(
                        accesses=("select",),
                        principal=ci_principal,
                        effect=PolicyEffect.ALLOW,
                        masking=RangerMaskingSpec(mask_type=mask_type),
                    ),
                ),
                description=(
                    f"GaC compiled: masking policy for PII column "
                    f"{qualified_name}.{col_name} — strategy: {mask_type.value}."
                ),
                labels=("gac-compiled", "pii", "masking"),
                git_sha=git_sha,
            )
        )

    return policies


# ---------------------------------------------------------------------------
# Atlas TypeDef inference
# ---------------------------------------------------------------------------


def _infer_atlas_typedef(
    *,
    type_name: str,
    qualified_name: str,
    columns: list[dict[str, Any]],
    git_sha: str,
) -> AtlasTypeDef:
    """
    Derive an Atlas TypeDef from the table schema discovered in the plan.

    All tables are typed as sub-types of the Atlas built-in ``DataSet``.
    Each column becomes an AtlasAttributeDef (string type by default;
    a real implementation would map Trino types to Atlas primitive types).
    """
    attribute_defs = tuple(
        AtlasAttributeDef(
            name=col["name"],
            type_name=_trino_type_to_atlas(col.get("type", "string")),
            cardinality=CardinalityType.SINGLE,
            is_optional=True,
            is_unique=col["name"] in ("id", "customer_id"),
            is_indexable=col["name"] in ("id", "customer_id", "email"),
            description=(
                f"PII column — masking: {col.get('maskType', 'NONE')}"
                if col.get("isPii")
                else ""
            ),
        )
        for col in columns
    )

    # Always include qualified_name as a mandatory indexed attribute
    qn_def = AtlasAttributeDef(
        name="qualifiedName",
        type_name="string",
        cardinality=CardinalityType.SINGLE,
        is_optional=False,
        is_unique=True,
        is_indexable=True,
        description="Globally unique identifier for this entity.",
    )

    return AtlasTypeDef(
        type_name=type_name,
        super_types=("DataSet",),
        attribute_defs=(qn_def,) + attribute_defs,
        description=f"GaC compiled typedef for {qualified_name}.",
        service_type="gac-governance",
        git_sha=git_sha,
    )


# ---------------------------------------------------------------------------
# Atlas Entity inference
# ---------------------------------------------------------------------------


def _infer_atlas_entity(
    *,
    qualified_name: str,
    is_pii: bool,
    columns: list[dict[str, Any]],
    descriptor: dict[str, Any],
    git_sha: str,
) -> AtlasEntity:
    """
    Produce an Atlas entity registration from a discovered table.

    PII tables receive an ``AtlasClassification(name="PII")`` tag
    with propagation enabled (so lineage-connected entities inherit it).
    """
    schema, _, table = qualified_name.partition(".")
    type_name = _to_typedef_name(qualified_name)

    # Base attributes from the plan descriptor
    attributes: list[tuple[str, Any]] = [
        ("qualifiedName", qualified_name),
        ("name", table),
        ("schemaName", schema),
        ("catalogName", descriptor.get("catalogName", "trino_prod")),
        ("columnCount", len(columns)),
        ("hasPiiColumns", is_pii),
        ("compiledBy", "gac_compiler"),
    ]

    classifications: tuple[AtlasClassification, ...] = ()
    if is_pii:
        pii_col_names = [c["name"] for c in columns if c.get("isPii", False)]
        classifications = (
            AtlasClassification(
                name="PII",
                attributes=tuple(
                    [
                        ("source", "gac_compiler"),
                        ("piiColumns", ",".join(pii_col_names)),
                        ("regulatoryScope", "GDPR"),
                    ]
                ),
                propagate=ClassificationPropagation.ENABLED,
            ),
        )

    return AtlasEntity(
        type_name=type_name,
        qualified_name=qualified_name,
        attributes=tuple(attributes),
        guid=_deterministic_guid(qualified_name),
        status=EntityStatus.ACTIVE,
        classifications=classifications,
        source_system="gac_compiler",
        git_sha=git_sha,
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _collect_table_scans(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Depth-first traversal collecting all TableScan plan nodes."""
    result: list[dict[str, Any]] = []
    if node.get("name") == "TableScan":
        result.append(node)
    for child in node.get("children", []):
        result.extend(_collect_table_scans(child))
    return result


def _to_typedef_name(qualified_name: str) -> str:
    """
    Derive a PascalCase Atlas TypeDef name from a qualified table name.

    "prod.pii_table" → "TrinoTable_prod_pii_table"
    """
    safe = qualified_name.replace(".", "_").replace("-", "_")
    return f"TrinoTable_{safe}"


def _deterministic_guid(seed: str) -> uuid.UUID:
    """Derive a deterministic UUID from a string seed (e.g., qualified_name)."""
    digest = hashlib.sha256(seed.encode()).digest()
    return uuid.UUID(bytes=digest[:16])


_TRINO_TO_ATLAS_TYPE: dict[str, str] = {
    "bigint": "long",
    "integer": "int",
    "int": "int",
    "smallint": "short",
    "tinyint": "byte",
    "boolean": "boolean",
    "double": "double",
    "real": "float",
    "decimal": "double",
    "varchar": "string",
    "char": "string",
    "varbinary": "binary",
    "date": "date",
    "timestamp": "date",
    "time": "string",
    "json": "string",
    "array": "array<string>",
    "map": "map<string,string>",
    "row": "string",
}


def _trino_type_to_atlas(trino_type: str) -> str:
    """Map a Trino column type string to an Atlas primitive type string."""
    base = trino_type.lower().split("(")[0].strip()
    return _TRINO_TO_ATLAS_TYPE.get(base, "string")
