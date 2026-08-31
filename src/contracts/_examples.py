"""
governance/src/contracts/_examples.py

Usage examples for the GaC contract layer.

This file is NOT production code — it serves as executable documentation
showing how the immutable types and Result ADT compose in practice.

Run with:
    cd governance
    python -m contracts._examples
"""

from __future__ import annotations

import sys
from uuid import uuid4

sys.path.insert(0, "src")

from contracts import (
    AtlasClassification,
    AtlasEntity,
    AtlasLineage,
    AtlasLineageEdge,
    AtlasTypeDef,
    AtlasAttributeDef,
    AtlasBusinessMetadata,
    ClassificationPropagation,
    EntityStatus,
    Failure,
    GovernanceFailure,
    LineageDirection,
    MaskType,
    MetadataFailure,
    MetadataFailureCode,
    PolicyEffect,
    PolicyFailure,
    PolicyFailureCode,
    RangerCondition,
    RangerMaskingSpec,
    RangerPolicy,
    RangerPolicyItem,
    RangerPrincipal,
    RangerResource,
    ReconciliationFailure,
    ReconciliationFailureCode,
    ResourceType,
    Result,
    Success,
    TransportFailure,
    TransportFailureCode,
    ValidationCode,
    ValidationFailure,
    err,
    ok,
)


# ---------------------------------------------------------------------------
# Example 1: Build a RangerPolicy and wrap it in Result
# ---------------------------------------------------------------------------

def build_ranger_policy_example() -> Result[RangerPolicy, GovernanceFailure]:
    """
    Simulate the PolicyCompiler Outbound Port compiling a Ranger policy
    from a PR diff and returning Result[RangerPolicy, GovernanceFailure].
    """
    try:
        resource = RangerResource(
            resource_type=ResourceType.TABLE,
            values=("prod_db.customer_pii",),
            is_recursive=False,
        )
        principal = RangerPrincipal(groups=("data-scientists",))
        masking_spec = RangerMaskingSpec(mask_type=MaskType.HASH)
        policy_item = RangerPolicyItem(
            accesses=("select",),
            principal=principal,
            effect=PolicyEffect.ALLOW,
            masking=masking_spec,
        )
        policy = RangerPolicy(
            name="ds-customer-pii-hashed-read",
            service="trino_prod",
            resources=(resource,),
            policy_items=(policy_item,),
            description="Data scientists may read PII columns, returned as SHA-256 hashes.",
            labels=("pii", "regulated", "gdpr"),
            git_sha="a1b2c3d4",
        )
        return ok(policy)

    except ValueError as exc:
        return err(
            PolicyFailure(
                code=PolicyFailureCode.COMPILATION_ERROR,
                message=str(exc),
                pr_id="PR-42",
            )
        )


# ---------------------------------------------------------------------------
# Example 2: Build an AtlasEntity and wrap it in Result
# ---------------------------------------------------------------------------

def register_atlas_entity_example() -> Result[AtlasEntity, GovernanceFailure]:
    """
    Simulate a DiscoveryEngine or a native hook registering a discovered Hive table
    entity in Atlas and returning Result[AtlasEntity, GovernanceFailure].
    """
    guid = uuid4()
    try:
        pii_tag = AtlasClassification(
            name="PII",
            attributes=(("pii_type", "EMAIL"), ("regulation", "GDPR")),
            propagate=ClassificationPropagation.ENABLED,
        )
        biz_meta = AtlasBusinessMetadata(
            namespace="DataQuality",
            attributes=(("completeness", 0.98), ("freshness_hours", 24)),
        )
        entity = AtlasEntity(
            type_name="hive_table",
            qualified_name="prod_db.customer_pii@cluster1",
            attributes=(
                ("db", "prod_db"),
                ("name", "customer_pii"),
                ("owner", "data-platform-team"),
                ("createTime", "2024-01-01T00:00:00Z"),
            ),
            guid=guid,
            classifications=(pii_tag,),
            business_metadata=(biz_meta,),
            source_system="discovery-engine",
            git_sha="a1b2c3d4",
        )
        return ok(entity)

    except Exception as exc:
        return err(
            MetadataFailure(
                code=MetadataFailureCode.ATLAS_API_ERROR,
                message=str(exc),
                source_system="discovery-engine",
            )
        )


# ---------------------------------------------------------------------------
# Example 3: AtlasTypeDef declaration (Plane 2 PolicyCompiler)
# ---------------------------------------------------------------------------

def declare_atlas_typedef_example() -> AtlasTypeDef:
    return AtlasTypeDef(
        type_name="TrinoView",
        super_types=("DataSet",),
        attribute_defs=(
            AtlasAttributeDef(name="catalog", type_name="string", is_optional=False, is_indexable=True),
            AtlasAttributeDef(name="schema", type_name="string", is_optional=False, is_indexable=True),
            AtlasAttributeDef(name="view_definition", type_name="string", is_optional=True),
            AtlasAttributeDef(name="owner", type_name="string", is_optional=True),
        ),
        description="A Trino view registered in the universal catalog.",
        service_type="gac-governance",
        git_sha="a1b2c3d4",
    )


# ---------------------------------------------------------------------------
# Example 4: AtlasLineage — lineage edge graph
# ---------------------------------------------------------------------------

def build_lineage_example() -> Result[AtlasLineage, GovernanceFailure]:
    src_guid = uuid4()
    dst_guid = uuid4()
    proc_guid = uuid4()
    try:
        edge = AtlasLineageEdge(
            from_entity_guid=src_guid,
            to_entity_guid=dst_guid,
            process_guid=proc_guid,
        )
        lineage = AtlasLineage(
            root_entity_guid=dst_guid,
            direction=LineageDirection.INPUT,
            edges=(edge,),
            depth=1,
            source_system="spark-hook",
        )
        return ok(lineage)
    except ValueError as exc:
        return err(
            MetadataFailure(
                code=MetadataFailureCode.ATLAS_API_ERROR,
                message=str(exc),
                source_system="spark-hook",
            )
        )


# ---------------------------------------------------------------------------
# Example 5: Result ADT — fold, map, flat_map, recover
# ---------------------------------------------------------------------------

def adt_combinator_examples() -> None:
    # --- fold (exhaustive eliminator) ---
    result: Result[RangerPolicy, GovernanceFailure] = build_ranger_policy_example()
    log_line = result.fold(
        on_success=lambda p: f"Policy compiled: {p.name} (labels={p.labels})",
        on_failure=lambda f: f"Compilation failed: {f.human_readable()}",
    )
    print(log_line)

    # --- map (transform success value) ---
    stamped = result.map(lambda p: p.with_git_sha("deadbeef"))
    assert stamped.fold(lambda p: p.git_sha, lambda _: None) == "deadbeef"

    # --- flat_map (chain fallible operations) ---
    def validate_labels(policy: RangerPolicy) -> Result[RangerPolicy, GovernanceFailure]:
        if "pii" not in policy.labels:
            return err(
                ValidationFailure(
                    code=ValidationCode.CLASSIFICATION_MISSING,
                    resource=policy.name,
                    principal="system",
                    reason="PII label required on customer data policies.",
                )
            )
        return ok(policy)

    validated = result.flat_map(validate_labels)
    assert validated.is_success()

    # --- recover (extract with default on failure) ---
    bad: Result[str, GovernanceFailure] = err(
        TransportFailure(
            code=TransportFailureCode.CONNECTION_TIMEOUT,
            endpoint="https://ranger.example.com:6080",
            message="i/o timeout after 30s",
        )
    )
    fallback = bad.recover(lambda f: f"DEFAULT: {f.human_readable()}")
    assert fallback.startswith("DEFAULT:")

    # --- ReconciliationFailure example ---
    recon_err: Result[None, GovernanceFailure] = err(
        ReconciliationFailure(
            code=ReconciliationFailureCode.REMEDIATION_FAILED,
            dimension="model-registry",
            resource_id="model-fraud-v3",
            message="Model Registry API returned 409 Conflict during model policy patch.",
            retry_count=3,
        )
    )
    assert recon_err.is_failure()
    print(recon_err.fold(lambda _: "ok", lambda f: f.human_readable()))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== RangerPolicy ===")
    ranger_result = build_ranger_policy_example()
    print(ranger_result)
    assert ranger_result.is_success()

    print("\n=== AtlasEntity ===")
    atlas_result = register_atlas_entity_example()
    print(atlas_result)
    assert atlas_result.is_success()

    print("\n=== AtlasTypeDef ===")
    typedef = declare_atlas_typedef_example()
    print(typedef)

    print("\n=== AtlasLineage ===")
    lineage_result = build_lineage_example()
    print(lineage_result)
    assert lineage_result.is_success()

    print("\n=== ADT Combinators ===")
    adt_combinator_examples()

    print("\n✓ All examples passed.")
