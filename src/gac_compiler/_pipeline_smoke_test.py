"""
governance/src/gac_compiler/_pipeline_smoke_test.py

End-to-end smoke test: LocalProxy stub → GaC Core Domain → Outbound Port.

Tests the complete Plane 1 → Plane 2 pipeline:
  1. Generate a fake Trino execution plan (LocalProxy, CQRS stub).
  2. Pass it to compile_plan() (Hexagonal Core Domain).
  3. Write outputs to a temp directory via write_compilation_result() (Outbound Port).
  4. Assert output file content and manifest correctness.

Run from repo root:
    python src/gac_compiler/_pipeline_smoke_test.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from local_proxy import generate_trino_execution_plan
from gac_compiler import compile_plan, write_compilation_result
from contracts import Success, Failure


def run() -> None:
    sql = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
    git_sha = "abc1234def5678"

    # -----------------------------------------------------------------------
    # Step 1: LocalProxy plan stub (Plane 1 → Plane 2 inbound)
    # -----------------------------------------------------------------------
    plan = generate_trino_execution_plan(sql)
    assert plan["isStub"] is True
    assert "prod.pii_table" in plan["piiTables"]
    print(f"[1] Plan stub generated — id={plan['id']}")

    # -----------------------------------------------------------------------
    # Step 2: Core Domain compilation
    # -----------------------------------------------------------------------
    compile_result = compile_plan(plan, git_sha=git_sha, requesting_principal="ci-cd-runner")
    assert isinstance(compile_result, Success), f"Compilation failed: {compile_result}"

    cr = compile_result.value
    assert cr.git_sha == git_sha
    assert len(cr.ranger_policies) > 0, "Expected at least one Ranger policy"
    assert len(cr.atlas_typedefs) > 0,  "Expected at least one Atlas TypeDef"
    assert len(cr.atlas_entities) > 0,  "Expected at least one Atlas entity"

    print(f"[2] Core Domain compiled:")
    print(f"    Ranger policies : {len(cr.ranger_policies)}")
    for p in cr.ranger_policies:
        print(f"      · {p.name}  labels={p.labels}")
    print(f"    Atlas TypeDefs  : {len(cr.atlas_typedefs)}")
    for t in cr.atlas_typedefs:
        print(f"      · {t.type_name}  superTypes={t.super_types}")
    print(f"    Atlas Entities  : {len(cr.atlas_entities)}")
    for e in cr.atlas_entities:
        print(f"      · {e.qualified_name}  classifications={e.classification_names()}")

    # PII masking policies must exist
    mask_policies = [p for p in cr.ranger_policies if "mask" in p.name]
    assert mask_policies, "Expected masking policies for PII columns"

    # DENY policy for SELECT * on PII table
    deny_policies = [p for p in cr.ranger_policies if "deny" in p.name]
    assert deny_policies, "Expected deny-select-star policy for PII table"

    # PII classification on entity
    pii_entity = cr.atlas_entities[0]
    assert pii_entity.has_classification("PII"), "Entity must carry PII classification"

    # -----------------------------------------------------------------------
    # Step 3: Outbound Port — write to temp outputs dir
    # -----------------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix="gac_outputs_") as tmpdir:
        outputs_dir = Path(tmpdir)
        write_result = write_compilation_result(cr, outputs_dir=outputs_dir)
        assert isinstance(write_result, Success), f"Write failed: {write_result}"

        report = write_result.value
        print(f"\n[3] Outbound Port wrote artefacts to: {outputs_dir}")
        print(f"    Ranger JSON files : {len(report.ranger_paths)}")
        for p in report.ranger_paths:
            print(f"      · {p.name}")
        print(f"    TypeDef JSON files: {len(report.typedef_paths)}")
        for p in report.typedef_paths:
            print(f"      · {p.name}")
        print(f"    Entity JSON files : {len(report.entity_paths)}")
        for p in report.entity_paths:
            print(f"      · {p.name}")

        # Verify manifest
        manifest_path = report.manifest_path
        assert manifest_path.exists(), "manifest.json must exist"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["isComplete"] is True
        assert manifest["gitSha"] == git_sha
        assert manifest["summary"]["rangerPolicies"] == len(report.ranger_paths)
        print(f"    manifest.json     : ✓ (isComplete=True)")

        # Verify a Ranger policy JSON structure
        if report.ranger_paths:
            rp_data = json.loads(report.ranger_paths[0].read_text())
            assert "name" in rp_data
            assert "service" in rp_data
            assert "_gac" in rp_data
            assert rp_data["_gac"]["gitSha"] == git_sha
            print(f"\n[4] Sample Ranger policy JSON:")
            print(json.dumps(rp_data, indent=2)[:600] + "\n    ...")

        # Verify an Atlas TypeDef JSON structure
        if report.typedef_paths:
            td_data = json.loads(report.typedef_paths[0].read_text())
            assert "entityDefs" in td_data
            assert td_data["entityDefs"][0]["superTypes"] == ["DataSet"]
            print(f"\n[5] Sample Atlas TypeDef JSON:")
            print(json.dumps(td_data, indent=2)[:600] + "\n    ...")

        # Verify an Atlas Entity JSON structure
        if report.entity_paths:
            ent_data = json.loads(report.entity_paths[0].read_text())
            assert ent_data["entity"]["typeName"].startswith("TrinoTable_")
            clf_names = [c["typeName"] for c in ent_data["entity"]["classifications"]]
            assert "PII" in clf_names, f"PII classification missing from entity JSON: {clf_names}"
            print(f"\n[6] Sample Atlas Entity JSON:")
            print(json.dumps(ent_data, indent=2)[:600] + "\n    ...")

    # -----------------------------------------------------------------------
    # Step 4: Non-PII table — no masking policies, no deny
    # -----------------------------------------------------------------------
    sql_safe = "SELECT id, value FROM staging.orders WHERE id = 1"
    plan_safe = generate_trino_execution_plan(sql_safe)
    cr_safe_res = compile_plan(plan_safe, git_sha="safe999")
    assert isinstance(cr_safe_res, Success)
    cr_safe = cr_safe_res.value
    assert not any("mask" in p.name for p in cr_safe.ranger_policies), \
        "Non-PII table must not generate masking policies"
    assert not any("deny" in p.name for p in cr_safe.ranger_policies), \
        "Non-PII table must not generate deny policies"
    print(f"\n[7] Non-PII table produces {len(cr_safe.ranger_policies)} simple allow polic(y/ies) — ✓")

    print("\n✓ End-to-end pipeline smoke test passed.")


if __name__ == "__main__":
    run()
