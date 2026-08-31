"""
Quick smoke test for generate_trino_execution_plan.
Run from repo root: python src/local_proxy/_smoke_test.py
"""
import sys, json
sys.path.insert(0, "src")

from local_proxy import generate_trino_execution_plan

# ----- Test 1: PII table + SELECT * -----
sql_pii = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
plan = generate_trino_execution_plan(sql_pii)

assert plan["isStub"] is True
assert "prod.pii_table" in plan["inputTables"]
assert "prod.pii_table" in plan["piiTables"]
assert plan["hasSelectStar"] is True

root = plan["plan"]
assert root["name"] == "Output"

gov = root["governanceSummary"]
assert "PII" in gov["classificationTags"]
assert gov["requiresMasking"] is True
assert gov["selectStarOnPii"] is True
assert gov["recommendedAdtVariant"] == "Mask(columns)"

table_scans = []
def collect(node):
    if node["name"] == "TableScan": table_scans.append(node)
    for c in node.get("children", []): collect(c)
collect(root)

assert len(table_scans) == 1
ts = table_scans[0]
assert ts["descriptor"]["qualifiedName"] == "prod.pii_table"
assert ts["descriptor"]["constraint"]["tag"] == "PII"
assert ts["descriptor"]["enforcedConstraint"]["masking"] == "HASH"
assert ts["descriptor"]["isPiiSource"] is True

pii_cols = [c for c in ts["outputs"] if c["isPii"]]
assert len(pii_cols) > 0, "Expected at least one PII column in TableScan outputs"
print(f"  PII columns: {[c['name'] for c in pii_cols]}")

# ----- Test 2: Non-PII table -----
sql_safe = "SELECT id, value FROM staging.orders WHERE id = 1"
plan2 = generate_trino_execution_plan(sql_safe)
assert plan2["piiTables"] == []
assert plan2["plan"]["governanceSummary"]["requiresMasking"] is False
assert plan2["plan"]["governanceSummary"]["recommendedAdtVariant"] == "Allow"

# ----- Test 3: Determinism — same SQL produces same query ID -----
plan3a = generate_trino_execution_plan(sql_pii)
plan3b = generate_trino_execution_plan(sql_pii)
assert plan3a["id"] == plan3b["id"], "Plan ID must be deterministic"

# ----- Test 4: JOIN with PII table -----
sql_join = "SELECT o.id, p.email FROM staging.orders o JOIN prod.pii_table p ON o.customer_id = p.customer_id"
plan4 = generate_trino_execution_plan(sql_join)
assert "prod.pii_table" in plan4["piiTables"]
assert "staging.orders" in plan4["inputTables"]
assert len(plan4["inputTables"]) == 2

print("\nAll smoke tests passed ✓")
print(f"\nSample plan output (PII query):\n{json.dumps(plan, indent=2, default=str)}")
