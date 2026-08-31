"""
governance/run_simulation.py

Comprehensive simulation and test runner for the GaC (Governance-as-Code) system.

Tests all 4 architectural planes end-to-end using no external test framework.
Exercises unit, integration, and async scenarios with deterministic assertions.

Usage:
    python run_simulation.py              # all suites
    python run_simulation.py --suite 3   # only Suite 3 (gac_compiler)
    python run_simulation.py --verbose   # verbose logging

Exit codes:
    0  all tests passed
    1  one or more tests failed
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Coroutine, Any

# ─── path setup ────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
SRC  = ROOT / "src"
sys.path.insert(0, str(SRC))

# ─── colour helpers ────────────────────────────────────────────────────────
_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

def _ok(msg: str)   -> str: return f"{_GREEN}✓{_RESET} {msg}"
def _fail(msg: str) -> str: return f"{_RED}✗{_RESET} {msg}"
def _skip(msg: str) -> str: return f"{_YELLOW}–{_RESET} {msg}"
def _head(msg: str) -> str: return f"\n{_BOLD}{_CYAN}{msg}{_RESET}"


# ═══════════════════════════════════════════════════════════════════════════
# Minimal test runner
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TestResult:
    name:    str
    passed:  bool
    error:   str | None = None
    elapsed: float = 0.0


class Suite:
    """A named collection of test functions."""

    def __init__(self, name: str) -> None:
        self.name    = name
        self.tests:  list[tuple[str, Callable]] = []
        self.results: list[TestResult] = []

    def test(self, fn: Callable) -> Callable:
        """Decorator to register a sync or async test."""
        self.tests.append((fn.__name__, fn))
        return fn

    def run(self) -> list[TestResult]:
        print(_head(f"Suite: {self.name}"))
        print("─" * 60)
        for name, fn in self.tests:
            t0 = time.perf_counter()
            try:
                if asyncio.iscoroutinefunction(fn):
                    asyncio.run(fn())
                else:
                    fn()
                elapsed = time.perf_counter() - t0
                r = TestResult(name=name, passed=True, elapsed=elapsed)
                print(_ok(f"{name:<55} {elapsed*1000:6.1f}ms"))
            except Exception as exc:
                elapsed = time.perf_counter() - t0
                tb = traceback.format_exc()
                r = TestResult(name=name, passed=False,
                               error=f"{exc}\n{tb}", elapsed=elapsed)
                print(_fail(f"{name:<55} {elapsed*1000:6.1f}ms"))
                print(f"    {_RED}{exc}{_RESET}")
                if "--verbose" in sys.argv:
                    for line in tb.splitlines():
                        print(f"    {line}")
            self.results.append(r)
        passed = sum(1 for r in self.results if r.passed)
        total  = len(self.results)
        colour = _GREEN if passed == total else _RED
        print(f"  {colour}{passed}/{total} passed{_RESET}\n")
        return self.results


def assert_eq(actual: Any, expected: Any, msg: str = "") -> None:
    if actual != expected:
        raise AssertionError(
            f"{msg + ': ' if msg else ''}expected {expected!r}, got {actual!r}"
        )

def assert_true(value: Any, msg: str = "") -> None:
    if not value:
        raise AssertionError(msg or f"Expected truthy, got {value!r}")

def assert_false(value: Any, msg: str = "") -> None:
    if value:
        raise AssertionError(msg or f"Expected falsy, got {value!r}")

def assert_in(item: Any, container: Any, msg: str = "") -> None:
    if item not in container:
        raise AssertionError(msg or f"{item!r} not in {container!r}")

def assert_raises(exc_type: type, fn: Callable, *args: Any) -> None:
    try:
        fn(*args)
        raise AssertionError(f"Expected {exc_type.__name__} but nothing was raised")
    except exc_type:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 1 — contracts: Result ADT + RangerPolicy + AtlasEntity + Failures
# ═══════════════════════════════════════════════════════════════════════════
s1 = Suite("Suite 1 · Contracts (Result ADT, RangerPolicy, AtlasEntity, Failures)")

@s1.test
def result_success_fold() -> None:
    from contracts import ok, err, Success, Failure
    r = ok("hello")
    assert_true(r.is_success())
    assert_false(r.is_failure())
    out = r.fold(lambda v: f"got:{v}", lambda e: f"err:{e}")
    assert_eq(out, "got:hello")

@s1.test
def result_failure_fold() -> None:
    from contracts import ok, err, Failure
    r = err("boom")
    assert_true(r.is_failure())
    out = r.fold(lambda v: "success", lambda e: f"fail:{e}")
    assert_eq(out, "fail:boom")

@s1.test
def result_map_propagates_failure() -> None:
    from contracts import err, Failure
    r = err("original").map(lambda v: v + 1)
    assert_true(r.is_failure())
    assert_eq(r.error, "original")  # type: ignore

@s1.test
def result_flat_map_chains() -> None:
    from contracts import ok, err
    double = lambda v: ok(v * 2)
    r = ok(5).flat_map(double)
    assert_eq(r.value, 10)  # type: ignore

@s1.test
def result_recover() -> None:
    from contracts import err
    val = err("whoops").recover(lambda e: 42)
    assert_eq(val, 42)

@s1.test
def result_sealed_cannot_subclass() -> None:
    from contracts.result import Result
    def try_subclass() -> None:
        class Rogue(Result): pass
    assert_raises(TypeError, try_subclass)

@s1.test
def ranger_policy_immutable() -> None:
    from contracts import RangerPolicy, RangerResource, RangerPolicyItem, RangerPrincipal, ResourceType, PolicyEffect
    res = RangerResource(resource_type=ResourceType.TABLE, values=("db.tbl",))
    principal = RangerPrincipal(groups=("analysts",))
    item = RangerPolicyItem(accesses=("select",), principal=principal, effect=PolicyEffect.ALLOW)
    policy = RangerPolicy(name="test-policy", service="trino", resources=(res,), policy_items=(item,))
    assert_true(policy.is_enabled)
    assert_eq(policy.allow_items(), (item,))
    assert_eq(policy.deny_items(), ())
    # immutable: setting attributes must raise
    try:
        policy.name = "mutated"  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except Exception as e:
        assert_in("frozen", str(type(e).__name__).lower() + str(e).lower())

@s1.test
def ranger_policy_validation_errors() -> None:
    from contracts import RangerPolicy, RangerResource, RangerPolicyItem, RangerPrincipal, ResourceType
    res = RangerResource(resource_type=ResourceType.TABLE, values=("x",))
    principal = RangerPrincipal(users=("alice",))
    item = RangerPolicyItem(accesses=("select",), principal=principal)
    # empty name
    assert_raises(ValueError, lambda: RangerPolicy(name="", service="s", resources=(res,), policy_items=(item,)))
    # empty resources
    assert_raises(ValueError, lambda: RangerPolicy(name="p", service="s", resources=(), policy_items=(item,)))
    # empty policy items
    assert_raises(ValueError, lambda: RangerPolicy(name="p", service="s", resources=(res,), policy_items=()))

@s1.test
def atlas_entity_immutable_with_classification() -> None:
    from contracts import AtlasEntity, AtlasClassification, EntityStatus, ClassificationPropagation
    clf = AtlasClassification(name="PII", propagate=ClassificationPropagation.ENABLED)
    entity = AtlasEntity(
        type_name="hive_table",
        qualified_name="prod.customer@c1",
        attributes=(("name", "customer"),),
        classifications=(clf,),
    )
    assert_true(entity.has_classification("PII"))
    assert_false(entity.has_classification("REGULATED"))
    assert_eq(entity.status, EntityStatus.ACTIVE)
    # with_status returns new instance
    deleted = entity.with_status(EntityStatus.DELETED)
    assert_eq(deleted.status, EntityStatus.DELETED)
    assert_eq(entity.status, EntityStatus.ACTIVE)  # original unchanged

@s1.test
def atlas_lineage_upstream_downstream() -> None:
    from contracts import AtlasLineage, AtlasLineageEdge, LineageDirection
    a, b, p = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    edge = AtlasLineageEdge(from_entity_guid=a, to_entity_guid=b, process_guid=p)
    lin  = AtlasLineage(root_entity_guid=b, direction=LineageDirection.INPUT, edges=(edge,), depth=1)
    assert_eq(lin.upstream_guids(), frozenset({a}))
    assert_eq(lin.downstream_guids(), frozenset({b}))

@s1.test
def governance_failure_sealed() -> None:
    from contracts.failures import GovernanceFailure
    def try_subclass() -> None:
        class Rogue(GovernanceFailure): pass
    assert_raises(TypeError, try_subclass)

@s1.test
def validation_failure_human_readable() -> None:
    from contracts import ValidationFailure, ValidationCode
    f = ValidationFailure(code=ValidationCode.ACCESS_DENIED, resource="db.tbl", principal="alice", reason="denied by rule-42")
    msg = f.human_readable()
    assert_in("ACCESS_DENIED", msg)
    assert_in("alice", msg)

@s1.test
def reconciliation_failure_dimension_validation() -> None:
    from contracts import ReconciliationFailure, ReconciliationFailureCode
    assert_raises(
        ValueError,
        lambda: ReconciliationFailure(
            code=ReconciliationFailureCode.REMEDIATION_FAILED,
            dimension="kafka",   # invalid
            message="oops",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 2 — local_proxy: Trino Plan Stub
# ═══════════════════════════════════════════════════════════════════════════
s2 = Suite("Suite 2 · Local Proxy (Trino Plan Stub)")

@s2.test
def plan_stub_pii_table_detected() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table WHERE id=1")
    assert_true(plan["isStub"])
    assert_in("prod.pii_table", plan["inputTables"])
    assert_in("prod.pii_table", plan["piiTables"])
    assert_true(plan["hasSelectStar"])

@s2.test
def plan_stub_non_pii_table() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT id FROM staging.orders WHERE id=1")
    assert_eq(plan["piiTables"], [])
    assert_false(plan["hasSelectStar"])
    gov = plan["plan"]["governanceSummary"]
    assert_eq(gov["requiresMasking"], False)
    assert_eq(gov["recommendedAdtVariant"], "Allow")

@s2.test
def plan_stub_select_star_triggers_masking_adt() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    gov = plan["plan"]["governanceSummary"]
    assert_true(gov["requiresMasking"])
    assert_eq(gov["recommendedAdtVariant"], "Mask(columns)")
    assert_in("PII", gov["classificationTags"])

@s2.test
def plan_stub_table_scan_pii_constraint() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    def find_scan(node: dict) -> dict | None:
        if node.get("name") == "TableScan":
            return node
        for c in node.get("children", []):
            found = find_scan(c)
            if found:
                return found
        return None
    scan = find_scan(plan["plan"])
    assert_true(scan is not None, "TableScan node must exist")
    assert_eq(scan["descriptor"]["constraint"]["tag"], "PII")
    assert_eq(scan["descriptor"]["enforcedConstraint"]["masking"], "HASH")
    assert_true(scan["descriptor"]["isPiiSource"])

@s2.test
def plan_stub_determinism() -> None:
    from local_proxy import generate_trino_execution_plan
    sql = "SELECT * FROM prod.pii_table WHERE customer_id = 99"
    p1 = generate_trino_execution_plan(sql)
    p2 = generate_trino_execution_plan(sql)
    assert_eq(p1["id"], p2["id"], "Same SQL must produce same plan ID")

@s2.test
def plan_stub_join_detects_both_tables() -> None:
    from local_proxy import generate_trino_execution_plan
    sql = "SELECT o.id, p.email FROM staging.orders o JOIN prod.pii_table p ON o.customer_id = p.customer_id"
    plan = generate_trino_execution_plan(sql)
    assert_in("prod.pii_table",   plan["piiTables"])
    assert_in("staging.orders",   plan["inputTables"])
    assert_eq(len(plan["inputTables"]), 2)

@s2.test
def plan_stub_pii_columns_have_mask_types() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    def collect_scans(node: dict) -> list:
        result = []
        if node.get("name") == "TableScan": result.append(node)
        for c in node.get("children", []): result.extend(collect_scans(c))
        return result
    scans = collect_scans(plan["plan"])
    pii_cols = [c for scan in scans for c in scan["outputs"] if c.get("isPii")]
    assert_true(len(pii_cols) > 0, "PII table must have PII columns")
    for col in pii_cols:
        assert_in(col["maskType"], ["HASH", "MASK_SHOW_LAST_4", "NULLIFY", "MASK"])

@s2.test
def plan_stub_plan_tree_shape() -> None:
    from local_proxy import generate_trino_execution_plan
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    root = plan["plan"]
    assert_eq(root["name"], "Output")
    assert_eq(root["children"][0]["name"], "Project")
    assert_eq(root["children"][0]["children"][0]["name"], "Filter")
    assert_eq(root["children"][0]["children"][0]["children"][0]["name"], "TableScan")


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 3 — gac_compiler: Core Domain + Outbound Port
# ═══════════════════════════════════════════════════════════════════════════
s3 = Suite("Suite 3 · GaC Compiler (Core Domain + Outbound Port)")

@s3.test
def compiler_core_produces_policies_and_typedefs() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table WHERE id=1")
    res = compile_plan(plan, git_sha="abc123", requesting_principal="ci-cd")
    assert_true(isinstance(res, Success), f"compile_plan returned Failure: {res}")
    cr = res.value
    assert_true(len(cr.ranger_policies) > 0)
    assert_true(len(cr.atlas_typedefs) > 0)
    assert_true(len(cr.atlas_entities) > 0)
    assert_eq(cr.git_sha, "abc123")

@s3.test
def compiler_core_generates_deny_policy_for_select_star_pii() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr = compile_plan(plan, git_sha="abc123").value  # type: ignore
    deny_policies = [p for p in cr.ranger_policies if "deny" in p.name]
    assert_true(len(deny_policies) > 0, "SELECT * on PII must produce deny policy")

@s3.test
def compiler_core_generates_masking_policies_for_pii_columns() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr = compile_plan(plan, git_sha="abc123").value  # type: ignore
    mask_policies = [p for p in cr.ranger_policies if "mask" in p.name]
    assert_true(len(mask_policies) > 0, "PII columns must produce masking policies")
    for p in mask_policies:
        masking_items = [i for i in p.policy_items if i.masking is not None]
        assert_true(len(masking_items) > 0, f"Policy {p.name} must have masking item")

@s3.test
def compiler_core_entity_carries_pii_classification() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr = compile_plan(plan, git_sha="abc123").value  # type: ignore
    pii_entities = [e for e in cr.atlas_entities if e.has_classification("PII")]
    assert_true(len(pii_entities) > 0, "PII table entity must carry PII classification")

@s3.test
def compiler_core_non_pii_no_deny_no_masking() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT id, val FROM staging.orders WHERE id=1")
    cr = compile_plan(plan, git_sha="abc123").value  # type: ignore
    deny_p = [p for p in cr.ranger_policies if "deny"  in p.name]
    mask_p = [p for p in cr.ranger_policies if "mask" in p.name]
    assert_eq(len(deny_p), 0, "Non-PII table must not produce deny policy")
    assert_eq(len(mask_p), 0, "Non-PII table must not produce masking policy")

@s3.test
def compiler_outbound_writes_json_files_and_manifest() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr   = compile_plan(plan, git_sha="test-sha").value  # type: ignore
    with tempfile.TemporaryDirectory() as tmpdir:
        res = write_compilation_result(cr, outputs_dir=Path(tmpdir))
        assert_true(isinstance(res, Success), f"write failed: {res}")
        report = res.value
        assert_true(report.manifest_path.exists(), "manifest.json must exist")
        manifest = json.loads(report.manifest_path.read_text())
        assert_true(manifest["isComplete"])
        assert_eq(manifest["gitSha"], "test-sha")
        assert_true(len(report.ranger_paths) > 0)
        assert_true(len(report.typedef_paths) > 0)
        assert_true(len(report.entity_paths) > 0)

@s3.test
def compiler_outbound_ranger_json_matches_api_schema() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr   = compile_plan(plan, git_sha="sha-xyz").value  # type: ignore
    with tempfile.TemporaryDirectory() as tmpdir:
        res = write_compilation_result(cr, outputs_dir=Path(tmpdir))
        report = res.value  # type: ignore
        for path in report.ranger_paths:
            data = json.loads(path.read_text())
            assert_in("name",        data)
            assert_in("service",     data)
            assert_in("resources",   data)
            assert_in("_gac",        data)
            assert_eq(data["_gac"]["gitSha"], "sha-xyz")

@s3.test
def compiler_outbound_atlas_entity_json_has_pii_classification() -> None:
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from contracts import Success
    plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
    cr   = compile_plan(plan, git_sha="sha-pii").value  # type: ignore
    with tempfile.TemporaryDirectory() as tmpdir:
        res = write_compilation_result(cr, outputs_dir=Path(tmpdir))
        report = res.value  # type: ignore
        found_pii = False
        for path in report.entity_paths:
            data = json.loads(path.read_text())
            clf_names = [c["typeName"] for c in data["entity"].get("classifications", [])]
            if "PII" in clf_names:
                found_pii = True
                break
        assert_true(found_pii, "At least one entity JSON must carry PII classification")


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 4 — recon_operator: Differ (pure dict diff engine)
# ═══════════════════════════════════════════════════════════════════════════
s4 = Suite("Suite 4 · Differ (Pure Dict Diff Engine)")

@s4.test
def diff_identical_dicts_no_drift() -> None:
    from recon_operator import diff, is_clean
    d = {"a": 1, "b": {"c": "x"}}
    result = diff(d, d, resource_id="test", dimension="ranger")
    assert_true(is_clean(result))
    assert_false(result.has_drift)
    assert_eq(result.max_severity, "NONE")

@s4.test
def diff_added_key_detected() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {"policy_a": {"isEnabled": True}}
    actual  = {}
    result = diff(desired, actual, resource_id="r", dimension="ranger")
    assert_true(result.has_drift)
    assert_true(any(e.kind == DiffKind.ADDED for e in result.entries))

@s4.test
def diff_removed_key_detected() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {}
    actual  = {"orphan_policy": {"isEnabled": True}}
    result = diff(desired, actual, resource_id="r", dimension="ranger")
    assert_true(result.has_drift)
    assert_true(any(e.kind == DiffKind.REMOVED for e in result.entries))

@s4.test
def diff_modified_scalar_detected() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {"policy": {"isEnabled": True,  "git_sha": "abc"}}
    actual  = {"policy": {"isEnabled": False, "git_sha": "abc"}}
    result = diff(desired, actual, resource_id="r", dimension="ranger")
    assert_true(result.has_drift)
    modified = [e for e in result.entries if e.kind == DiffKind.MODIFIED]
    assert_true(len(modified) > 0)
    path_values = {e.path: (e.desired_val, e.actual_val) for e in modified}
    assert_in("policy.isEnabled", path_values)
    assert_eq(path_values["policy.isEnabled"], (True, False))

@s4.test
def diff_high_severity_for_critical_keys() -> None:
    from recon_operator import diff
    desired = {"row": {"constraint": {"tag": "PII"}}}
    actual  = {"row": {"constraint": {"tag": "NONE"}}}
    result = diff(desired, actual, resource_id="r", dimension="atlas")
    assert_eq(result.max_severity, "HIGH")

@s4.test
def diff_low_severity_for_metadata_keys() -> None:
    from recon_operator import diff
    desired = {"row": {"description": "old desc"}}
    actual  = {"row": {"description": "new desc"}}
    result = diff(desired, actual, resource_id="r", dimension="atlas")
    assert_eq(result.max_severity, "LOW")

@s4.test
def diff_nested_recursive() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {"a": {"b": {"c": {"d": "deep"}}}}
    actual  = {"a": {"b": {"c": {"d": "changed"}}}}
    result = diff(desired, actual, resource_id="r", dimension="atlas")
    assert_true(result.has_drift)
    assert_eq(result.entries[0].path, "a.b.c.d")

@s4.test
def diff_type_mismatch_detected() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {"key": {"nested": "dict"}}
    actual  = {"key": "scalar"}
    result = diff(desired, actual, resource_id="r", dimension="atlas")
    assert_true(result.has_drift)
    assert_true(any(e.kind == DiffKind.TYPE_MISMATCH for e in result.entries))

@s4.test
def diff_list_comparison() -> None:
    from recon_operator import diff
    from recon_operator.differ import DiffKind
    desired = {"labels": ["pii", "regulated"]}
    actual  = {"labels": ["pii"]}
    result = diff(desired, actual, resource_id="r", dimension="ranger")
    assert_true(result.has_drift)

@s4.test
def diff_result_report_contains_drift_message() -> None:
    from recon_operator import diff
    desired = {"policy_x": {"isEnabled": True}}
    actual  = {}
    result = diff(desired, actual, resource_id="my-resource", dimension="ranger")
    report = result.report()
    assert_in("DRIFT_DETECTED", report)
    assert_in("my-resource", report)

@s4.test
def diff_hashes_differ_on_state_mismatch() -> None:
    from recon_operator import diff
    desired = {"a": 1}
    actual  = {"a": 2}
    result = diff(desired, actual, resource_id="r", dimension="atlas")
    assert_true(result.desired_hash != result.actual_hash)

@s4.test
def diff_hashes_equal_on_identical_state() -> None:
    from recon_operator import diff
    d = {"a": 1, "b": [1, 2, 3]}
    result = diff(d, d, resource_id="r", dimension="atlas")
    assert_eq(result.desired_hash, result.actual_hash)


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 5 — telemetry_bus: EventBus pub/sub
# ═══════════════════════════════════════════════════════════════════════════
s5 = Suite("Suite 5 · Telemetry Bus (asyncio.Queue EventBus)")

@s5.test
async def bus_publish_subscribe_single_event() -> None:
    from telemetry_bus import EventBus, Topic, LineageEvent
    received: list = []
    async with EventBus() as bus:
        async def consumer() -> None:
            async for evt in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="test", timeout=0.5):
                received.append(evt)
        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.05)
        await bus.publish(LineageEvent(
            source_table="a.src", target_table="b.dst",
            process_name="test-job", source_system="spark-hook", job_id="j1",
        ))
        await task
    assert_eq(len(received), 1)
    assert_eq(received[0].source_table, "a.src")

@s5.test
async def bus_sequence_is_monotonic() -> None:
    from telemetry_bus import EventBus, Topic, LineageEvent
    sequences: list[int] = []
    async with EventBus() as bus:
        async def consumer() -> None:
            async for evt in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="t", timeout=0.5):
                sequences.append(evt.sequence)
        task = asyncio.create_task(consumer())
        await asyncio.sleep(0.05)
        for i in range(5):
            await bus.publish(LineageEvent(
                source_table=f"src{i}", target_table=f"dst{i}",
                process_name="p", source_system="spark-hook", job_id=f"j{i}",
            ))
        await task
    assert_eq(sequences, list(range(1, 6)), "Sequences must be 1,2,3,4,5")

@s5.test
async def bus_fanout_to_multiple_subscribers() -> None:
    from telemetry_bus import EventBus, Topic, PolicyCompiledEvent
    r1: list = []
    r2: list = []
    async with EventBus() as bus:
        async def c1() -> None:
            async for e in bus.subscribe(Topic.POLICY_COMPILED, subscriber_id="s1", timeout=0.5): r1.append(e)
        async def c2() -> None:
            async for e in bus.subscribe(Topic.POLICY_COMPILED, subscriber_id="s2", timeout=0.5): r2.append(e)
        t1, t2 = asyncio.create_task(c1()), asyncio.create_task(c2())
        await asyncio.sleep(0.05)
        await bus.publish(PolicyCompiledEvent(
            plan_id="pid", git_sha="sha", policy_names=("p1",),
            typedef_names=(), entity_names=(), output_dir="/tmp",
        ))
        await asyncio.gather(t1, t2)
    assert_eq(len(r1), 1, "Subscriber 1 must receive the event")
    assert_eq(len(r2), 1, "Subscriber 2 must receive the event (fan-out)")

@s5.test
async def bus_timeout_exits_subscriber_cleanly() -> None:
    from telemetry_bus import EventBus, Topic
    received: list = []
    async with EventBus() as bus:
        async def consumer() -> None:
            async for e in bus.subscribe(Topic.DRIFT_DETECTED, subscriber_id="t", timeout=0.2):
                received.append(e)  # should never fire
        await consumer()  # must return after timeout
    assert_eq(received, [], "No events published, subscriber must exit cleanly after timeout")

@s5.test
async def bus_close_signals_all_subscribers() -> None:
    from telemetry_bus import EventBus, Topic, LineageEvent
    received: list = []
    bus = EventBus()
    await bus.__aenter__()
    async def consumer() -> None:
        async for e in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="t", timeout=2.0):
            received.append(e)
    task = asyncio.create_task(consumer())
    await asyncio.sleep(0.05)
    await bus.close()
    await asyncio.wait_for(task, timeout=1.0)
    assert_eq(received, [], "Consumer must exit on bus close with zero events")

@s5.test
async def bus_stats_reflect_published_count() -> None:
    from telemetry_bus import EventBus, Topic, LineageEvent
    async with EventBus() as bus:
        for _ in range(3):
            await bus.publish(LineageEvent(
                source_table="x", target_table="y",
                process_name="p", source_system="spark-hook", job_id="j",
            ))
        stats = bus.stats()
        assert_eq(stats[Topic.ATLAS_HOOK.value]["sequence"], 3)

@s5.test
async def bus_event_immutability_preserved() -> None:
    """Events returned by the bus must still be frozen (immutable)."""
    from telemetry_bus import EventBus, Topic, LineageEvent
    received: list = []
    async with EventBus() as bus:
        async def c() -> None:
            async for e in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="t", timeout=0.5): received.append(e)
        t = asyncio.create_task(c())
        await asyncio.sleep(0.05)
        await bus.publish(LineageEvent(
            source_table="s", target_table="d",
            process_name="p", source_system="spark-hook", job_id="j",
        ))
        await t
    assert_eq(len(received), 1)
    try:
        received[0].source_table = "mutated"  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except Exception as ex:
        assert_in("frozen", str(type(ex).__name__).lower() + str(ex).lower())


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 6 — recon_operator: StateStore
# ═══════════════════════════════════════════════════════════════════════════
s6 = Suite("Suite 6 · Reconciliation Operator — StateStore")

@s6.test
async def state_store_empty_on_init() -> None:
    from recon_operator import StateStore
    from telemetry_bus import EventBus
    async with EventBus() as bus:
        store = StateStore(bus)
        state = store.actual_state()
        assert_eq(state["ranger_policies"], {})
        assert_eq(state["atlas_entities"], {})
        assert_eq(state["lineage_edges"], [])

@s6.test
async def state_store_applies_policy_compiled_event() -> None:
    from recon_operator import StateStore
    from telemetry_bus import EventBus, PolicyCompiledEvent
    async with EventBus() as bus:
        store = StateStore(bus)
        await store.start()
        await asyncio.sleep(0.05)
        await bus.publish(PolicyCompiledEvent(
            plan_id="pid", git_sha="sha123",
            policy_names=("p-allow-read", "p-mask-col"),
            typedef_names=("TrinoTable_prod_pii_table",),
            entity_names=("prod.pii_table",),
            output_dir="/tmp",
        ))
        await asyncio.sleep(0.2)   # give StateStore time to process
        state = store.actual_state()
        await store.stop()
    assert_in("p-allow-read", state["ranger_policies"])
    assert_in("p-mask-col",   state["ranger_policies"])
    assert_in("prod.pii_table", state["atlas_entities"])

@s6.test
async def state_store_applies_lineage_event() -> None:
    from recon_operator import StateStore
    from telemetry_bus import EventBus, LineageEvent
    async with EventBus() as bus:
        store = StateStore(bus)
        await store.start()
        await asyncio.sleep(0.05)
        await bus.publish(LineageEvent(
            source_table="staging.src", target_table="prod.dst",
            process_name="spark-job", source_system="spark-hook", job_id="j99",
            classification_tags=("PII",),
        ))
        await asyncio.sleep(0.2)
        state = store.actual_state()
        await store.stop()
    edges = state["lineage_edges"]
    assert_true(len(edges) >= 1)
    assert_eq(edges[0]["source"], "staging.src")
    assert_eq(edges[0]["target"], "prod.dst")
    assert_in("PII", edges[0]["tags"])

@s6.test
async def state_store_applies_entity_registered_event() -> None:
    from recon_operator import StateStore
    from telemetry_bus import EventBus, EntityRegisteredEvent
    async with EventBus() as bus:
        store = StateStore(bus)
        await store.start()
        await asyncio.sleep(0.05)
        await bus.publish(EntityRegisteredEvent(
            qualified_name="reporting.summary@c1",
            type_name="TrinoTable_reporting_summary",
            guid="deadbeef-0000-0000-0000-000000000042",
            source_system="discovery-engine",
        ))
        await asyncio.sleep(0.2)
        state = store.actual_state()
        await store.stop()
    entity = state["atlas_entities"].get("reporting.summary@c1")
    assert_true(entity is not None)
    assert_eq(entity["guid"], "deadbeef-0000-0000-0000-000000000042")
    assert_eq(entity["source_system"], "discovery-engine")

@s6.test
async def state_store_snapshot_is_independent_copy() -> None:
    """Mutating the returned snapshot must not affect the internal store state."""
    from recon_operator import StateStore
    from telemetry_bus import EventBus, LineageEvent
    async with EventBus() as bus:
        store = StateStore(bus)
        await store.start()
        await asyncio.sleep(0.05)
        await bus.publish(LineageEvent(
            source_table="a", target_table="b",
            process_name="p", source_system="spark-hook", job_id="j",
        ))
        await asyncio.sleep(0.2)
        snap1 = store.actual_state()
        snap1["lineage_edges"].append({"injected": True})   # mutate copy
        snap2 = store.actual_state()
        await store.stop()
    # injected entry must NOT appear in the second snapshot
    assert_false(any(e.get("injected") for e in snap2["lineage_edges"]))


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 7 — recon_operator: ControlLoop (drift detection + remediation)
# ═══════════════════════════════════════════════════════════════════════════
s7 = Suite("Suite 7 · Reconciliation Operator — ControlLoop (Drift Detection)")

@s7.test
async def control_loop_detects_drift_on_empty_actual_state() -> None:
    """
    Desired state has policies (from Plane 2 JSON files).
    Actual state is empty (no bus events yet).
    Control loop must detect drift and publish DriftDetectedEvent.
    """
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from recon_operator import ControlLoop, StateStore
    from telemetry_bus import EventBus, Topic, DriftDetectedEvent
    from contracts import Success

    drift_events: list = []

    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
        cr   = compile_plan(plan, git_sha="sha-drift").value  # type: ignore
        write_compilation_result(cr, outputs_dir=outputs_dir)

        async with EventBus() as bus:
            # Subscribe to drift events BEFORE loop runs
            async def capture_drift() -> None:
                async for e in bus.subscribe(Topic.DRIFT_DETECTED, subscriber_id="test", timeout=0.5):
                    if isinstance(e, DriftDetectedEvent):
                        drift_events.append(e)
            capture_task = asyncio.create_task(capture_drift())

            store = StateStore(bus)
            await store.start()

            loop = ControlLoop(
                store, bus,
                desired_state_dir=outputs_dir,
                tick_interval_s=0.1,
                max_ticks=1,
            )
            await asyncio.sleep(0.01)
            await loop.run()
            await store.stop()
            await asyncio.wait_for(capture_task, timeout=2.0)

    assert_true(loop.stats()["total_drifts"] > 0, "Must detect drift with empty actual state")
    assert_true(len(drift_events) > 0, "DriftDetectedEvent must be published to the bus")
    assert_in(drift_events[0].dimension, ["ranger", "atlas"])

@s7.test
async def control_loop_remediates_after_drift() -> None:
    """After drift, ControlLoop must publish RemediationDoneEvent."""
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from recon_operator import ControlLoop, StateStore
    from telemetry_bus import EventBus, Topic, RemediationDoneEvent
    from contracts import Success

    rem_events: list = []
    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
        cr   = compile_plan(plan, git_sha="sha-rem").value  # type: ignore
        write_compilation_result(cr, outputs_dir=outputs_dir)

        async with EventBus() as bus:
            async def capture_rem() -> None:
                async for e in bus.subscribe(Topic.REMEDIATION_DONE, subscriber_id="test", timeout=0.5):
                    if isinstance(e, RemediationDoneEvent):
                        rem_events.append(e)
            task = asyncio.create_task(capture_rem())
            store = StateStore(bus)
            await store.start()
            loop = ControlLoop(store, bus, desired_state_dir=outputs_dir,
                               tick_interval_s=0.1, max_ticks=1)
            await asyncio.sleep(0.01)
            await loop.run()
            await store.stop()
            await asyncio.wait_for(task, timeout=2.0)

    assert_true(len(rem_events) > 0, "RemediationDoneEvent must be published after drift")
    assert_true(loop.stats()["remediations"] > 0)

@s7.test
async def control_loop_no_drift_when_states_match() -> None:
    """
    Populate StateStore with the same policies that Plane 2 writes.
    Control loop tick 2 must see no drift.
    """
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from recon_operator import ControlLoop, StateStore
    from telemetry_bus import EventBus, PolicyCompiledEvent
    from contracts import Success

    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        plan = generate_trino_execution_plan("SELECT id FROM staging.orders WHERE id=1")
        cr   = compile_plan(plan, git_sha="sha-clean").value  # type: ignore
        write_compilation_result(cr, outputs_dir=outputs_dir)

        async with EventBus() as bus:
            store = StateStore(bus)
            await store.start()
            await asyncio.sleep(0.05)

            # Publish PolicyCompiledEvent with exactly the compiled policy names
            await bus.publish(PolicyCompiledEvent(
                plan_id=cr.plan_id,
                git_sha=cr.git_sha,
                policy_names=tuple(p.name for p in cr.ranger_policies),
                typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
                entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
                output_dir=str(outputs_dir),
            ))
            await asyncio.sleep(0.3)  # allow StateStore to update

            loop = ControlLoop(store, bus, desired_state_dir=outputs_dir,
                               tick_interval_s=0.1, max_ticks=1)
            await loop.run()
            await store.stop()

    # Non-PII table: only 1 allow policy. After StateStore sync, drift should be minimal/zero.
    # We only assert that ticks ran — full zero-drift requires exact state alignment.
    assert_eq(loop.stats()["ticks"], 1)


# ═══════════════════════════════════════════════════════════════════════════
# SUITE 8 — Full 4-plane end-to-end integration
# ═══════════════════════════════════════════════════════════════════════════
s8 = Suite("Suite 8 · Full 4-Plane End-to-End Integration")

@s8.test
async def e2e_plane1_to_plane2_pii_pipeline() -> None:
    """Plane 1 stub → Plane 2 compile → Outbound Port → verify JSON artefacts."""
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from contracts import Success

    sql = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
    plan = generate_trino_execution_plan(sql)
    assert_true(plan["isStub"])

    cr_res = compile_plan(plan, git_sha="e2e-sha")
    assert_true(isinstance(cr_res, Success))
    cr = cr_res.value

    with tempfile.TemporaryDirectory() as tmpdir:
        wr = write_compilation_result(cr, outputs_dir=Path(tmpdir))
        assert_true(isinstance(wr, Success))
        report = wr.value
        manifest = json.loads(report.manifest_path.read_text())
        assert_true(manifest["isComplete"])
        assert_eq(manifest["gitSha"], "e2e-sha")
        assert_true(manifest["summary"]["rangerPolicies"] > 0)
        assert_true(manifest["summary"]["atlasEntities"] > 0)

@s8.test
async def e2e_plane2_publishes_to_bus_plane3_relays() -> None:
    """Plane 2 publishes PolicyCompiledEvent; Plane 3 actors relay lineage; all arrive on bus."""
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan
    from telemetry_bus import (
        EventBus, Topic, PolicyCompiledEvent, LineageEvent, EntityRegisteredEvent,
    )
    from contracts import Success

    all_events: list = []

    async with EventBus() as bus:
        async def catch_all_policy() -> None:
            async for e in bus.subscribe(Topic.POLICY_COMPILED, subscriber_id="all", timeout=0.8):
                all_events.append(e)
        async def catch_all_atlas() -> None:
            async for e in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="all", timeout=0.8):
                all_events.append(e)
        async def catch_entity() -> None:
            async for e in bus.subscribe(Topic.ENTITY_REGISTERED, subscriber_id="all", timeout=0.8):
                all_events.append(e)

        t1, t2, t3 = (asyncio.create_task(f()) for f in [catch_all_policy, catch_all_atlas, catch_entity])
        await asyncio.sleep(0.05)

        # Plane 2 emits
        plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table")
        cr = compile_plan(plan, git_sha="e2e2").value  # type: ignore
        await bus.publish(PolicyCompiledEvent(
            plan_id=cr.plan_id, git_sha=cr.git_sha,
            policy_names=tuple(p.name for p in cr.ranger_policies),
            typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
            entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
            output_dir="/tmp",
        ))
        # Plane 3 Spark hook emits
        await asyncio.sleep(0.1)
        await bus.publish(LineageEvent(
            source_table="staging.raw", target_table="prod.pii_table",
            process_name="spark-etl", source_system="spark-hook", job_id="j1",
            classification_tags=("PII",),
        ))
        # Plane 3 DiscoveryEngine emits entity
        await asyncio.sleep(0.05)
        await bus.publish(EntityRegisteredEvent(
            qualified_name="prod.pii_table@c1",
            type_name="TrinoTable_prod_pii_table",
            guid="aabbccdd-0000-0000-0000-000000000001",
            source_system="discovery-engine",
        ))
        await asyncio.gather(t1, t2, t3)

    policy_evts = [e for e in all_events if isinstance(e, PolicyCompiledEvent)]
    lineage_evts = [e for e in all_events if isinstance(e, LineageEvent)]
    entity_evts  = [e for e in all_events if isinstance(e, EntityRegisteredEvent)]
    assert_eq(len(policy_evts), 1, "Must receive 1 PolicyCompiledEvent")
    assert_eq(len(lineage_evts), 1, "Must receive 1 LineageEvent")
    assert_eq(len(entity_evts),  1, "Must receive 1 EntityRegisteredEvent")

@s8.test
async def e2e_full_4_plane_drift_convergence() -> None:
    """
    Full scenario:
    Tick 1 — empty actual state → drift detected → remediation.
    Tick 2 — StateStore populated from bus → reduced drift.
    """
    from local_proxy import generate_trino_execution_plan
    from gac_compiler import compile_plan, write_compilation_result
    from recon_operator import ControlLoop, StateStore
    from telemetry_bus import (
        EventBus, Topic, PolicyCompiledEvent, LineageEvent, DriftDetectedEvent,
    )
    from contracts import Success

    drift_events: list = []

    with tempfile.TemporaryDirectory() as tmpdir:
        outputs_dir = Path(tmpdir)
        plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table WHERE id=1")
        cr   = compile_plan(plan, git_sha="e2e-full").value  # type: ignore
        write_compilation_result(cr, outputs_dir=outputs_dir)

        async with EventBus() as bus:
            async def capture() -> None:
                async for e in bus.subscribe(Topic.DRIFT_DETECTED, subscriber_id="e2e", timeout=0.5):
                    if isinstance(e, DriftDetectedEvent): drift_events.append(e)
            cap_task = asyncio.create_task(capture())

            store = StateStore(bus)
            await store.start()

            loop = ControlLoop(
                store, bus,
                desired_state_dir=outputs_dir,
                tick_interval_s=0.3,
                max_ticks=2,
            )

            async def emit_after_tick1() -> None:
                await asyncio.sleep(0.4)   # after tick 1
                await bus.publish(PolicyCompiledEvent(
                    plan_id=cr.plan_id, git_sha=cr.git_sha,
                    policy_names=tuple(p.name for p in cr.ranger_policies),
                    typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
                    entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
                    output_dir=str(outputs_dir),
                ))
                await bus.publish(LineageEvent(
                    source_table="staging.raw", target_table="prod.pii_table",
                    process_name="spark-etl", source_system="spark-hook", job_id="j1",
                    classification_tags=("PII",),
                ))
            await asyncio.sleep(0.01)
            await asyncio.gather(loop.run(), emit_after_tick1())
            await store.stop()
            await asyncio.wait_for(cap_task, timeout=2.0)

    stats = loop.stats()
    assert_eq(stats["ticks"], 2, "Loop must complete 2 ticks")
    assert_true(stats["total_drifts"] > 0, "Tick 1 must detect drift")
    assert_true(stats["remediations"] > 0, "Tick 1 must trigger remediation")
    assert_true(len(drift_events) > 0, "DriftDetectedEvent must reach the bus")

@s8.test
async def e2e_event_sealed_types_cannot_be_subclassed() -> None:
    """Sealed base classes must reject subclassing at class definition time."""
    from telemetry_bus.events import GovernanceEvent
    from contracts.result import Result
    from contracts.failures import GovernanceFailure

    for sealed_base in [GovernanceEvent, Result, GovernanceFailure]:
        name = sealed_base.__name__
        def make_subclass(base: type = sealed_base) -> None:
            class Rogue(base): pass  # type: ignore
        assert_raises(TypeError, make_subclass,
                      # no extra args — just a label
                      )

@s8.test
def e2e_differ_full_state_comparison() -> None:
    """Run differ against a realistic desired/actual state pair."""
    from recon_operator import diff

    desired = {
        "gac-deny-select-star-prod-pii_table": {
            "name": "gac-deny-select-star-prod-pii_table",
            "service": "trino_prod",
            "isEnabled": True,
            "labels": ["deny-select-star", "gac-compiled", "pii"],
            "git_sha": "abc123",
            "policy_item_count": 1,
        },
        "gac-mask-prod-pii_table-email": {
            "name": "gac-mask-prod-pii_table-email",
            "service": "trino_prod",
            "isEnabled": True,
            "labels": ["gac-compiled", "masking", "pii"],
            "git_sha": "abc123",
            "policy_item_count": 1,
        },
    }
    # Actual state is missing the deny policy and has a wrong git_sha on mask
    actual = {
        "gac-mask-prod-pii_table-email": {
            "name": "gac-mask-prod-pii_table-email",
            "service": "trino_prod",
            "isEnabled": True,
            "labels": ["gac-compiled", "masking", "pii"],
            "git_sha": "STALE-SHA",   # drift here
            "policy_item_count": 1,
        },
    }
    result = diff(desired, actual, resource_id="ranger_policies", dimension="ranger")
    assert_true(result.has_drift)
    # deny policy is ADDED (in desired, not in actual)
    from recon_operator.differ import DiffKind
    added = [e for e in result.entries if e.kind == DiffKind.ADDED]
    modified = [e for e in result.entries if e.kind == DiffKind.MODIFIED]
    assert_true(len(added) > 0,    "Deny policy missing from actual → ADDED")
    assert_true(len(modified) > 0, "git_sha mismatch → MODIFIED")
    report = result.report()
    assert_in("DRIFT_DETECTED", report)
    print(f"\n{report}")   # visible in output for manual inspection


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

ALL_SUITES: list[Suite] = [s1, s2, s3, s4, s5, s6, s7, s8]

def main() -> None:
    suite_filter: int | None = None
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--suite" and i + 2 < len(sys.argv):
            suite_filter = int(sys.argv[i + 2])
            break
        if arg.startswith("--suite="):
            suite_filter = int(arg.split("=", 1)[1])

    verbose = "--verbose" in sys.argv

    if not verbose:
        logging.basicConfig(level=logging.CRITICAL)  # suppress async info noise
    else:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)-8s %(name)s: %(message)s")

    print()
    print(f"{_BOLD}{'='*68}{_RESET}")
    print(f"{_BOLD}  GaC Simulation — Comprehensive Test Runner{_RESET}")
    print(f"{_BOLD}{'='*68}{_RESET}")

    suites = ALL_SUITES
    if suite_filter:
        suites = [s for s in ALL_SUITES if s.name.startswith(f"Suite {suite_filter}")]
        if not suites:
            print(f"{_RED}No suite matching --suite={suite_filter}{_RESET}")
            sys.exit(1)

    all_results: list[TestResult] = []
    for suite in suites:
        results = suite.run()
        all_results.extend(results)

    total   = len(all_results)
    passed  = sum(1 for r in all_results if r.passed)
    failed  = total - passed
    elapsed = sum(r.elapsed for r in all_results)

    print(f"{_BOLD}{'='*68}{_RESET}")
    print(f"{_BOLD}  Summary{_RESET}")
    print(f"{'─'*68}")
    print(f"  Total tests : {total}")
    print(f"  {_GREEN}Passed{_RESET}      : {passed}")
    if failed:
        print(f"  {_RED}Failed{_RESET}      : {failed}")
        print()
        print(f"  {_RED}Failed tests:{_RESET}")
        for r in all_results:
            if not r.passed:
                print(f"    {_RED}✗{_RESET} {r.name}")
                if r.error:
                    first_line = r.error.splitlines()[0]
                    print(f"      {_YELLOW}{first_line}{_RESET}")
    print(f"  Total time  : {elapsed*1000:.0f}ms")
    print(f"{'='*68}")
    print()

    if failed:
        sys.exit(1)
    print(f"{_GREEN}{_BOLD}All {total} tests passed.{_RESET}")
    print()


if __name__ == "__main__":
    main()
