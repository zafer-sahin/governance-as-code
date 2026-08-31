"""
governance/src/recon_operator/_demo.py

Full 4-Plane integration demo:
  Plane 1 (local_proxy)    →  generate_trino_execution_plan()
  Plane 2 (gac_compiler)   →  compile_plan() + write_compilation_result()
  Plane 3 (telemetry_bus)  →  EventBus + LineageEvent + EntityRegisteredEvent
  Plane 4 (recon_operator) →  StateStore + ControlLoop → DRIFT_DETECTED

Scenario
--------
  1. Plane 2 compiles a PII SQL plan and writes JSON artefacts (desired state).
  2. Plane 3 publishes PolicyCompiledEvent + LineageEvents to the bus.
  3. StateStore builds actual state from those events.
  4. ControlLoop runs TWO ticks:
       Tick 1: actual state is INCOMPLETE → DRIFT_DETECTED (policies missing
               from actual because StateStore hasn't received them yet).
       Tick 2: actual state is POPULATED  → no drift (or minimal delta).

Run from repo root:
    python src/recon_operator/_demo.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from contracts import Success
from gac_compiler import compile_plan, write_compilation_result
from local_proxy import generate_trino_execution_plan
from recon_operator import ControlLoop, StateStore
from telemetry_bus import (
    EntityRegisteredEvent,
    EventBus,
    LineageEvent,
    PolicyCompiledEvent,
    Topic,
)


# ============================================================================
# Helper: Plane 2 — compile + write desired state
# ============================================================================

def build_desired_state(outputs_dir: Path, git_sha: str) -> None:
    """Compile a PII SQL plan and write JSON artefacts to outputs_dir."""
    sql = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
    plan = generate_trino_execution_plan(sql)
    cr_res = compile_plan(plan, git_sha=git_sha, requesting_principal="ci-cd-runner")
    assert isinstance(cr_res, Success), f"Compilation failed: {cr_res}"
    wr_res = write_compilation_result(cr_res.value, outputs_dir=outputs_dir)
    assert isinstance(wr_res, Success), f"Write failed: {wr_res}"
    report = wr_res.value
    log.info("[Plane2] Desired state written: %d policies, %d entities",
             len(report.ranger_paths), len(report.entity_paths))


# ============================================================================
# Helper: Plane 3 — publish telemetry events
# ============================================================================

async def emit_telemetry(bus: EventBus, git_sha: str, cr_res: object) -> None:
    """Simulate Spark hook, discovery scanner, and Plane 2 PolicyCompiledEvent."""
    from contracts import Success as Suc
    assert isinstance(cr_res, Suc)
    cr = cr_res.value

    # Plane 2 emits PolicyCompiledEvent after provisioning
    pc_event = PolicyCompiledEvent(
        plan_id=cr.plan_id,
        git_sha=cr.git_sha,
        policy_names=tuple(p.name for p in cr.ranger_policies),
        typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
        entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
        output_dir="(in-memory)",
        requesting_principal="ci-cd-runner",
    )
    await bus.publish(pc_event)
    log.info("[Plane2→Bus] PolicyCompiledEvent published — %d policies", len(pc_event.policy_names))

    # Plane 3 Spark hook
    await asyncio.sleep(0.05)
    le = LineageEvent(
        source_table="staging.raw_events",
        target_table="prod.pii_table",
        process_name="spark-etl-customer-enrichment",
        source_system="spark-hook",
        job_id="spark-job-00042",
        classification_tags=("PII",),
    )
    await bus.publish(le)
    log.info("[Plane3/Spark] LineageEvent published: %s → %s", le.source_table, le.target_table)

    # Plane 3 DiscoveryEngine direct entity registration
    await asyncio.sleep(0.05)
    ee = EntityRegisteredEvent(
        qualified_name="prod.pii_table@cluster1",
        type_name="TrinoTable_prod_pii_table",
        guid="deadbeef-0000-0000-0000-000000000001",
        source_system="discovery-engine",
    )
    await bus.publish(ee)
    log.info("[Plane3/DiscoveryEngine] EntityRegisteredEvent published: %s", ee.qualified_name)


# ============================================================================
# Main orchestrator
# ============================================================================

async def main() -> None:
    print()
    print("=" * 68)
    print("  GaC Control Loop Demo — All 4 Planes")
    print("=" * 68)
    print()

    git_sha = "abc1234def567890"

    # ------------------------------------------------------------------
    # Step 1: Plane 2 — write desired state to a temp directory
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix="gac_recon_") as tmpdir:
        outputs_dir = Path(tmpdir)
        build_desired_state(outputs_dir, git_sha)

        # Recompile to get CompilationResult (for event metadata)
        sql = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
        plan = generate_trino_execution_plan(sql)
        cr_res = compile_plan(plan, git_sha=git_sha)

        async with EventBus() as bus:

            # ------------------------------------------------------------------
            # Step 2: Plane 4 — StateStore starts listening
            # ------------------------------------------------------------------
            store = StateStore(bus)
            await store.start()

            # ------------------------------------------------------------------
            # Step 3: Control loop — 2 ticks
            # ------------------------------------------------------------------
            loop = ControlLoop(
                store,
                bus,
                desired_state_dir=outputs_dir,
                tick_interval_s=0.4,
                max_ticks=2,
            )

            print()
            print("─" * 68)
            print("  TICK 1: Actual state EMPTY → drift expected")
            print("─" * 68)
            print()

            async def run_loop_and_emit() -> None:
                """Run control loop; emit telemetry AFTER tick 1 so tick 2 sees it."""
                loop_task = asyncio.create_task(loop.run(), name="control-loop")

                # Wait for tick 1 to complete (≈ tick_interval_s)
                await asyncio.sleep(0.5)

                print()
                print("─" * 68)
                print("  Emitting Plane 3 telemetry — StateStore will update…")
                print("─" * 68)
                print()
                await emit_telemetry(bus, git_sha, cr_res)

                # Brief pause so StateStore can process events before tick 2
                await asyncio.sleep(0.3)

                print()
                print("─" * 68)
                print("  TICK 2: Actual state POPULATED → minimal/no drift")
                print("─" * 68)
                print()

                await loop_task

            await run_loop_and_emit()
            await store.stop()

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        print()
        print("=" * 68)
        stats = loop.stats()
        print(f"  Control Loop Stats:")
        print(f"    Ticks       : {stats['ticks']}")
        print(f"    Drifts      : {stats['total_drifts']}")
        print(f"    Remediations: {stats['remediations']}")
        print("=" * 68)
        print()

        # Basic assertions
        assert stats["ticks"] == 2
        assert stats["total_drifts"] > 0,  "Tick 1 must detect drift (empty actual state)"
        assert stats["remediations"] > 0,  "Tick 1 must trigger remediation"
        print("✓ All assertions passed.")


if __name__ == "__main__":
    asyncio.run(main())
