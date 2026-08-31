"""
governance/src/telemetry_bus/_demo.py

End-to-end async demo: Plane 2 (gac_compiler) → EventBus → Plane 4 (recon_operator stub).

Demonstrates the full Plane 2 → 3 → 4 event flow using the in-memory bus:

  [Plane 2]  compile_plan()  →  publish(PolicyCompiledEvent) on POLICY_COMPILED
  [Plane 3]  Spark hook      →  publish(LineageEvent)         on ATLAS_HOOK
  [Plane 3]  discovery scanner →  publish(LineageEvent)         on ATLAS_HOOK
  [Plane 4]  recon_operator  →  subscribe(POLICY_COMPILED)    → process drift check
  [Plane 4]  atlas_consumer  →  subscribe(ATLAS_HOOK)         → process lineage

Run from repo root:
    python src/telemetry_bus/_demo.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from local_proxy import generate_trino_execution_plan
from gac_compiler import compile_plan, write_compilation_result
from contracts import Success
from telemetry_bus import (
    EventBus,
    Topic,
    PolicyCompiledEvent,
    LineageEvent,
    EntityRegisteredEvent,
    DriftDetectedEvent,
    RemediationDoneEvent,
)

# ============================================================================
# Plane 2 Producer — GaC Compiler publishes after compile_plan()
# ============================================================================

async def plane2_publisher(bus: EventBus, *, git_sha: str) -> None:
    """
    Simulates the GitOps CI/CD runner (Plane 2):
    1. Receives a Trino plan (as if triggered by a GitHub webhook).
    2. Compiles the plan via Core Domain.
    3. Writes JSON artefacts via Outbound Port.
    4. Publishes PolicyCompiledEvent to the bus → consumed by Plane 4.
    """
    log.info("[Plane2] Webhook received — compiling plan for git_sha=%s", git_sha[:8])

    sql = "SELECT * FROM prod.pii_table WHERE customer_id = 42"
    plan = generate_trino_execution_plan(sql)

    compile_res = compile_plan(plan, git_sha=git_sha, requesting_principal="ci-cd-runner")
    assert isinstance(compile_res, Success), f"Compilation failed: {compile_res}"
    cr = compile_res.value

    # Write artefacts to outputs/
    import tempfile
    with tempfile.TemporaryDirectory(prefix="gac_demo_") as tmpdir:
        write_res = write_compilation_result(cr, outputs_dir=Path(tmpdir))
        assert isinstance(write_res, Success)
        report = write_res.value
        output_dir = str(report.manifest_path.parent)

    event = PolicyCompiledEvent(
        plan_id=cr.plan_id,
        git_sha=cr.git_sha,
        policy_names=tuple(p.name for p in cr.ranger_policies),
        typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
        entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
        output_dir=output_dir,
        requesting_principal="ci-cd-runner",
    )

    seq = await bus.publish(event)
    log.info(
        "[Plane2] ✓ PolicyCompiledEvent published — seq=%d  policies=%d  typedefs=%d",
        seq,
        len(event.policy_names),
        len(event.typedef_names),
    )


# ============================================================================
# Plane 3 Producers — Spark hook and DiscoveryEngine publish lineage to ATLAS_HOOK
# ============================================================================

async def plane3_spark_hook(bus: EventBus) -> None:
    """
    Simulates the native Spark Atlas hook (fire-and-forget async emit).
    Publishes lineage edge: staging.raw_events → prod.pii_table.
    """
    await asyncio.sleep(0.05)   # hook fires after job completion
    event = LineageEvent(
        source_table="staging.raw_events",
        target_table="prod.pii_table",
        process_name="spark-etl-customer-enrichment",
        source_system="spark-hook",
        job_id="spark-job-00042",
        classification_tags=("PII",),
    )
    seq = await bus.publish(event)
    log.info("[Plane3/Spark] ✓ LineageEvent published — seq=%d  %s → %s",
             seq, event.source_table, event.target_table)


async def plane3_discovery_scanner(bus: EventBus) -> None:
    """
    Simulates the autonomous data discovery scanner.
    Runs independently of Spark/Trino execution — emits on ATLAS_HOOK.
    Also emits an EntityRegisteredEvent after direct Atlas ingest.
    """
    await asyncio.sleep(0.15)   # scanner has its own schedule
    lineage_event = LineageEvent(
        source_table="prod.pii_table",
        target_table="reporting.customer_summary",
        process_name="discovery-scan-2024-01",
        source_system="discovery-engine",
        job_id="scan-00007",
        classification_tags=("PII", "REGULATED"),
    )
    seq = await bus.publish(lineage_event)
    log.info("[Plane3/DiscoveryEngine] ✓ LineageEvent published — seq=%d  %s → %s",
             seq, lineage_event.source_table, lineage_event.target_table)

    await asyncio.sleep(0.05)   # simulate direct Atlas ingest latency
    entity_event = EntityRegisteredEvent(
        qualified_name="reporting.customer_summary@cluster1",
        type_name="TrinoTable_reporting_customer_summary",
        guid="3f2e1d0c-0000-0000-0000-000000000001",
        source_system="discovery-engine",
    )
    seq2 = await bus.publish(entity_event)
    log.info("[Plane3/DiscoveryEngine] ✓ EntityRegisteredEvent published — seq=%d  entity=%s",
             seq2, entity_event.qualified_name)


# ============================================================================
# Plane 4 Consumers — ReconOperator subscribes and processes events
# ============================================================================

async def plane4_policy_consumer(bus: EventBus, *, received: list) -> None:
    """
    Simulates the Reconciliation Operator consuming POLICY_COMPILED events.
    On receipt: runs a (stub) drift check and emits DriftDetectedEvent if needed.
    """
    log.info("[Plane4/Recon] Subscribing to POLICY_COMPILED...")
    async for event in bus.subscribe(Topic.POLICY_COMPILED, subscriber_id="recon-operator", timeout=1.0):
        assert isinstance(event, PolicyCompiledEvent)
        received.append(event)
        log.info("[Plane4/Recon] ← %s", event.summary())

        # Stub drift check: pretend Ranger has an outdated policy version
        drift = DriftDetectedEvent(
            dimension="ranger",
            resource_id=event.policy_names[0] if event.policy_names else "unknown",
            desired_hash="sha256:desired_abc",
            actual_hash="sha256:actual_xyz",
            severity="HIGH",
        )
        drift_seq = await bus.publish(drift)
        log.info("[Plane4/Recon] ↑ DriftDetectedEvent published — seq=%d  dim=%s  severity=%s",
                 drift_seq, drift.dimension, drift.severity)

        # Stub remediation
        await asyncio.sleep(0.02)
        remediation = RemediationDoneEvent(
            dimension=drift.dimension,
            resource_id=drift.resource_id,
            drift_event_id=drift.event_id,
            patch_applied=f"Ranger REST PUT /policies — version bump to {event.git_sha[:8]}",
        )
        rem_seq = await bus.publish(remediation)
        log.info("[Plane4/Recon] ✓ RemediationDoneEvent published — seq=%d  patch='%s'",
                 rem_seq, remediation.patch_applied)


async def plane4_atlas_consumer(bus: EventBus, *, received: list) -> None:
    """
    Simulates the Atlas consumer reading from ATLAS_HOOK topic.
    Processes lineage and entity events independently of Plane 4 recon.
    """
    log.info("[Plane4/Atlas] Subscribing to ATLAS_HOOK + ENTITY_REGISTERED...")
    collected: list = []

    async def read_atlas_hook() -> None:
        async for event in bus.subscribe(Topic.ATLAS_HOOK, subscriber_id="atlas-consumer", timeout=0.8):
            assert isinstance(event, LineageEvent)
            collected.append(event)
            log.info("[Plane4/Atlas] ← %s", event.summary())

    async def read_entity_registered() -> None:
        async for event in bus.subscribe(Topic.ENTITY_REGISTERED, subscriber_id="atlas-entity-consumer", timeout=0.8):
            assert isinstance(event, EntityRegisteredEvent)
            collected.append(event)
            log.info("[Plane4/Atlas] ← %s", event.summary())

    await asyncio.gather(read_atlas_hook(), read_entity_registered())
    received.extend(collected)


# ============================================================================
# Orchestrator — runs all producers and consumers concurrently
# ============================================================================

async def main() -> None:
    log.info("=" * 60)
    log.info("GaC Telemetry Bus Demo — Plane 2 → Plane 3 → Plane 4")
    log.info("=" * 60)

    policy_events_received: list = []
    atlas_events_received: list = []

    async with EventBus() as bus:
        log.info("Bus started: %r", bus)

        # Run producers and consumers concurrently
        # Consumers start first so they are ready to receive immediately
        await asyncio.gather(
            # Plane 4 consumers (start first — they block on queue)
            plane4_policy_consumer(bus, received=policy_events_received),
            plane4_atlas_consumer(bus, received=atlas_events_received),
            # Plane 2 + Plane 3 producers (emit after a brief delay)
            _delayed(0.05, plane2_publisher(bus, git_sha="abc1234def567890")),
            _delayed(0.10, plane3_spark_hook(bus)),
            _delayed(0.20, plane3_discovery_scanner(bus)),
        )

    # -----------------------------------------------------------------------
    # Assertions
    # -----------------------------------------------------------------------
    log.info("")
    log.info("Final bus stats: %s", bus.stats())
    log.info("")

    assert len(policy_events_received) >= 1, \
        f"Plane 4 Recon must receive ≥1 PolicyCompiledEvent, got {len(policy_events_received)}"
    assert all(isinstance(e, PolicyCompiledEvent) for e in policy_events_received)

    assert len(atlas_events_received) >= 2, \
        f"Atlas consumer must receive ≥2 events (LineageEvent + EntityRegisteredEvent), got {len(atlas_events_received)}"

    lineage_events = [e for e in atlas_events_received if isinstance(e, LineageEvent)]
    entity_events  = [e for e in atlas_events_received if isinstance(e, EntityRegisteredEvent)]
    assert len(lineage_events) >= 2,  f"Expected ≥2 LineageEvents, got {len(lineage_events)}"
    assert len(entity_events)  >= 1,  f"Expected ≥1 EntityRegisteredEvent, got {len(entity_events)}"

    # Sequence numbers must be monotonically increasing per topic
    seqs = [e.sequence for e in atlas_events_received if isinstance(e, LineageEvent)]
    assert seqs == sorted(seqs), f"LineageEvent sequences must be ordered: {seqs}"

    log.info("=" * 60)
    log.info("✓ All assertions passed. Demo complete.")
    log.info(
        "  PolicyCompiled received : %d",   len(policy_events_received))
    log.info(
        "  LineageEvents received  : %d",   len(lineage_events))
    log.info(
        "  EntityEvents received   : %d",   len(entity_events))
    log.info("=" * 60)


async def _delayed(seconds: float, coro: object) -> None:
    """Run a coroutine after a delay — simulates async event timing."""
    await asyncio.sleep(seconds)
    await coro  # type: ignore[misc]


if __name__ == "__main__":
    asyncio.run(main())
