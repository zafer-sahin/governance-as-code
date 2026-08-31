"""
governance/src/recon_operator/control_loop.py

Governance Reconciliation Operator — Control Loop (Plane 4).

Responsibility:
  Periodically compare "desired state" (JSON files from Plane 2 / gac_compiler
  outputs, representing what Git says should be true) against "actual state"
  (dict built by StateStore from Plane 3 telemetry events) and:
    1. Print "DRIFT_DETECTED" for every divergence.
    2. Publish a DriftDetectedEvent to the bus.
    3. Apply stub auto-remediation and publish RemediationDoneEvent.

Design:
  - ControlLoop is an async class. Call run() as a background task.
  - tick_interval_s: how often the loop wakes and checks (Kubernetes
    reconcile interval equivalent).
  - The "desired state" is loaded fresh from JSON files every tick
    (simulates the operator polling Git).
  - The "actual state" is read from StateStore.actual_state() — a
    frozen snapshot safe for comparison.
  - Diff is performed by differ.diff() — pure, no I/O.
  - Remediation is a stub: prints intent and emits an event.

Pattern: Control Loop / Declarative State Matching — Plane 4 GaC architecture.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from recon_operator.differ import DiffResult, diff, is_clean
from recon_operator.state_store import StateStore
from telemetry_bus.events import DriftDetectedEvent, RemediationDoneEvent

log = logging.getLogger(__name__)


class ControlLoop:
    """
    Async control loop implementing the Reconciliation Operator (Plane 4).

    Runs every ``tick_interval_s`` seconds, compares desired vs actual state,
    and triggers remediation on drift.

    Parameters
    ----------
    store:
        The StateStore populated by telemetry bus subscriptions.
    bus:
        The EventBus instance (used to publish DriftDetected / RemediationDone).
    desired_state_dir:
        Directory containing Plane 2 JSON outputs (desired state source).
    tick_interval_s:
        Reconciliation interval in seconds. Default: 1 s (demo mode).
    max_ticks:
        Optional limit on loop iterations (used in tests / demos).
    """

    def __init__(
        self,
        store: StateStore,
        bus: Any,
        *,
        desired_state_dir: Path,
        tick_interval_s: float = 1.0,
        max_ticks: int | None = None,
    ) -> None:
        self._store = store
        self._bus = bus
        from pathlib import Path
        self._desired_dir = Path(desired_state_dir) if isinstance(desired_state_dir, str) else desired_state_dir
        self._interval = tick_interval_s
        self._max_ticks = max_ticks
        self._ticks: int = 0
        self._total_drifts: int = 0
        self._total_remediations: int = 0
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the control loop. Blocks until stop() is called or max_ticks reached."""
        log.info(
            "[ControlLoop] Started — interval=%.1fs  desired_dir=%s",
            self._interval, self._desired_dir,
        )
        while not self._stop_event.is_set():
            await self._tick()
            self._ticks += 1
            if self._max_ticks and self._ticks >= self._max_ticks:
                log.info("[ControlLoop] Max ticks (%d) reached — stopping.", self._max_ticks)
                break
            await asyncio.sleep(self._interval)
        log.info(
            "[ControlLoop] Stopped after %d tick(s). "
            "Total drifts=%d  remediations=%d",
            self._ticks, self._total_drifts, self._total_remediations,
        )

    async def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Single reconciliation tick
    # ------------------------------------------------------------------

    async def _tick(self) -> None:
        log.info("[ControlLoop] ── Tick #%d ──────────────────────────────────", self._ticks + 1)

        # 1. Load desired state (Git / Plane 2 outputs)
        desired = self._load_desired_state()
        log.info(
            "[ControlLoop] Desired state: %d ranger policies, %d atlas entities",
            len(desired.get("ranger_policies", {})),
            len(desired.get("atlas_entities", {})),
        )

        # 2. Read actual state (Plane 3 telemetry)
        actual = self._store.actual_state()
        log.info(
            "[ControlLoop] Actual state : %d ranger policies, %d atlas entities, %d lineage edges",
            len(actual.get("ranger_policies", {})),
            len(actual.get("atlas_entities", {})),
            len(actual.get("lineage_edges", [])),
        )

        # 3. Diff each governance dimension
        drift_results: list[DiffResult] = []

        # Ranger policies diff
        ranger_result = diff(
            desired.get("ranger_policies", {}),
            actual.get("ranger_policies", {}),
            resource_id="ranger_policies",
            dimension="ranger",
        )
        drift_results.append(ranger_result)

        # Atlas entities diff
        atlas_result = diff(
            desired.get("atlas_entities", {}),
            actual.get("atlas_entities", {}),
            resource_id="atlas_entities",
            dimension="atlas",
        )
        drift_results.append(atlas_result)

        # 4. Report and remediate
        any_drift = False
        for result in drift_results:
            print(result.report())   # ← explicit DRIFT_DETECTED print
            if result.has_drift:
                any_drift = True
                self._total_drifts += len(result.entries)
                print(f"\n  ╔══════════════════════════════════════════════╗")
                print(f"  ║  DRIFT_DETECTED  dim={result.dimension:<8s}  entries={len(result.entries):<3d} ║")
                print(f"  ╚══════════════════════════════════════════════╝\n")
                await self._publish_drift(result)
                await self._remediate(result)

        if not any_drift:
            print("[ControlLoop] ✓ No drift detected — desired == actual")

    # ------------------------------------------------------------------
    # Desired state loader (reads JSON files from Plane 2 outputs)
    # ------------------------------------------------------------------

    def _load_desired_state(self) -> dict[str, Any]:
        """
        Build desired state dict from Plane 2 JSON output files.

        Reads:
          <desired_dir>/ranger_policies/*.json  → "ranger_policies" key
          <desired_dir>/atlas_entities/*.json   → "atlas_entities"  key

        Returns an empty dict for missing files/directories (idempotent).
        """
        desired: dict[str, Any] = {
            "ranger_policies": {},
            "atlas_entities":  {},
        }

        ranger_dir = self._desired_dir / "ranger_policies"
        if ranger_dir.is_dir():
            for f in sorted(ranger_dir.glob("*.json")):
                try:
                    payload = json.loads(f.read_text(encoding="utf-8"))
                    name = payload.get("name", f.stem)
                    # Normalise to a flat comparison-friendly dict
                    desired["ranger_policies"][name] = {
                        "name":       name,
                        "service":    payload.get("service", ""),
                        "isEnabled":  payload.get("isEnabled", True),
                        "labels":     sorted(payload.get("labels", [])),
                        "git_sha":    payload.get("_gac", {}).get("gitSha", ""),
                        "policy_item_count": (
                            len(payload.get("policyItems", []))
                            + len(payload.get("dataMaskPolicyItems", []))
                        ),
                    }
                except (json.JSONDecodeError, OSError) as exc:
                    log.warning("[ControlLoop] Failed to parse %s: %s", f, exc)

        entity_dir = self._desired_dir / "atlas_entities"
        if entity_dir.is_dir():
            for f in sorted(entity_dir.glob("*.json")):
                try:
                    payload = json.loads(f.read_text(encoding="utf-8"))
                    entity = payload.get("entity", {})
                    attrs  = entity.get("attributes", {})
                    qname  = attrs.get("qualifiedName", f.stem)
                    desired["atlas_entities"][qname] = {
                        "qualified_name": qname,
                        "type_name":      entity.get("typeName", ""),
                        "status":         entity.get("status", "ACTIVE"),
                        "git_sha":        payload.get("_gac", {}).get("gitSha", ""),
                        "classification_count": len(entity.get("classifications", [])),
                    }
                except (json.JSONDecodeError, OSError) as exc:
                    log.warning("[ControlLoop] Failed to parse %s: %s", f, exc)

        return desired

    # ------------------------------------------------------------------
    # Event publishing
    # ------------------------------------------------------------------

    async def _publish_drift(self, result: DiffResult) -> None:
        """Publish a DriftDetectedEvent for this diff result."""
        event = DriftDetectedEvent(
            dimension=result.dimension,
            resource_id=result.resource_id,
            desired_hash=result.desired_hash,
            actual_hash=result.actual_hash,
            severity=result.max_severity,
            verbose_report=result.report(),
        )
        seq = await self._bus.publish(event)
        log.info("[ControlLoop] ↑ DriftDetectedEvent seq=%d  dim=%s  severity=%s",
                 seq, result.dimension, result.max_severity)

    async def _remediate(self, result: DiffResult) -> None:
        """
        Stub auto-remediation: logs intent and emits RemediationDoneEvent.

        A real implementation would call:
          - Ranger REST PUT /policies for ranger dimension
          - Atlas  REST PUT /typedefs | /entities for atlas dimension
          - Model Registry REST PUT /models for model-registry dimension
        """
        patches = []
        for entry in result.entries:
            patch = (
                f"[STUB] Would apply {entry.kind.name} patch at '{entry.path}': "
                f"{entry.desired_val!r} → (desired)"
            )
            patches.append(patch)
            log.info("[ControlLoop] %s", patch)

        summary = f"{len(result.entries)} patch(es) applied to {result.dimension}"
        event = RemediationDoneEvent(
            dimension=result.dimension,
            resource_id=result.resource_id,
            drift_event_id="n/a",  # linked by caller in production
            patch_applied=summary,
        )
        seq = await self._bus.publish(event)
        self._total_remediations += 1
        log.info("[ControlLoop] ✓ RemediationDoneEvent seq=%d  %s", seq, summary)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        return {
            "ticks":          self._ticks,
            "total_drifts":   self._total_drifts,
            "remediations":   self._total_remediations,
        }
