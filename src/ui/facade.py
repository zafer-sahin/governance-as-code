from typing import List, Dict, Any
from ui.daemon import GaCDaemon
from telemetry_bus.events import GovernanceEvent

class DiffViewModel:
    def __init__(self, key: str, kind: str, desired: Any, actual: Any, dimension: str, severity: str):
        self.key = key
        self.kind = kind
        self.desired = str(desired) if desired is not None else ""
        self.actual = str(actual) if actual is not None else ""
        self.dimension = dimension
        self.severity = severity

class GaCUIFacade:
    """
    Facade adhering to Dependency Inversion. 
    The Streamlit UI only knows about this Facade and the ViewModels it returns.
    """
    def __init__(self, desired_state_dir: str):
        self._daemon = GaCDaemon(desired_state_dir)
        
    def start(self):
        self._daemon.start()
        
    def stop(self):
        self._daemon.stop()
        
    def is_running(self) -> bool:
        return self._daemon.is_running()
        
    def get_events(self) -> List[GovernanceEvent]:
        return self._daemon.get_events()
        
    def get_drifts_as_view_models(self) -> List[DiffViewModel]:
        """Reads recent DriftDetectedEvents from the event history and converts them to ViewModels."""
        vms = []
        # We can scan the history backwards to find the latest DriftDetectedEvent per dimension
        events = self._daemon.get_events()
        # Simple extraction: Just grab the latest one for each dimension (ranger, atlas)
        latest_drifts = {}
        for e in events:
            if type(e).__name__ == "DriftDetectedEvent":
                latest_drifts[e.resource_id] = e
            elif type(e).__name__ == "RemediationDoneEvent":
                if e.resource_id in latest_drifts:
                    del latest_drifts[e.resource_id]
                
        for resource_id, e in latest_drifts.items():
            vms.append(DiffViewModel(
                key=e.resource_id,
                kind="Resource Drift",
                desired=f"Hash: {e.desired_hash[:8]}" if e.desired_hash else "None",
                actual=f"Hash: {e.actual_hash[:8]}" if e.actual_hash else "None",
                dimension=e.dimension,
                severity=e.severity
            ))
        return vms
        
    def publish_event(self, event: GovernanceEvent):
        self._daemon.publish_event(event)

    def get_verbose_diff_report(self) -> str:
        """Recalculates the full diff to generate the verbose terminal report for the UI."""
        from recon_operator.differ import diff
        # Read states directly from the background loop
        desired = self._daemon._control_loop._load_desired_state()
        actual = self._daemon._store.actual_state()
        
        reports = []
        
        # 1. Ranger Policies Diff
        d_ranger = desired.get("ranger_policies", {})
        a_ranger = actual.get("ranger_policies", {})
        for name in set(d_ranger.keys()) | set(a_ranger.keys()):
            res = diff(desired, actual, name, "ranger")
            if res.entries:
                reports.append(res.report())
                
        # 2. Atlas Entities Diff
        d_atlas = desired.get("atlas_entities", {})
        a_atlas = actual.get("atlas_entities", {})
        for name in set(d_atlas.keys()) | set(a_atlas.keys()):
            res = diff(desired, actual, name, "atlas")
            if res.entries:
                reports.append(res.report())
                
        return "\n\n".join(reports)

    def simulate_e2e_events(self):
        """Simulates the E2E drift convergence scenario by compiling a plan and publishing events."""
        from local_proxy import generate_trino_execution_plan
        from gac_compiler import compile_plan, write_compilation_result
        from telemetry_bus.events import PolicyCompiledEvent, LineageEvent
        from pathlib import Path
        import time
        
        outputs_dir = Path(self._daemon.desired_state_dir)
        
        # 1. Generate and compile a plan (This updates Desired State in Git/Filesystem)
        plan = generate_trino_execution_plan("SELECT * FROM prod.pii_table WHERE id=1")
        # Assume it's a Success for the simulation
        cr = compile_plan(plan, git_sha="streamlit-e2e-demo").value
        write_compilation_result(cr, outputs_dir=outputs_dir)
        
        # Give Control Loop a chance to tick (1.0s interval) and detect drift 
        # because the actual state (StateStore) is currently empty.
        time.sleep(1.5)
        
        # 2. Publish PolicyCompiledEvent (StateStore will now populate Actual State)
        policy_event = PolicyCompiledEvent(
            plan_id=cr.plan_id, git_sha=cr.git_sha,
            policy_names=tuple(p.name for p in cr.ranger_policies),
            typedef_names=tuple(t.type_name for t in cr.atlas_typedefs),
            entity_names=tuple(e.qualified_name for e in cr.atlas_entities),
            output_dir=str(outputs_dir)
        )
        self.publish_event(policy_event)
        
        # 3. Publish LineageEvent
        lineage_event = LineageEvent(
            source_table="staging.raw", target_table="prod.pii_table",
            process_name="spark-etl", source_system="spark-hook", job_id="ui-sim-1",
            classification_tags=("PII",)
        )
        self.publish_event(lineage_event)

