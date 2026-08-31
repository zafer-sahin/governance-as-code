import streamlit as st
import pandas as pd
import os
import sys

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ui.facade import GaCUIFacade
from telemetry_bus.events import LineageEvent # Example for playground

# Default simulation dir
OUTPUTS_DIR = "/tmp/gac_outputs"

st.set_page_config(page_title="GaC Control Plane", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for Low Noise Density
st.markdown("""
<style>
    .reportview-container .main .block-container{
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    h1, h2, h3 {
        margin-bottom: 0.2rem;
    }
</style>
""", unsafe_allow_html=True)

def get_facade() -> GaCUIFacade:
    if "facade" not in st.session_state:
        os.makedirs(OUTPUTS_DIR, exist_ok=True)
        facade = GaCUIFacade(OUTPUTS_DIR)
        facade.start()
        st.session_state.facade = facade
    return st.session_state.facade

facade = get_facade()

# UI Layout (Master-Detail)
st.sidebar.title("GaC Control Plane")
st.sidebar.markdown("### System Status")
if facade.is_running():
    st.sidebar.success("● Daemon Active")
else:
    st.sidebar.error("○ Daemon Stopped")

# Refresher
if st.sidebar.button("↻ Refresh State"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### Master Filters")
dimension_filter = st.sidebar.selectbox("Dimension", ["All", "ranger", "atlas"])

# Tabs
tab_dash, tab_flow, tab_playground = st.tabs(["Dashboard & Diffs", "Event Flow (Step-by-Step)", "Playground"])

with tab_dash:
    st.markdown("## Drift Reconciliation Report")
    
    # Render Diffs
    drifts = facade.get_drifts_as_view_models()
    if dimension_filter != "All":
        drifts = [d for d in drifts if d.dimension == dimension_filter]
        
    if not drifts:
        st.info("No drifts detected or waiting for the control loop.")
    else:
        st.metric("Total Divergences", len(drifts))
        
        # Build DataFrame
        df = pd.DataFrame([vars(d) for d in drifts])
        
        # Diff Coloring (Rdiff style)
        def color_diff_row(row):
            kind = row['kind']
            if kind == 'ADDED':
                return ['background-color: rgba(46, 160, 67, 0.15)'] * len(row)
            elif kind == 'REMOVED':
                return ['background-color: rgba(248, 81, 73, 0.15)'] * len(row)
            elif kind == 'MODIFIED':
                return ['background-color: rgba(210, 153, 34, 0.15)'] * len(row)
            return [''] * len(row)
            
        styled_df = df.style.apply(color_diff_row, axis=1)
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
        
        st.markdown("### 📝 Verbose Terminal Output")
        verbose_report = facade.get_verbose_diff_report()
        if verbose_report:
            st.code(verbose_report, language="text")
        else:
            st.info("No active divergence to report.")


with tab_flow:
    st.markdown("## Simulation Flow (Step-by-Step)")
    events = facade.get_events()
    
    if "event_cursor" not in st.session_state:
        st.session_state.event_cursor = 0
        
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Next Event ⏭"):
            if st.session_state.event_cursor < len(events):
                st.session_state.event_cursor += 1
        if st.button("Reset ↺"):
            st.session_state.event_cursor = 0
            
    with col2:
        st.progress(st.session_state.event_cursor / max(1, len(events)))
        st.caption(f"Showing {st.session_state.event_cursor} of {len(events)} events")
        
    import dataclasses
    for i in range(st.session_state.event_cursor):
        event = events[i]
        with st.expander(f"Event {i+1}: {type(event).__name__}", expanded=(i == st.session_state.event_cursor - 1)):
            try:
                st.json(dataclasses.asdict(event))
            except TypeError:
                st.write(str(event))
                
            if getattr(event, "verbose_report", ""):
                st.markdown("**Verbose Drift Report:**")
                st.code(event.verbose_report, language="text")


with tab_playground:
    st.markdown("## Interactive Playground")
    st.caption("Emit raw events or queries directly to the EventBus to simulate triggers.")
    
    with st.form("emit_lineage"):
        st.subheader("Emit Mock Lineage Event")
        src = st.text_input("Source Table", "staging.raw_data")
        tgt = st.text_input("Target Table", "prod.cleaned_data")
        tags = st.text_input("Classification Tags (comma separated)", "PII, Confidential")
        
        if st.form_submit_button("Publish Event 🚀"):
            tag_tuple = tuple(t.strip() for t in tags.split(","))
            event = LineageEvent(
                source_table=src,
                target_table=tgt,
                process_name="streamlit-playground",
                source_system="ui-mock",
                job_id="demo-1",
                classification_tags=tag_tuple
            )
            facade.publish_event(event)
            st.success("Event emitted to bus!")

    st.markdown("---")
    st.subheader("Data Feeder (Simulation Scenario)")
    st.caption("Load data directly from the run_simulation.py E2E scenario.")
    
    if st.button("Run Full E2E Scenario"):
        with st.spinner("Compiling plan and publishing events... Waiting for background loop..."):
            facade.simulate_e2e_events()
            import time
            time.sleep(2.0) # Give ControlLoop 2 ticks (2 seconds) to generate Drift events
        st.success("Simulation done! Refreshing UI...")
        st.rerun()
