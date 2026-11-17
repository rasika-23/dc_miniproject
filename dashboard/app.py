"""Streamlit entry point:  streamlit run dashboard/app.py"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

st.set_page_config(page_title="Distributed Monitoring Platform", page_icon=":material/hub:", layout="wide",
                   initial_sidebar_state="expanded")

from dashboard import cluster_view, election_view, fault_controls, metrics_view, theme  # noqa: E402
from dashboard.discovery import get_config, get_snapshot  # noqa: E402

theme.inject_css()

pages = [
    st.Page(cluster_view.render_overview, title="Overview", icon=":material/dashboard:", url_path="overview", default=True),
    st.Page(cluster_view.render_nodes, title="Node details", icon=":material/dns:", url_path="nodes"),
    st.Page(election_view.render, title="Election history", icon=":material/how_to_vote:", url_path="elections"),
    st.Page(cluster_view.render_events, title="Event timeline", icon=":material/timeline:", url_path="events"),
    st.Page(fault_controls.render, title="Fault controls", icon=":material/bolt:", url_path="faults"),
    st.Page(metrics_view.render, title="Metrics and reports", icon=":material/monitoring:", url_path="metrics"),
]
nav = st.navigation(pages)

with st.sidebar:
    st.markdown("---")
    st.toggle("Auto-refresh", value=True, key="auto_refresh",
              help="The dashboard only observes the cluster; closing it never affects monitoring or elections.")
    st.select_slider("Refresh every", options=[1, 2, 3, 5, 10], value=2, key="refresh_s",
                     format_func=lambda s: f"{s} s", disabled=not st.session_state.get("auto_refresh", True))
    if st.button("Refresh now", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    snap = get_snapshot()
    label = {"operational": f"Node {snap['coordinator_id']} is coordinator", "unavailable": "Cluster unavailable"}.get(
        snap["state"], "Election in progress")
    st.caption(f"{label} - {len(snap['reachable'])}/{len(snap['nodes'])} nodes reachable")
    st.caption(f"Config: {get_config().source.name if get_config().source else 'default'}")

nav.run()
