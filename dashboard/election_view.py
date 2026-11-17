"""Election history: who led, who took over, how long it took."""
from __future__ import annotations

import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import components as C
from dashboard.cluster_view import live
from dashboard.discovery import election_rows, get_config, get_events, get_snapshot
from dashboard.theme import ACCENT, LINE


def render() -> None:
    st.markdown(C.hero("Election history", "Every Bully election: previous coordinator, winner, epoch and duration."),
                unsafe_allow_html=True)
    only_wins = st.toggle("Show only elections that produced a coordinator", value=True)
    live(_body, only_wins=only_wins)


def _body(only_wins: bool) -> None:
    snap = get_snapshot()
    events = get_events(3600)
    rows = election_rows(events)
    wins = [r for r in rows if r["outcome"] == "Won"]
    durations = [r["duration_ms"] for r in wins if r["duration_ms"] is not None]

    cols = st.columns(4)
    cols[0].markdown(C.kpi("Current coordinator", f"Node {snap['coordinator_id']}" if snap["coordinator_id"] else "None",
                           "authoritative" if snap["coordinator_id"] else "election pending"), unsafe_allow_html=True)
    cols[1].markdown(C.kpi("Current epoch", snap["epoch"], f"highest seen {snap['highest_epoch']}", "#0891b2"),
                     unsafe_allow_html=True)
    cols[2].markdown(C.kpi("Elections won (last hour)", len(wins), f"{len(rows)} started in total", "#16a34a"),
                     unsafe_allow_html=True)
    cols[3].markdown(C.kpi("Average duration", f"{sum(durations) / len(durations):.0f} ms" if durations else "-",
                           "start to announcement", "#d97706"), unsafe_allow_html=True)
    st.write("")

    ann = sorted((e for e in events if e["event_type"] == "COORDINATOR_ANNOUNCED"), key=lambda e: e["ts"])
    st.markdown('<div class="dm-section">Coordinator over time</div>', unsafe_allow_html=True)
    if ann:
        x = [pd.to_datetime(e["ts"], unit="s", utc=True).tz_convert(None) for e in ann]
        fig = go.Figure(go.Scatter(x=x, y=[e["node_id"] for e in ann], mode="lines+markers", line=dict(shape="hv",
                                   color=ACCENT, width=3), marker=dict(size=11, color=ACCENT, line=dict(color="white", width=2)),
                                   text=[f"Node {e['node_id']} - epoch {e['epoch']}" for e in ann],
                                   hovertemplate="%{text}<br>%{x|%H:%M:%S}<extra></extra>"))
        fig.update_yaxes(tickmode="array", tickvals=get_config().node_ids, ticktext=[f"Node {i}" for i in get_config().node_ids],
                         gridcolor=LINE, title=None)
        fig.update_xaxes(gridcolor=LINE)
        fig.update_layout(height=250, margin=dict(l=0, r=0, t=6, b=0), paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch", key="coord_history", config={"displayModeBar": False})
    else:
        st.markdown('<div class="dm-note">No elections recorded in the last hour.</div>', unsafe_allow_html=True)

    st.markdown('<div class="dm-section">Elections</div>', unsafe_allow_html=True)
    shown = wins if only_wins else rows
    if not shown:
        st.markdown('<div class="dm-note">Nothing to show yet. Crash the coordinator on the Fault controls page to '
                    'trigger a failover.</div>', unsafe_allow_html=True)
        return
    fmt = lambda t: time.strftime("%H:%M:%S", time.localtime(t)) if t else "-"
    df = pd.DataFrame([{
        "Started": fmt(r["start_ts"]), "Ended": fmt(r["end_ts"]),
        "Previous coordinator": f"Node {r['previous_coordinator']}" if r["previous_coordinator"] else "-",
        "Started by": f"Node {r['candidate']}",
        "New coordinator": f"Node {r['new_coordinator']}" if r["new_coordinator"] else "-",
        "Epoch": r["epoch"], "Duration (ms)": r["duration_ms"], "Outcome": r["outcome"], "Retries": r["retries"],
        "Trigger": r["reason"].replace("_", " "),
    } for r in shown])
    st.dataframe(df, hide_index=True, width="stretch", column_config={
        "Duration (ms)": st.column_config.NumberColumn(format="%.1f")})
    st.caption("Duration is the time from the first ELECTION message to the COORDINATOR announcement. Elections from "
               "different nodes can overlap during a failover; the lower-priority ones end as 'Deferred'.")
