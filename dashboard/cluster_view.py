"""Overview dashboard, node details and the event timeline."""
from __future__ import annotations

import math
import time
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import components as C
from dashboard.discovery import get_config, get_controller, get_events, get_snapshot, session_availability
from dashboard.theme import ACCENT, CATEGORY_COLORS, INK, LINE, MUTED, status_colors


# ============================================================================= helpers
def live(render, **kwargs) -> None:
    """Run `render` as a fragment that refreshes itself (interval set in the sidebar)."""
    every = f"{st.session_state.get('refresh_s', 2)}s" if st.session_state.get("auto_refresh", True) else None
    st.fragment(run_every=every)(render)(**kwargs)


def topology_figure(snap: Dict[str, Any]) -> go.Figure:
    nodes = snap["nodes"]
    n = len(nodes)
    pos = {nd["id"]: (math.sin(2 * math.pi * i / n), math.cos(2 * math.pi * i / n)) for i, nd in enumerate(nodes)}
    fig = go.Figure()
    # peer-to-peer mesh (every node can talk to every other node)
    for i, a in enumerate(nodes):
        for b in nodes[i + 1:]:
            fig.add_trace(go.Scatter(x=[pos[a["id"]][0], pos[b["id"]][0]], y=[pos[a["id"]][1], pos[b["id"]][1]],
                                     mode="lines", line=dict(color="#e2e8f0", width=1), hoverinfo="skip",
                                     showlegend=False))
    coord = snap["coordinator_id"]
    if coord is not None:
        for nd in nodes:
            if nd["id"] == coord:
                continue
            fg, _ = status_colors(nd["display_state"])
            dead = nd["display_state"] in ("FAILED", "UNREACHABLE")
            fig.add_trace(go.Scatter(
                x=[pos[coord][0], pos[nd["id"]][0]], y=[pos[coord][1], pos[nd["id"]][1]], mode="lines",
                line=dict(color=fg, width=2.5 if not dead else 2, dash="dot" if dead else "solid"),
                hoverinfo="skip", showlegend=False))
        cx, cy = pos[coord]
        fig.add_trace(go.Scatter(x=[cx], y=[cy], mode="markers", hoverinfo="skip", showlegend=False,
                                 marker=dict(size=78, color="rgba(0,0,0,0)", line=dict(color=ACCENT, width=3))))
    xs, ys, colors, texts, hovers, labels = [], [], [], [], [], []
    for nd in nodes:
        fg, _ = status_colors(nd["display_state"])
        xs.append(pos[nd["id"]][0]); ys.append(pos[nd["id"]][1]); colors.append(fg)
        texts.append(f"N{nd['id']}")
        role = "Coordinator" if nd["id"] == coord else nd["role"].capitalize()
        hovers.append(f"<b>Node {nd['id']}</b><br>{role} - {nd['display_state'].capitalize()}<br>"
                      f"Priority {nd['priority']} - port {nd['port']}<br>"
                      f"Response {C.fmt_ms(nd['response_time_ms'])}")
    fig.add_trace(go.Scatter(x=xs, y=ys, mode="markers+text", text=texts, textposition="middle center",
                             textfont=dict(color="white", size=15, family="Plus Jakarta Sans"),
                             marker=dict(size=56, color=colors, line=dict(color="white", width=3)),
                             hovertext=hovers, hoverinfo="text", showlegend=False))
    for nd in nodes:
        x, y = pos[nd["id"]]
        label = "coordinator" if nd["id"] == coord else f"priority {nd['priority']}"
        fig.add_annotation(x=x, y=y - 0.2 if y < 0.9 else y + 0.2, text=label, showarrow=False,
                           font=dict(size=11, color=ACCENT if nd["id"] == coord else MUTED))
    fig.update_xaxes(visible=False, range=[-1.45, 1.45])
    fig.update_yaxes(visible=False, range=[-1.4, 1.35], scaleanchor="x")
    fig.update_layout(margin=dict(l=0, r=0, t=6, b=0), height=395, paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", hovermode="closest")
    return fig


def _state_detail(snap: Dict[str, Any]) -> str:
    if snap["state"] == "operational":
        return f"Node {snap['coordinator_id']} coordinating, epoch {snap['epoch']}"
    if snap["state"] == "split_brain":
        return f"nodes {[snap['coordinator_id']] + snap['conflicting']} claim leadership"
    if snap["state"] == "election_in_progress" and snap["electing"]:
        return f"nodes {snap['electing']} electing"
    return ""


# ============================================================================= overview
def render_overview() -> None:
    live(_overview_body)


def _overview_body() -> None:
    cfg = get_config()
    snap = get_snapshot()
    events = get_events(600)
    st.markdown(C.hero("Distributed monitoring platform",
                       "Live heartbeat monitoring, failure detection and Bully-election failover across five peer nodes.",
                       C.state_chip(snap["state"], _state_detail(snap))), unsafe_allow_html=True)

    if snap["state"] == "unavailable":
        st.markdown('<div class="dm-danger">No node answers on its configured address. Start the cluster with '
                    '<b>python scripts/start_cluster.py</b> or use the button below.</div>', unsafe_allow_html=True)
        if st.button("Start all nodes", type="primary", key="ov_start"):
            get_controller().start_cluster()
            st.toast("Starting 5 node processes - they elect a coordinator in a few seconds.")
        st.write("")
    elif snap["state"] in ("election_in_progress", "no_coordinator"):
        st.markdown('<div class="dm-warn">The cluster has no coordinator at the moment. The remaining nodes are running '
                    'a Bully election; this page keeps polling every node and will update as soon as one wins.</div>',
                    unsafe_allow_html=True)
        st.write("")
    elif snap["state"] == "split_brain":
        st.markdown('<div class="dm-danger">More than one node claims to be coordinator. Epochs and priorities should '
                    'resolve this within a few heartbeats; persistent conflicts are a known limit of timeout-based '
                    'detection (see README).</div>', unsafe_allow_html=True)
        st.write("")

    c = snap["counts"]
    avail = session_availability()
    cols = st.columns(6)
    cards = [
        ("Healthy nodes", c["HEALTHY"], f"of {len(snap['nodes'])} configured", "#16a34a"),
        ("Suspected", c["SUSPECTED"], "missed heartbeats", "#d97706"),
        ("Recovering", c["RECOVERING"], "validating after restart", "#0284c7"),
        ("Failed nodes", c["FAILED"], "declared failed", "#dc2626"),
        ("Coordinator", f"Node {snap['coordinator_id']}" if snap["coordinator_id"] else "None",
         f"epoch {snap['epoch']}" if snap["coordinator_id"] else "election pending", ACCENT),
        ("Monitoring uptime", f"{avail:.1f}%" if avail is not None else "-", "observed in this session", "#0891b2"),
    ]
    for col, (lbl, val, sub, accent) in zip(cols, cards):
        col.markdown(C.kpi(lbl, val, sub, accent), unsafe_allow_html=True)
    st.write("")

    left, right = st.columns([3, 2], gap="large")
    with left:
        st.markdown('<div class="dm-section">Cluster topology</div>', unsafe_allow_html=True)
        with st.container(border=True):
            st.plotly_chart(topology_figure(snap), width="stretch", key="topology",
                            config={"displayModeBar": False})
        st.caption("Grey lines: peer-to-peer links. Coloured lines: coordinator to worker, coloured by the "
                   "coordinator's view of that worker. Ring: current coordinator.")
    with right:
        st.markdown('<div class="dm-section">Recent events</div>', unsafe_allow_html=True)
        st.markdown(C.event_feed(events, limit=14), unsafe_allow_html=True)

    st.write("")
    st.markdown('<div class="dm-section">Nodes</div>', unsafe_allow_html=True)
    cols = st.columns(len(snap["nodes"]))
    for col, n in zip(cols, snap["nodes"]):
        col.markdown(C.node_card(n), unsafe_allow_html=True)

    t = cfg.timing
    st.write("")
    st.caption(f"Detection settings: heartbeat every {t.heartbeat_interval:g}s, suspected after {t.suspicion_timeout:g}s, "
               f"failed after {t.failure_timeout:g}s, coordinator timeout {t.coordinator_timeout:g}s. "
               f"Status source: {'coordinator membership view' if snap['source'] == 'coordinator' else 'direct probes'}.")


# ============================================================================= node details
def render_nodes() -> None:
    st.markdown(C.hero("Node details", "Role, health, heartbeat and failure history for every node."),
                unsafe_allow_html=True)
    live(_nodes_body)


def _nodes_body() -> None:
    snap = get_snapshot()
    events = get_events(3600)
    df = pd.DataFrame([{
        "Node": f"Node {n['id']}", "Role": n["role"].capitalize(), "Status": n["display_state"].capitalize(),
        "Priority": n["priority"], "Port": n["port"],
        "Last heartbeat (s)": n["last_heartbeat_age_s"], "Response (ms)": n["response_time_ms"],
        "Uptime (s)": n["uptime_s"], "Failures": n["failure_count"],
        "Follows": f"Node {n['follows']}" if n["follows"] else "-",
        "Process": ("running" if n["process"]["running"] else "stopped") if n["process"]["pid"] else "unknown",
    } for n in snap["nodes"]])
    st.dataframe(df, hide_index=True, width="stretch", column_config={
        "Last heartbeat (s)": st.column_config.NumberColumn(format="%.2f"),
        "Response (ms)": st.column_config.NumberColumn(format="%.1f"),
        "Uptime (s)": st.column_config.NumberColumn(format="%.0f")})

    ids = [n["id"] for n in snap["nodes"]]
    sel = st.session_state.get("node_detail", ids[-1])
    sel = st.selectbox("Inspect node", ids, index=ids.index(sel) if sel in ids else 0, key="node_detail",
                       format_func=lambda i: f"Node {i}")
    node = next(n for n in snap["nodes"] if n["id"] == sel)
    a, b = st.columns([2, 3], gap="large")
    with a:
        st.markdown(C.node_card(node), unsafe_allow_html=True)
        proc = node["process"]
        st.caption(f"Process id: {proc['pid'] or 'unknown'} - incarnation: {node['incarnation'] or '-'}")
    with b:
        st.markdown('<div class="dm-section">Response time history (this session)</div>', unsafe_allow_html=True)
        hist = [(h["ts"], h["nodes"][sel]["rt"]) for h in st.session_state.get("history", [])
                if sel in h["nodes"] and h["nodes"][sel]["rt"] is not None]
        if len(hist) >= 2:
            fig = go.Figure(go.Scatter(x=[pd.to_datetime(t, unit="s", utc=True).tz_convert(None) for t, _ in hist],
                                       y=[v for _, v in hist], mode="lines", line=dict(color=ACCENT, width=2.5),
                                       fill="tozeroy", fillcolor="rgba(79,70,229,0.08)"))
            fig.update_layout(height=220, margin=dict(l=0, r=0, t=6, b=0), yaxis_title="ms",
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            fig.update_yaxes(gridcolor=LINE)
            st.plotly_chart(fig, width="stretch", key="rt_hist", config={"displayModeBar": False})
        else:
            st.caption("Waiting for coordinator measurements. Response time is the heartbeat transit time reported "
                       "by the coordinator.")

    st.markdown('<div class="dm-section">Failure history</div>', unsafe_allow_html=True)
    types = {"NODE_SUSPECTED", "NODE_FAILED", "NODE_RECOVERING", "NODE_HEALTHY", "SUSPICION_CLEARED",
             "COORDINATOR_TIMEOUT", "CRASH_INJECTED", "RESTART_REQUESTED"}
    rows = [{"Time": time.strftime("%H:%M:%S", time.localtime(e["ts"])), "Event": C.describe_event(e)[1],
             "Seen by": "Controller" if e["node_id"] == 0 else f"Node {e['node_id']}"}
            for e in events if e["event_type"] in types and e.get("subject_node_id") == sel]
    if rows:
        st.dataframe(pd.DataFrame(rows[:60]), hide_index=True, width="stretch")
    else:
        st.markdown('<div class="dm-note">No suspicion, failure or recovery events recorded for this node in the '
                    'last hour.</div>', unsafe_allow_html=True)


# ============================================================================= event timeline
def render_events() -> None:
    st.markdown(C.hero("Event timeline", "Heartbeats, elections, failures and recoveries from every node's local journal, "
                                          "merged and de-duplicated by event id."), unsafe_allow_html=True)
    f1, f2, f3, f4 = st.columns([2, 2, 2, 1])
    window = f1.select_slider("Time window", [1, 5, 15, 30, 60, 240], value=15, format_func=lambda m: f"{m} min")
    cats = f2.multiselect("Categories", list(CATEGORY_COLORS), default=[c for c in CATEGORY_COLORS if c != "heartbeat"])
    nodes = f3.multiselect("Logged by", ["Controller"] + [f"Node {i}" for i in get_config().node_ids], default=[])
    search = f4.text_input("Search", "")
    live(_events_body, window=window, cats=cats, nodes=nodes, search=search)


def _events_body(window: int, cats: List[str], nodes: List[str], search: str) -> None:
    events = get_events(window * 60)
    rows = []
    for e in events:
        cat, text = C.describe_event(e)
        who = "Controller" if e["node_id"] == 0 else f"Node {e['node_id']}"
        if cat not in cats or (nodes and who not in nodes) or (search and search.lower() not in text.lower()):
            continue
        rows.append({"ts": e["ts"], "time": pd.to_datetime(e["ts"], unit="s", utc=True).tz_convert(None),
                     "who": who, "category": cat, "type": e["event_type"], "text": text, "epoch": e.get("epoch", 0)})
    if not rows:
        st.markdown('<div class="dm-note">No events match the filters. Heartbeat events are sampled and hidden by '
                    'default; tick the "heartbeat" category to see them.</div>', unsafe_allow_html=True)
        return
    df = pd.DataFrame(rows)
    fig = go.Figure()
    order = ["Controller"] + [f"Node {i}" for i in get_config().node_ids]
    for cat, grp in df.groupby("category"):
        fig.add_trace(go.Scatter(x=grp["time"], y=grp["who"], mode="markers", name=cat,
                                 marker=dict(size=11, color=CATEGORY_COLORS.get(cat, MUTED),
                                             line=dict(color="white", width=1.5)),
                                 text=grp["text"], hovertemplate="%{text}<br>%{x|%H:%M:%S}<extra></extra>"))
    fig.update_yaxes(categoryorder="array", categoryarray=order[::-1], gridcolor=LINE)
    fig.update_xaxes(gridcolor=LINE)
    fig.update_layout(height=330, margin=dict(l=0, r=0, t=6, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", legend=dict(orientation="h", y=1.12), hovermode="closest")
    st.plotly_chart(fig, width="stretch", key="timeline", config={"displayModeBar": False})
    table = df.assign(Time=df["ts"].map(lambda t: time.strftime("%H:%M:%S", time.localtime(t)) + f".{int(t % 1 * 1000):03d}"))
    st.dataframe(table[["Time", "who", "category", "text", "epoch"]].rename(
        columns={"who": "Logged by", "category": "Category", "text": "Event", "epoch": "Epoch"}).head(500),
        hide_index=True, width="stretch", height=360)
    st.caption(f"{len(df)} events shown. Each node keeps its own SQLite journal; the dashboard merges the journals of "
               "reachable nodes, so a crashed node's recent history appears again once it restarts.")
