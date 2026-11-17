"""Service-centric pages: overview, service detail and incidents / SLOs."""
from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import components as C
from dashboard.cluster_view import live
from dashboard.discovery import (get_config, get_controller, get_events, get_sentinel, get_snapshot,
                                 session_availability)
from dashboard.theme import ACCENT, INK, LINE, MUTED, status_colors

SENTINEL_EVENTS = {"SERVICE_STATE_CHANGED", "INCIDENT_OPENED", "INCIDENT_UPDATED", "INCIDENT_RESOLVED",
                   "ALERT_DISPATCHED", "ASSIGNMENT_REBALANCED", "MONITORING_GAP", "SENTINEL_ACTIVATED",
                   "SERVICE_FAULT", "COORDINATOR_CHANGED", "NODE_FAILED"}


def _fmt(ts) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) if ts else "-"


def _no_services() -> bool:
    if get_config().services:
        return False
    st.markdown('<div class="dm-note">No services are configured. Add a <b>services:</b> list to '
                '<b>config/nodes.yaml</b> (see the README) and restart the nodes.</div>', unsafe_allow_html=True)
    return True


def _headline(view: Dict[str, Any], snap: Dict[str, Any]) -> str:
    s = view["summary"]
    if s["DOWN"]:
        svc = f"{s['DOWN']} service{'s' if s['DOWN'] > 1 else ''} down"
        chip = C.pill(svc, "#b91c1c", "#fee2e2")
        return f'<span class="dm-chip" style="color:#b91c1c;background:#fee2e2;border-color:#b91c1c33">' \
               f'<span class="dm-dot dm-pulse" style="background:#b91c1c"></span>{svc}</span>'
    if s["DEGRADED"] or s["NO_DATA"]:
        n = s["DEGRADED"] + s["NO_DATA"]
        return f'<span class="dm-chip" style="color:#b45309;background:#fef3c7;border-color:#b4530933">' \
               f'<span class="dm-dot" style="background:#b45309"></span>{n} service{"s" if n > 1 else ""} degraded</span>'
    return '<span class="dm-chip" style="color:#15803d;background:#dcfce7;border-color:#15803d33">' \
           '<span class="dm-dot" style="background:#15803d"></span>All services operational</span>'


# ============================================================================= overview
def render_overview() -> None:
    live(_overview_body)


def _overview_body() -> None:
    snap, sent = get_snapshot(), get_sentinel()
    view = sent["view"]
    cluster_chip = C.state_chip(snap["state"], f"node {snap['coordinator_id']} coordinating" if snap["coordinator_id"] else "")
    if view is None:
        st.markdown(C.hero("Sentinel", "Highly available service monitoring on a self-electing cluster.", cluster_chip),
                    unsafe_allow_html=True)
        if _no_services():
            return
        if snap["state"] == "unavailable":
            st.markdown('<div class="dm-danger">The monitoring cluster is not running. Start it with '
                        '<b>python scripts/start_cluster.py</b> or the button below.</div>', unsafe_allow_html=True)
            if st.button("Start cluster and demo services", type="primary", key="svc_start"):
                ctrl = get_controller()
                ctrl.start_cluster()
                from fault_injection import process_control
                process_control.start_services(ctrl.cfg)
                st.toast("Starting 5 nodes and the demo services. The first results appear in a few seconds.")
        else:
            st.markdown('<div class="dm-warn">Waiting for the first replicated service state...</div>', unsafe_allow_html=True)
        return

    st.markdown(C.hero("Sentinel", "Highly available service monitoring: probes, incidents and alerts that survive node "
                                   "failures and coordinator failover.",
                       _headline(view, snap) + "&nbsp;&nbsp;" + cluster_chip), unsafe_allow_html=True)
    if sent["stale"]:
        age = time.time() - (view["generated_at"] or time.time())
        st.markdown(f'<div class="dm-warn"><b>No coordinator right now.</b> Showing the last replicated state from node '
                    f'{sent["from_node"]} ({age:.0f} s old). Worker nodes keep probing during the election and buffer '
                    f'their results for the new coordinator.</div>', unsafe_allow_html=True)
        st.write("")

    s = view["summary"]
    n = s["total"]
    cols = st.columns(6)
    cards = [
        ("Services up", f"{s['UP']}/{n}", "passing their checks", "#16a34a"),
        ("Degraded", s["DEGRADED"] + s["NO_DATA"], "slow or no data", "#d97706"),
        ("Down", s["DOWN"], "failed quorum checks", "#dc2626"),
        ("Open incidents", s["open_incidents"], f"{s['resolved_incidents']} resolved", "#7c3aed"),
        ("Availability", f"{s['availability_pct']:.2f}%" if s["availability_pct"] is not None else "-",
         f"average, last {get_config().sentinel.availability_window_h:g} h", ACCENT),
        ("Probe coverage", f"{s['coverage_pct']:.0f}%" if s["coverage_pct"] is not None else "-",
         "services with a live prober", "#0891b2"),
    ]
    for col, (lbl, val, sub, accent) in zip(cols, cards):
        col.markdown(C.kpi(lbl, val, sub, accent), unsafe_allow_html=True)
    st.write("")

    left, right = st.columns([3, 2], gap="large")
    with left:
        st.markdown('<div class="dm-section">Service status</div>', unsafe_allow_html=True)
        order = {"DOWN": 0, "DEGRADED": 1, "NO_DATA": 2, "UNKNOWN": 3, "UP": 4}
        services = sorted(view["services"], key=lambda x: (order.get(x["status"], 9), x["id"]))
        grid = st.columns(2)
        for i, sv in enumerate(services):
            grid[i % 2].markdown(C.service_card(sv), unsafe_allow_html=True)
    with right:
        st.markdown('<div class="dm-section">Active incidents</div>', unsafe_allow_html=True)
        open_inc = [i for i in sent["incidents"] if i["state"] == "open"]
        if not open_inc:
            st.markdown('<div class="dm-note">No open incidents.</div>', unsafe_allow_html=True)
        for inc in open_inc[:4]:
            warn = " warn" if inc["severity"] in ("warning", "minor") else ""
            st.markdown(
                f'<div class="dm-inc{warn} dm-root"><div class="t">{C.esc(inc["service_id"])} - {C.esc(inc["kind"])} '
                f'{C.severity_pill(inc["severity"])}</div><div class="s">{C.esc(inc["incident_id"])} &middot; open '
                f'{C.fmt_duration(time.time() - inc["opened_ts"])} &middot; opened by node {inc["opened_by_node"]} '
                f'(epoch {inc["epoch"]})</div><div class="s">{C.esc(inc.get("reason", ""))}</div></div>',
                unsafe_allow_html=True)
        st.markdown('<div class="dm-section" style="margin-top:.8rem">Recent activity</div>', unsafe_allow_html=True)
        events = [e for e in get_events(900) if e["event_type"] in SENTINEL_EVENTS]
        st.markdown(C.event_feed(events, limit=9), unsafe_allow_html=True)

    st.write("")
    st.markdown('<div class="dm-section">Monitoring cluster</div>', unsafe_allow_html=True)
    c = snap["counts"]
    avail = session_availability()
    m = st.columns(4)
    m[0].markdown(C.kpi("Coordinator", f"Node {snap['coordinator_id']}" if snap["coordinator_id"] else "None",
                        f"epoch {snap['epoch']}", ACCENT), unsafe_allow_html=True)
    m[1].markdown(C.kpi("Nodes healthy", f"{c['HEALTHY']}/{len(snap['nodes'])}",
                        f"{c['FAILED']} failed, {c['SUSPECTED']} suspected", "#16a34a"), unsafe_allow_html=True)
    probing = {n: len(v) for n, v in view["assignments"].items()}
    m[2].markdown(C.kpi("Nodes probing", len([n for n, k in probing.items() if k]),
                        "each service is checked by " + str(get_config().sentinel.replication) + " nodes", "#0891b2"),
                  unsafe_allow_html=True)
    m[3].markdown(C.kpi("Dashboard-observed uptime", f"{avail:.1f}%" if avail is not None else "-",
                        "coordinator reachable", "#d97706"), unsafe_allow_html=True)


# ============================================================================= services
def render_services() -> None:
    st.markdown(C.hero("Services", "Latency, availability and which nodes probe each service."), unsafe_allow_html=True)
    if _no_services():
        return
    live(_services_body)


def _services_body() -> None:
    sent = get_sentinel()
    view = sent["view"]
    if view is None:
        st.markdown('<div class="dm-note">No service data yet.</div>', unsafe_allow_html=True)
        return
    ids = [s["id"] for s in view["services"]]
    sel = st.session_state.get("svc_detail", ids[0])
    sel = st.selectbox("Service", ids, index=ids.index(sel) if sel in ids else 0, key="svc_detail",
                       format_func=lambda i: next(f"{s['name']} ({i})" for s in view["services"] if s["id"] == i))
    sv = next(s for s in view["services"] if s["id"] == sel)
    a, b = st.columns([2, 3], gap="large")
    with a:
        st.markdown(C.service_card(sv), unsafe_allow_html=True)
        st.caption(f"Endpoint: {sv['url']}")
        if sv["description"]:
            st.caption(sv["description"])
    with b:
        st.markdown('<div class="dm-section">Check latency</div>', unsafe_allow_html=True)
        rec = sv["recent"]
        if len(rec) >= 2:
            ok = [r for r in rec if r[1]]
            bad = [r for r in rec if not r[1]]
            to_t = lambda ts: pd.to_datetime(ts, unit="s", utc=True).tz_convert(None)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=[to_t(r[0]) for r in ok], y=[r[2] for r in ok], mode="lines+markers", name="ok",
                                     line=dict(color=ACCENT, width=2), marker=dict(size=6),
                                     text=[f"probed by node {r[3]}" for r in ok],
                                     hovertemplate="%{y:.0f} ms - %{text}<extra></extra>"))
            if bad:
                fig.add_trace(go.Scatter(x=[to_t(r[0]) for r in bad], y=[r[2] for r in bad], mode="markers",
                                         name="failed", marker=dict(size=10, color="#dc2626", symbol="x"),
                                         text=[f"node {r[3]}" for r in bad],
                                         hovertemplate="failed check (%{text})<extra></extra>"))
            fig.add_hline(y=get_config().service(sel).latency_warn_ms, line_dash="dot", line_color="#d97706",
                          annotation_text="slow threshold", annotation_position="top left")
            fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)", yaxis_title="ms", legend=dict(orientation="h", y=1.12))
            fig.update_yaxes(gridcolor=LINE)
            st.plotly_chart(fig, width="stretch", key="svc_latency", config={"displayModeBar": False})
        else:
            st.caption("Waiting for check results.")

    st.markdown('<div class="dm-section">Who probes what</div>', unsafe_allow_html=True)
    nodes = get_config().node_ids
    snap = get_snapshot()
    alive = {n["id"]: n["probe"] == "up" for n in snap["nodes"]}
    rows = []
    for s in view["services"]:
        row = {"Service": s["id"]}
        for n in nodes:
            row[f"Node {n}"] = ("probing" if alive.get(n) else "probing (node down)") if n in s["probers"] else ""
        rows.append(row)
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    st.caption("Each service is checked by two nodes. A failure only counts after consecutive failed checks seen by both "
               "(quorum), so one node with a local network problem cannot raise a false outage. When a node fails, its "
               "checks are re-assigned to healthy nodes.")


# ============================================================================= incidents
def render_incidents() -> None:
    st.markdown(C.hero("Incidents and SLOs", "What broke, who was alerted, and how much error budget is left."),
                unsafe_allow_html=True)
    if _no_services():
        return
    live(_incidents_body)


def _incidents_body() -> None:
    sent = get_sentinel()
    view = sent["view"]
    if view is None:
        st.markdown('<div class="dm-note">No service data yet.</div>', unsafe_allow_html=True)
        return
    incidents: List[Dict[str, Any]] = sent["incidents"]
    s = view["summary"]
    k = st.columns(4)
    k[0].markdown(C.kpi("Open incidents", s["open_incidents"], "right now", "#7c3aed"), unsafe_allow_html=True)
    k[1].markdown(C.kpi("Resolved", s["resolved_incidents"], "in the replicated history", "#16a34a"), unsafe_allow_html=True)
    k[2].markdown(C.kpi("Mean time to resolve", C.fmt_duration(s["mttr_s"]) if s["mttr_s"] else "-", "MTTR", "#0891b2"),
                  unsafe_allow_html=True)
    k[3].markdown(C.kpi("Alerts sent", len(sent["alerts"]), "last 100 kept", "#db2777"), unsafe_allow_html=True)
    st.write("")

    t_inc, t_alert, t_slo = st.tabs(["Incidents", "Alerts", "SLO and error budget"])
    with t_inc:
        open_inc = [i for i in incidents if i["state"] == "open"]
        if open_inc:
            st.markdown('<div class="dm-section">Open</div>', unsafe_allow_html=True)
        for inc in open_inc:
            with st.expander(f"{inc['incident_id']} - {inc['service_id']} ({inc['kind']}, {inc['severity']}) - open "
                             f"{C.fmt_duration(time.time() - inc['opened_ts'])}", expanded=True):
                for t in inc.get("timeline", []):
                    st.markdown(f"`{_fmt(t['ts'])}` **{t['event']}** - {t['text']}")
        st.markdown('<div class="dm-section">History</div>', unsafe_allow_html=True)
        if incidents:
            st.dataframe(pd.DataFrame([{
                "Incident": i["incident_id"], "Service": i["service_id"], "Kind": i["kind"], "Severity": i["severity"],
                "State": i["state"], "Opened": _fmt(i["opened_ts"]), "Resolved": _fmt(i.get("resolved_ts")),
                "Duration": C.fmt_duration((i.get("resolved_ts") or time.time()) - i["opened_ts"]),
                "Opened by": f"Node {i['opened_by_node']}", "Epoch": i["epoch"], "Reason": i.get("reason", "")}
                for i in incidents]), hide_index=True, width="stretch")
        else:
            st.markdown('<div class="dm-note">No incidents yet. Break a service on the <b>Fault controls</b> page to '
                        'create one.</div>', unsafe_allow_html=True)
    with t_alert:
        if sent["alerts"]:
            st.dataframe(pd.DataFrame([{
                "Time": _fmt(a["ts"]), "Event": a["event"], "Severity": a["severity"], "Service": a["service_id"],
                "Message": a["message"], "Sent by": f"Node {a['node_id']}", "Epoch": a.get("epoch"),
                "Channels": ", ".join(a.get("channels", [])), "Idempotency key": a["dedupe_key"]}
                for a in sent["alerts"]]), hide_index=True, width="stretch")
            st.caption("Every alert has an idempotency key (incident id plus event). The key list is replicated to all "
                       "nodes, so a newly elected coordinator does not re-send alerts that were already delivered. "
                       "Configure a webhook in config/nodes.yaml to deliver them to your own systems.")
        else:
            st.markdown('<div class="dm-note">No alerts sent yet.</div>', unsafe_allow_html=True)
    with t_slo:
        st.dataframe(pd.DataFrame([{
            "Service": sv["id"], "Criticality": sv["criticality"], "SLO (%)": sv["slo_target"],
            "Availability (%)": sv["availability_pct"], "Error budget left (%)": sv["error_budget_left_pct"],
            "p95 latency (ms)": sv["p95"], "Status": sv["status"]} for sv in view["services"]]),
            hide_index=True, width="stretch", column_config={
                "Availability (%)": st.column_config.NumberColumn(format="%.3f"),
                "Error budget left (%)": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.0f%%"),
                "p95 latency (ms)": st.column_config.NumberColumn(format="%.0f")})
        st.caption(f"Availability is the share of the last {get_config().sentinel.availability_window_h:g} hours (or of the "
                   "time monitoring has run) in which the service was not DOWN. Error budget is the downtime your SLO "
                   "allows; for example a 99.9% SLO allows about 86 seconds per day.")
