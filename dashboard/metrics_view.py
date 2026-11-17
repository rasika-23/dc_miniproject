"""Experiments, charts and CSV reports."""
from __future__ import annotations

import time
from typing import Any, Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import components as C
from dashboard.cluster_view import live
from dashboard.discovery import get_config, get_controller
from dashboard.theme import ACCENT, LINE
from metrics.calculator import summarize
from metrics.experiment_runner import SCENARIOS, run_scenario
from metrics.export_results import runs_csv, summary_csv

METRIC_LABELS = {
    "detection_latency_ms": "Failure detection latency (ms)",
    "failover_time_ms": "Failover time: fault to new coordinator (ms)",
    "election_duration_ms": "Election duration (ms)",
    "recovery_time_ms": "Recovery time (ms)",
    "false_positive_rate_pct": "False-positive rate (%)",
    "monitoring_availability_pct": "Monitoring availability (%)",
    "message_overhead_msgs": "Message overhead (messages)",
    "false_suspicions": "False suspicions",
    "false_failure_declarations": "False failure declarations",
    "failure_declarations": "Failure declarations",
    "elections_started": "Elections started",
    "election_retries": "Election retries",
    "suspicion_latency_ms": "Time to first suspicion (ms)",
    "time_to_coordinator_ms": "Startup to first coordinator (ms)",
    "time_to_stable_ms": "Startup to stable cluster (ms)",
    "reclaim_election_duration_ms": "Reclaim election duration (ms)",
    "checks_passed_pct": "Checks passed (%)",
}


def _label(name: str) -> str:
    return METRIC_LABELS.get(name, name)


def render() -> None:
    st.markdown(C.hero("Metrics and reports", "Run repeatable fault experiments, compare detection and failover times, "
                                              "and export the results as CSV."), unsafe_allow_html=True)
    tab_run, tab_results, tab_live = st.tabs(["Run an experiment", "Results and CSV", "Live message counters"])
    with tab_run:
        _run_tab()
    with tab_results:
        _results_tab()
    with tab_live:
        live(_live_tab)


# ============================================================================= run
def _run_tab() -> None:
    ctrl = get_controller()
    keys = list(SCENARIOS)
    key = st.selectbox("Scenario", keys, format_func=lambda k: SCENARIOS[k].title, key="exp_scenario")
    sc = SCENARIOS[key]
    st.markdown(f'<div class="dm-note">{C.esc(sc.description)}</div>', unsafe_allow_html=True)
    st.write("")
    c1, c2 = st.columns([1, 3])
    reps = c1.number_input("Repetitions", 1, 10, 1, key="exp_reps", help="Averages and ranges are reported over repeats")
    params: Dict[str, Any] = {}
    if sc.params:
        cols = c2.columns(min(3, len(sc.params)))
        for i, (name, (default, help_)) in enumerate(sc.params.items()):
            col = cols[i % len(cols)]
            wkey = f"p_{key}_{name}"
            if isinstance(default, bool):
                params[name] = col.checkbox(help_, value=default, key=wkey)
            elif isinstance(default, int):
                params[name] = int(col.number_input(help_, value=default, step=1, key=wkey))
            else:
                params[name] = float(col.number_input(help_, value=float(default), step=0.5, key=wkey))
    t = get_config().timing
    st.markdown(f'<div class="dm-warn">This kills and restarts real node processes. Each repetition first restores a healthy '
                f'cluster, then restores it again afterwards, so expect roughly {int(40 + t.failure_timeout * 3)}-'
                f'{int(90 + t.failure_timeout * 6)} seconds per run with the current timing settings. Keep this tab open '
                f'until it finishes.</div>', unsafe_allow_html=True)
    st.write("")
    if st.button("Run experiment", type="primary", key="run_exp"):
        bar = st.progress(0.0, text="Starting")
        with st.status(f"Running {sc.title}", expanded=True) as status:
            def cb(msg: str, frac: float) -> None:
                bar.progress(min(max(frac, 0.0), 1.0), text=msg)
                status.write(msg)
            try:
                results = run_scenario(ctrl, key, int(reps), params, cb)
            except Exception as exc:
                status.update(label="Experiment failed", state="error")
                st.error(f"{type(exc).__name__}: {exc}")
                return
            ok = all(r.passed for r in results)
            status.update(label="Finished - all checks passed" if ok else "Finished - some checks failed",
                          state="complete" if ok else "error", expanded=False)
        bar.empty()
        st.session_state["last_experiment"] = results[0].experiment_id
        _show_run_results(results)
        st.info("Results are stored and available on the 'Results and CSV' tab.")


def _show_run_results(results) -> None:
    for r in results:
        label = f"Run {r.run_index + 1}: {'passed' if r.passed else 'needs attention'}"
        with st.expander(label, expanded=not r.passed):
            if r.error:
                st.error(r.error)
            for name, ok in r.checks.items():
                st.markdown(f"{':white_check_mark:' if ok else ':x:'} {name}")
            for note in r.notes:
                st.caption(note)
    names = [n for n in sorted({n for r in results for n in r.metrics}) if n != "checks_passed_pct"]
    rows = []
    for n in names:
        s = summarize(r.metrics[n]["value"] for r in results if n in r.metrics)
        if s["n"]:
            rows.append({"Metric": _label(n), "Average": s["mean"], "Min": s["min"], "Max": s["max"], "Runs": s["n"]})
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch",
                     column_config={c: st.column_config.NumberColumn(format="%.1f") for c in ("Average", "Min", "Max")})


# ============================================================================= results
def _results_tab() -> None:
    ctrl = get_controller()
    exps = ctrl.repo.experiments()
    if not exps:
        st.markdown('<div class="dm-note">No experiment results yet. Run a scenario on the first tab.</div>',
                    unsafe_allow_html=True)
        return
    ids = [e["experiment_id"] for e in exps]
    default = st.session_state.get("last_experiment")
    sel = st.selectbox("Experiment", ids, index=ids.index(default) if default in ids else 0, key="res_exp",
                       format_func=lambda i: next(
                           f"{SCENARIOS[e['scenario']].title if e['scenario'] in SCENARIOS else e['scenario']} - "
                           f"{time.strftime('%d %b %H:%M', time.localtime(e['started_ts']))} - {e['runs']} run(s)"
                           for e in exps if e["experiment_id"] == i))
    rows = ctrl.repo.metric_results(experiment_id=sel)
    all_rows = ctrl.repo.metric_results()

    names = sorted({r["metric_name"] for r in rows} - {"checks_passed_pct"})
    summ = []
    for n in names:
        vals = [r["value"] for r in rows if r["metric_name"] == n]
        s = summarize(vals)
        unit = next(r["unit"] for r in rows if r["metric_name"] == n)
        summ.append({"Metric": _label(n), "Unit": unit, "Runs": s["n"], "Average": s["mean"], "Min": s["min"],
                     "Max": s["max"]})
    st.markdown('<div class="dm-section">Summary across repeated runs</div>', unsafe_allow_html=True)
    st.dataframe(pd.DataFrame(summ), hide_index=True, width="stretch",
                 column_config={c: st.column_config.NumberColumn(format="%.1f") for c in ("Average", "Min", "Max")})

    # charts
    chartable = [n for n in names if any(r["metric_name"] == n and r["value"] is not None for r in rows)]
    if chartable:
        st.markdown('<div class="dm-section">Per-run values</div>', unsafe_allow_html=True)
        metric = st.selectbox("Metric", chartable, format_func=_label, key="res_metric")
        pts = sorted(((r["run_index"] + 1, r["value"]) for r in rows if r["metric_name"] == metric and r["value"] is not None))
        avg = sum(v for _, v in pts) / len(pts)
        fig = go.Figure(go.Bar(x=[f"Run {i}" for i, _ in pts], y=[v for _, v in pts], marker_color=ACCENT,
                               text=[f"{v:.0f}" if abs(v) >= 10 else f"{v:.1f}" for _, v in pts], textposition="outside"))
        fig.add_hline(y=avg, line_dash="dot", line_color="#0891b2", annotation_text=f"average {avg:.1f}",
                      annotation_position="top left")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0), paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)", yaxis_title=_label(metric))
        fig.update_yaxes(gridcolor=LINE)
        st.plotly_chart(fig, width="stretch", key="res_chart", config={"displayModeBar": False})

    # checks
    with st.expander("Checks and notes per run"):
        for r in [x for x in rows if x["metric_name"] == "checks_passed_pct"]:
            d = r.get("details") or {}
            st.markdown(f"**Run {r['run_index'] + 1}**" + (f" - error: {d['error']}" if d.get("error") else ""))
            for name, ok in (d.get("checks") or {}).items():
                st.markdown(f"{':white_check_mark:' if ok else ':x:'} {name}")
            for note in d.get("notes") or []:
                st.caption(note)
            for pf in d.get("per_fault") or []:
                st.caption("Fault on node {target}: detection {det} ms{fo}".format(
                    target=pf["target"], det=pf.get("detection_latency_ms"),
                    fo=f", failover {pf['failover_time_ms']} ms, new coordinator node {pf['new_coordinator']}"
                    if pf.get("failover_time_ms") is not None else ""))

    # comparison across everything stored
    cmp_metric = "detection_latency_ms"
    by_scn: Dict[str, List[float]] = {}
    for r in all_rows:
        if r["metric_name"] == cmp_metric and r["value"] is not None:
            by_scn.setdefault(r["scenario"], []).append(r["value"])
    if len(by_scn) > 1:
        st.markdown('<div class="dm-section">Detection latency by scenario (all stored runs)</div>', unsafe_allow_html=True)
        fig = go.Figure(go.Bar(x=[SCENARIOS[k].title if k in SCENARIOS else k for k in by_scn],
                               y=[sum(v) / len(v) for v in by_scn.values()], marker_color="#0891b2"))
        fig.update_layout(height=260, margin=dict(l=0, r=0, t=6, b=0), paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)", yaxis_title="ms")
        fig.update_yaxes(gridcolor=LINE)
        st.plotly_chart(fig, width="stretch", key="cmp_chart", config={"displayModeBar": False})

    st.markdown('<div class="dm-section">Download</div>', unsafe_allow_html=True)
    d1, d2, d3 = st.columns(3)
    d1.download_button("Runs CSV (this experiment)", runs_csv(rows), file_name=f"{sel}_runs.csv", mime="text/csv",
                       width="stretch")
    d2.download_button("Summary CSV (this experiment)", summary_csv(rows), file_name=f"{sel}_summary.csv",
                       mime="text/csv", width="stretch")
    d3.download_button("All experiments (summary CSV)", summary_csv(all_rows), file_name="all_experiments_summary.csv",
                       mime="text/csv", width="stretch")


# ============================================================================= live counters
def _live_tab() -> None:
    ctrl = get_controller()
    data = ctrl.counters()
    if not data:
        st.markdown('<div class="dm-note">No node is reachable.</div>', unsafe_allow_html=True)
        return
    types = sorted({t for m in data.values() for t in m["counters"]["sent"]})
    fig = go.Figure()
    palette = ["#4f46e5", "#0891b2", "#16a34a", "#d97706", "#dc2626", "#9333ea", "#64748b"]
    for i, t in enumerate(types):
        fig.add_trace(go.Bar(name=t, x=[f"Node {n}" for n in data], y=[data[n]["counters"]["sent"].get(t, 0) for n in data],
                             marker_color=palette[i % len(palette)]))
    fig.update_layout(barmode="stack", height=320, margin=dict(l=0, r=0, t=6, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", legend=dict(orientation="h", y=1.15), yaxis_title="messages sent")
    fig.update_yaxes(gridcolor=LINE)
    st.plotly_chart(fig, width="stretch", key="counters", config={"displayModeBar": False})
    df = pd.DataFrame([{"Node": f"Node {n}", "Role": m["role"].capitalize(), "Sent": m["counters"]["sent_total"],
                        "Received": m["counters"]["received_total"], "Dropped (injected)": m["counters"]["dropped"],
                        "Delayed (injected)": m["counters"]["delayed"], "Unreachable peer": m["counters"]["failed"],
                        "Elections started": m["elections_started"]} for n, m in data.items()])
    st.dataframe(df, hide_index=True, width="stretch")
    st.caption("Counters belong to the current process incarnation of each node and restart from zero when a node restarts.")
