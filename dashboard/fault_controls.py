"""Fault injection controls: crash, delay, message loss and recovery."""
from __future__ import annotations

import time

import pandas as pd
import streamlit as st

from dashboard import components as C
from dashboard.cluster_view import live
from dashboard.discovery import get_config, get_controller, get_snapshot


def _clock(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int(ts % 1 * 1000):03d}"


def render() -> None:
    ctrl, cfg = get_controller(), get_config()
    st.markdown(C.hero("Fault controls", "Break the cluster on purpose and watch how it reacts. Controls work on this "
                                         "machine only: crashes are real process kills."), unsafe_allow_html=True)

    live(_status_strip)
    st.write("")

    # ------------------------------------------------------------------ quick actions
    st.markdown('<div class="dm-section">Quick actions</div>', unsafe_allow_html=True)
    q1, q2, q3, q4 = st.columns(4)
    if q1.button("Start all nodes", width="stretch", help="Start every node process that is not running"):
        ts = [n for n in cfg.node_ids if not ctrl.is_up(n)]
        for n in ts:
            ctrl.start(n, scenario="manual-dashboard")
        st.success(f"Started nodes {ts}." if ts else "All nodes were already running.")
    if q2.button("Crash the coordinator", width="stretch", type="primary",
                 help="Kill whichever node currently coordinates"):
        coord = get_snapshot()["coordinator_id"]
        if coord is None:
            st.warning("There is no coordinator right now.")
        else:
            ts = ctrl.crash(coord, scenario="manual-dashboard")
            st.success(f"Killed coordinator node {coord} at {_clock(ts)}. Expect a coordinator timeout, then an election.")
    if q3.button("Restart stopped nodes", width="stretch"):
        down = [n for n in cfg.node_ids if not ctrl.is_up(n)]
        for n in down:
            ctrl.start(n, scenario="manual-dashboard")
        st.success(f"Restarted nodes {down}." if down else "No node was stopped.")
    if q4.button("Clear all injected faults", width="stretch"):
        ctrl.clear_faults()
        st.success("Delay and loss faults cleared on every reachable node.")

    # ------------------------------------------------------------------ targeted faults
    st.write("")
    st.markdown('<div class="dm-section">Target a node</div>', unsafe_allow_html=True)
    target = st.radio("Node", cfg.node_ids, horizontal=True, format_func=lambda i: f"Node {i}",
                      key="fault_target", label_visibility="collapsed")
    tab_crash, tab_delay, tab_loss, tab_recover = st.tabs(["Crash", "Delay heartbeats", "Lose messages", "Recover"])

    with tab_crash:
        st.markdown('<div class="dm-note">Kills the node process immediately (no goodbye message). If the node is the '
                    'coordinator, the others detect the silence and elect a new one. The exact kill time is stored as '
                    'ground truth for the metrics.</div>', unsafe_allow_html=True)
        st.write("")
        if st.button(f"Crash node {target}", type="primary", key="do_crash"):
            if not ctrl.is_up(target):
                st.warning(f"Node {target} is not running.")
            else:
                ts = ctrl.crash(target, scenario="manual-dashboard")
                st.success(f"Node {target} killed at {_clock(ts)}.")

    with tab_delay:
        st.markdown('<div class="dm-note">Delays the node\'s outgoing heartbeats like network latency. Delays shorter than '
                    f'the suspicion timeout ({cfg.timing.suspicion_timeout:g}s) are harmless; longer ones cause false '
                    f'suspicions, and beyond {cfg.timing.failure_timeout:g}s a false failure that heals itself.</div>',
                    unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        delay = c1.slider("Delay (ms)", 250, 15000, 4000, step=250, key="delay_ms")
        dur = c2.slider("Duration (s)", 5, 120, 20, key="delay_dur")
        scope = c3.selectbox("Applies to", ["heartbeat", "all"], key="delay_scope")
        if st.button(f"Delay node {target}'s heartbeats", type="primary", key="do_delay"):
            ts = ctrl.delay_heartbeats(target, delay, dur, scope=scope, scenario="manual-dashboard")
            st.success(f"Delay of {delay} ms active on node {target} from {_clock(ts)} for {dur}s.") if ts else \
                st.error(f"Node {target} did not accept the fault - is it running?")

    with tab_loss:
        st.markdown('<div class="dm-note">Randomly drops a share of the node\'s outgoing messages. Heartbeat loss produces '
                    'false suspicions; election loss produces retries and conflicting claims during a failover.</div>',
                    unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        pct = c1.slider("Loss (%)", 5, 100, 50, step=5, key="loss_pct")
        dur = c2.slider("Duration (s)", 5, 120, 20, key="loss_dur")
        scope = c3.selectbox("Applies to", ["heartbeat", "election", "all"], key="loss_scope")
        if st.button(f"Drop messages on node {target}", type="primary", key="do_loss"):
            ts = ctrl.lose_messages(target, pct, dur, scope=scope, scenario="manual-dashboard")
            st.success(f"{pct}% {scope} message loss active on node {target} from {_clock(ts)} for {dur}s.") if ts else \
                st.error(f"Node {target} did not accept the fault - is it running?")

    with tab_recover:
        st.markdown('<div class="dm-note">Restarting creates a new process incarnation. The node re-registers, passes '
                    f'{cfg.timing.recovery_validation_beats} validation heartbeats and only then becomes healthy again. '
                    'If it has the highest priority it starts an election and reclaims leadership.</div>',
                    unsafe_allow_html=True)
        st.write("")
        r1, r2 = st.columns(2)
        if r1.button(f"Restart node {target}", type="primary", key="do_restart"):
            if ctrl.is_up(target):
                st.info(f"Node {target} is already running.")
            else:
                ts = ctrl.start(target, scenario="manual-dashboard")
                st.success(f"Node {target} restarted at {_clock(ts)}.")
        if r2.button(f"Clear faults on node {target}", key="do_clear"):
            ctrl.clear_faults(target)
            st.success(f"Injected delay/loss removed from node {target}.")

    # ------------------------------------------------------------------ log
    st.write("")
    st.markdown('<div class="dm-section">Recent fault actions (ground truth)</div>', unsafe_allow_html=True)
    live(_action_log)


def _status_strip() -> None:
    snap = get_snapshot()
    cols = st.columns(len(snap["nodes"]))
    for col, n in zip(cols, snap["nodes"]):
        col.markdown(C.node_card(n), unsafe_allow_html=True)


def _action_log() -> None:
    rows = get_controller().repo.faults(limit=25)
    if not rows:
        st.markdown('<div class="dm-note">No faults injected yet.</div>', unsafe_allow_html=True)
        return
    label = {"crash": "Crash", "restart": "Restart", "heartbeat_delay": "Heartbeat delay", "message_loss": "Message loss"}

    def params(r):
        p = r.get("params") or {}
        return ", ".join(f"{k}={v}" for k, v in p.items() if k not in ("expires_at",) and v is not None)

    st.dataframe(pd.DataFrame([{
        "Time": _clock(r["start_ts"]), "Action": label.get(r["fault_type"], r["fault_type"]),
        "Node": f"Node {r['target_node_id']}", "Details": params(r),
        "Until": _clock(r["end_ts"]) if r["end_ts"] and r["fault_type"] != "restart" else ("-" if r["fault_type"] != "crash" else "still down"),
        "Source": r.get("scenario") or r.get("source"),
    } for r in rows]), hide_index=True, width="stretch")
