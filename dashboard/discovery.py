"""Dashboard data access: locate the active coordinator and assemble one cluster snapshot.

The dashboard is only a *client*. It never assumes a particular node is the coordinator, and it
degrades to an "election in progress" / "cluster unavailable" state instead of failing.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import streamlit as st

from election.leader_discovery import STATE_OPERATIONAL
from fault_injection import process_control
from fault_injection.controller import ClusterController
from node.config import ClusterConfig, load_config

COUNT_KEYS = ["HEALTHY", "SUSPECTED", "RECOVERING", "FAILED", "UNKNOWN"]


@st.cache_resource(show_spinner=False)
def get_controller() -> ClusterController:
    return ClusterController(load_config())


def get_config() -> ClusterConfig:
    return get_controller().cfg


# ----------------------------------------------------------------------------- snapshot
def build_snapshot(ctrl: ClusterController) -> Dict[str, Any]:
    cfg = ctrl.cfg
    disc, members = ctrl.coordinator_membership()
    nodes: List[Dict[str, Any]] = []
    for spec in cfg.nodes:
        h = disc.health.get(spec.id)
        m = members.get(spec.id) if members else None
        probe = "up" if h else "down"
        if m is not None:
            health = m["health"]
            display = "UNREACHABLE" if (probe == "down" and health == "HEALTHY") else health
        else:
            health = "HEALTHY" if h else "FAILED"
            display = health
        role = (h or {}).get("role") or ("coordinator" if spec.id == disc.coordinator_id else "unknown")
        nodes.append({
            "id": spec.id, "priority": spec.priority, "port": spec.port, "probe": probe,
            "health": health, "display_state": display, "role": role,
            "uptime_s": (h or {}).get("uptime_s") if h else None,
            "incarnation": (h or {}).get("incarnation") if h else None,
            "last_heartbeat_age_s": m["last_heartbeat_age_s"] if m else None,
            "response_time_ms": m["response_time_ms"] if m else None,
            "failure_count": m["failure_count"] if m else 0,
            "epoch": (h or {}).get("epoch"), "follows": (h or {}).get("coordinator_id"),
            "election_in_progress": bool((h or {}).get("election_in_progress")),
            "faults": (h or {}).get("faults", []) if h else [],
            "process": process_control.recorded_status(spec.id, cfg),
        })
    counts = {k: 0 for k in COUNT_KEYS}
    for n in nodes:
        counts["SUSPECTED" if n["display_state"] == "UNREACHABLE" else n["health"]] += 1
    return {"ts": time.time(), "state": disc.state, "coordinator_id": disc.coordinator_id, "epoch": disc.epoch,
            "highest_epoch": disc.highest_epoch, "conflicting": disc.conflicting, "reachable": disc.reachable,
            "nodes": nodes, "counts": counts, "source": "coordinator" if members else "probe",
            "electing": [n["id"] for n in nodes if n["election_in_progress"] or n["role"] == "candidate"]}


@st.cache_data(ttl=1.0, show_spinner=False)
def _cached_snapshot(_ctrl: ClusterController, bucket: int) -> Dict[str, Any]:
    return build_snapshot(_ctrl)


def get_snapshot() -> Dict[str, Any]:
    """Shared by all fragments; refreshed at most once per second."""
    snap = _cached_snapshot(get_controller(), int(time.time()))
    _record(snap)
    return snap


def _record(snap: Dict[str, Any]) -> None:
    """Keep a short per-session history (response times, observed availability)."""
    ss = st.session_state
    hist = ss.setdefault("history", [])
    if hist and hist[-1]["ts"] == snap["ts"]:
        return
    prev = ss.get("avail_last_ts")
    if prev is not None:
        dt = min(snap["ts"] - prev, 10.0)
        ss["avail_total"] = ss.get("avail_total", 0.0) + dt
        if snap["state"] == STATE_OPERATIONAL:
            ss["avail_up"] = ss.get("avail_up", 0.0) + dt
    ss["avail_last_ts"] = snap["ts"]
    hist.append({"ts": snap["ts"], "state": snap["state"], "coordinator": snap["coordinator_id"],
                 "nodes": {n["id"]: {"rt": n["response_time_ms"], "health": n["display_state"]}
                           for n in snap["nodes"]}})
    del hist[:-400]


def session_availability() -> Optional[float]:
    total = st.session_state.get("avail_total", 0.0)
    return None if total < 1 else 100.0 * st.session_state.get("avail_up", 0.0) / total


# ----------------------------------------------------------------------------- events
_FAULT_EVENT = {"crash": "CRASH_INJECTED", "restart": "RESTART_REQUESTED",
                "heartbeat_delay": "DELAY_INJECTED", "message_loss": "LOSS_INJECTED"}


def _controller_events(ctrl: ClusterController, since: float) -> List[Dict[str, Any]]:
    out = []
    for row in ctrl.repo.faults(since=since):
        etype = _FAULT_EVENT.get(row["fault_type"])
        if etype and row["start_ts"] >= since:
            out.append({"event_id": row["fault_id"], "ts": row["start_ts"], "node_id": 0, "incarnation": 0,
                        "event_type": etype, "epoch": 0, "subject_node_id": row["target_node_id"],
                        "details": row.get("params") or {}, "table": "controller"})
    for e in ctrl.repo.node_events(since=since, event_types=["EXPERIMENT_STARTED"]):
        out.append({**e, "table": "controller"})
    return out


@st.cache_data(ttl=2.0, show_spinner=False)
def _cached_events(_ctrl: ClusterController, window_s: int, bucket: int) -> List[Dict[str, Any]]:
    since = time.time() - window_s
    ev = _ctrl.collect_events(since=since)
    merged = [{**e, "table": "node"} for e in ev["node_events"]]
    merged += [{**e, "table": "election"} for e in ev["election_events"]]
    merged += _controller_events(_ctrl, since)
    merged.sort(key=lambda e: e["ts"], reverse=True)
    return merged


def get_events(window_s: int = 900) -> List[Dict[str, Any]]:
    """Events from every reachable node (merged, de-duplicated by event id), newest first."""
    return _cached_events(get_controller(), window_s, int(time.time() // 2))


def election_rows(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group election events by election id into one row per election."""
    groups: Dict[str, List[dict]] = {}
    for e in events:
        if e["table"] == "election" and e.get("election_id"):
            groups.setdefault(e["election_id"], []).append(e)
    rows = []
    for eid, evs in groups.items():
        evs.sort(key=lambda e: e["ts"])
        started = next((e for e in evs if e["event_type"] == "ELECTION_STARTED"), evs[0])
        won = next((e for e in evs if e["event_type"] == "ELECTION_WON"), None)
        ann = next((e for e in evs if e["event_type"] == "COORDINATOR_ANNOUNCED"), None)
        lost = next((e for e in evs if e["event_type"] == "ELECTION_LOST"), None)
        failed = next((e for e in evs if e["event_type"] == "ELECTION_FAILED"), None)
        outcome = "Won" if won else "Deferred" if lost else "Failed" if failed else "In progress"
        end = ann or lost or failed
        d = started.get("details") or {}
        rows.append({
            "election_id": eid, "candidate": started["node_id"], "reason": d.get("reason", ""),
            "previous_coordinator": d.get("previous_coordinator"), "outcome": outcome,
            "new_coordinator": started["node_id"] if won else (lost.get("details", {}).get("winner") if lost else None),
            "epoch": won["epoch"] if won else None, "start_ts": started["ts"], "end_ts": end["ts"] if end else None,
            "duration_ms": (ann or won or lost or {}).get("duration_ms") if end else None,
            "retries": sum(1 for e in evs if e["event_type"] == "ELECTION_RETRY"),
        })
    rows.sort(key=lambda r: r["start_ts"], reverse=True)
    return rows
