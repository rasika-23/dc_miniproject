"""Small HTML building blocks (cards, pills, event feed) used by every page."""
from __future__ import annotations

import html
import time
from typing import Any, Dict, List, Optional, Tuple

from dashboard.theme import CATEGORY_COLORS, status_colors

STATE_LABELS = {
    "operational": ("Operational", "#15803d", "#dcfce7", False),
    "election_in_progress": ("Election in progress", "#b45309", "#fef3c7", True),
    "no_coordinator": ("No coordinator yet", "#b45309", "#fef3c7", True),
    "split_brain": ("Conflicting coordinators", "#b91c1c", "#fee2e2", True),
    "unavailable": ("Cluster unavailable", "#b91c1c", "#fee2e2", False),
}


def esc(x: Any) -> str:
    return html.escape(str(x))


def pill(text: str, fg: str, bg: str) -> str:
    return f'<span class="dm-pill" style="color:{fg};background:{bg}">{esc(text)}</span>'


def health_pill(health: str) -> str:
    fg, bg = status_colors(health)
    label = {"UNREACHABLE": "Not responding"}.get(health, health.capitalize())
    return pill(label, fg, bg)


def state_chip(state: str, detail: str = "") -> str:
    label, fg, bg, pulse = STATE_LABELS.get(state, ("Unknown", "#475569", "#f1f5f9", False))
    cls = "dm-dot dm-pulse" if pulse else "dm-dot"
    extra = f" &middot; {esc(detail)}" if detail else ""
    return (f'<span class="dm-chip" style="color:{fg};background:{bg};border-color:{fg}33">'
            f'<span class="{cls}" style="background:{fg}"></span>{esc(label)}{extra}</span>')


def hero(title: str, subtitle: str, chip_html: str = "") -> str:
    return (f'<div class="dm-hero dm-root"><div><h1>{esc(title)}</h1><p>{esc(subtitle)}</p></div>'
            f'<div>{chip_html}</div></div>')


def kpi(label: str, value: Any, sub: str = "", accent: str = "#4f46e5") -> str:
    return (f'<div class="dm-kpi dm-root" style="--accent:{accent}"><div class="lbl">{esc(label)}</div>'
            f'<div class="val">{esc(value)}</div><div class="sub">{esc(sub) if sub else "&nbsp;"}</div></div>')


def fmt_age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "never"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms ago"
    if seconds < 90:
        return f"{seconds:.1f} s ago"
    return f"{seconds / 60:.1f} min ago"


def fmt_uptime(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    return f"{s // 3600}h {(s % 3600) // 60:02d}m"


def fmt_ms(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.0f} ms" if value >= 10 else f"{value:.1f} ms"


def node_card(n: Dict[str, Any]) -> str:
    state = n["display_state"]
    fg, bg = status_colors(state)
    is_coord = n.get("role") == "coordinator"
    role_pill = pill("Coordinator", "#4338ca", "#e0e7ff") if is_coord else (
        pill("Candidate", "#b45309", "#fef3c7") if n.get("role") == "candidate" else pill("Worker", "#475569", "#f1f5f9"))
    tags = ""
    for f in n.get("faults", []):
        label = (f"Delay {f['delay_ms']:.0f} ms" if f["kind"] == "heartbeat_delay" else f"Loss {f['loss_pct']:.0f}%")
        tags += pill(f"{label} ({f['scope']})", "#7e22ce", "#f3e8ff")
    if n.get("process", {}).get("running") is False and n.get("probe") == "down":
        tags += pill("process stopped", "#b91c1c", "#fee2e2")
    rows = [
        ("Priority / port", f"{n['priority']} / {n['port']}"),
        ("Last heartbeat", fmt_age(n.get("last_heartbeat_age_s")) if n.get("last_heartbeat_age_s") is not None
         else ("self" if is_coord else "never")),
        ("Response time", fmt_ms(n.get("response_time_ms")) if not is_coord else "-"),
        ("Uptime", fmt_uptime(n.get("uptime_s"))),
    ]
    body = "".join(f'<div class="row"><span>{esc(k)}</span><b>{esc(v)}</b></div>' for k, v in rows)
    return (f'<div class="dm-node dm-root {"coord" if is_coord else ""}" style="--accent:{fg}">'
            f'<div class="top"><span class="name">Node {n["id"]}</span>{role_pill}</div>'
            f'<div style="margin-bottom:.5rem">{health_pill(state)}</div>{body}'
            f'<div class="dm-tags">{tags}</div></div>')


# ----------------------------------------------------------------------------- events
def describe_event(e: Dict[str, Any]) -> Tuple[str, str]:
    """Return (category, human readable text) for an event row."""
    t = e["event_type"]
    d = e.get("details") or {}
    who = e.get("node_id")
    subj = e.get("subject_node_id")
    n = f"Node {who}"
    s = f"Node {subj}"
    if t == "NODE_STARTED":
        return "info", f"{n} started (priority {d.get('priority', '?')})"
    if t == "NODE_REGISTERED":
        return "info", f"{s} registered with coordinator {n}"
    if t == "NODE_SUSPECTED":
        return "suspicion", f"{n} suspects {s}: silent for {d.get('silent_for_s', '?')} s"
    if t == "NODE_FAILED":
        return "failure", f"{n} declared {s} failed after {d.get('silent_for_s', '?')} s of silence"
    if t == "NODE_RECOVERING":
        return "recovery", f"{s} is recovering (validation by {n})"
    if t == "NODE_HEALTHY":
        return "recovery", f"{s} is healthy again (seen by {n})"
    if t == "SUSPICION_CLEARED":
        return "suspicion", f"{n} cleared suspicion of {s} - it was a false suspicion"
    if t == "COORDINATOR_TIMEOUT":
        return "failure", f"{n} lost contact with coordinator {s} ({d.get('silent_for_s', '?')} s)"
    if t == "COORDINATOR_CHANGED":
        return "election", f"{n} now follows {s} (epoch {e.get('epoch')})" if subj != who else \
            f"{n} became coordinator (epoch {e.get('epoch')})"
    if t == "ROLE_CHANGED":
        return "role", f"{n} role: {d.get('from', '?')} to {d.get('to', '?')}"
    if t == "STEPPED_DOWN":
        return "role", f"{n} stepped down in favour of {s}"
    if t == "FAULT_APPLIED":
        return "fault", f"{n} fault applied: {d.get('fault', '?')}"
    if t == "FAULT_CLEARED":
        return "fault", f"{n} fault cleared ({d.get('reason', '')})"
    if t == "STALE_MESSAGE_REJECTED":
        return "info", f"{n} rejected a stale message ({d.get('reason', '')})"
    if t == "LOWER_PRIORITY_CLAIM_REJECTED":
        return "election", f"{n} rejected a lower-priority claim by {s}"
    if t in ("HEARTBEAT_RECEIVED", "COORDINATOR_HEARTBEAT_RECEIVED"):
        return "heartbeat", f"{n} heartbeat from {s} ({d.get('latency_ms', '?')} ms)"
    if t == "PEERS_DISCOVERED":
        return "info", f"{n} discovered peers {d.get('reachable', [])}"
    # election table
    if t == "ELECTION_STARTED":
        return "election", f"{n} started an election ({d.get('reason', '')})"
    if t == "ELECTION_RECEIVED":
        return "election", f"{n} received ELECTION from node {d.get('from_node')}"
    if t == "OK_RECEIVED":
        return "election", f"{n} received OK from {d.get('ok_from')}"
    if t == "ELECTION_WON":
        return "election", f"{n} won the election (epoch {e.get('epoch')})"
    if t == "COORDINATOR_ANNOUNCED":
        return "election", f"{n} announced itself as coordinator (epoch {e.get('epoch')})"
    if t == "COORDINATOR_ACCEPTED":
        return "election", f"{n} accepted coordinator {d.get('coordinator')} (epoch {e.get('epoch')})"
    if t == "ELECTION_RETRY":
        return "election", f"{n} retried its election (attempt {d.get('attempt')})"
    if t == "ELECTION_FAILED":
        return "failure", f"{n} election failed after {d.get('attempts')} attempts"
    if t == "ELECTION_LOST":
        return "election", f"{n} deferred to node {d.get('winner')}"
    # controller (ground truth)
    if t == "CRASH_INJECTED":
        return "fault", f"Controller crashed {s}"
    if t == "RESTART_REQUESTED":
        return "fault", f"Controller restarted {s}"
    if t == "DELAY_INJECTED":
        return "fault", f"Controller delayed heartbeats of {s} by {d.get('delay_ms', '?')} ms"
    if t == "LOSS_INJECTED":
        return "fault", f"Controller injected {d.get('loss_pct', '?')}% {d.get('scope', '')} message loss on {s}"
    if t == "EXPERIMENT_STARTED":
        return "info", f"Experiment run started: {d.get('scenario', '')} (run {d.get('run', '?')})"
    return "info", f"{n}: {t}"


def event_feed(events: List[Dict[str, Any]], limit: int = 12, hide_heartbeats: bool = True) -> str:
    now = time.time()
    rows = []
    for e in events:
        cat, text = describe_event(e)
        if hide_heartbeats and cat == "heartbeat":
            continue
        color = CATEGORY_COLORS.get(cat, "#64748b")
        ago = now - e["ts"]
        when = f"{ago:.0f}s ago" if ago < 90 else time.strftime("%H:%M:%S", time.localtime(e["ts"]))
        rows.append(f'<div class="dm-ev"><span class="dm-dot dot" style="background:{color}"></span>'
                    f'<div><div class="txt">{esc(text)}</div><div class="when">{when}</div></div></div>')
        if len(rows) >= limit:
            break
    if not rows:
        return '<div class="dm-feed dm-root"><div class="dm-empty">No events yet.</div></div>'
    return f'<div class="dm-feed dm-root">{"".join(rows)}</div>'
