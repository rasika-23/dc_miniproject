"""Metric calculations. Pure functions over event lists and ground-truth fault records."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

DECLARATION_EVENTS = ("NODE_FAILED", "COORDINATOR_TIMEOUT")


@dataclass
class GroundFault:
    """A fault whose start time is known for certain (recorded by the experiment controller)."""
    fault_type: str                 # crash | heartbeat_delay | message_loss
    target: int
    start_ts: float
    end_ts: Optional[float] = None
    was_coordinator: bool = False
    params: Dict[str, Any] = field(default_factory=dict)


def _ms(seconds: Optional[float]) -> Optional[float]:
    return None if seconds is None else round(seconds * 1000.0, 3)


def summarize(values: Iterable[Optional[float]]) -> Dict[str, Optional[float]]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return {"n": 0, "mean": None, "min": None, "max": None}
    return {"n": len(vals), "mean": sum(vals) / len(vals), "min": min(vals), "max": max(vals)}


# ----------------------------------------------------------------------------- detection
def detection_latency_ms(node_events: Sequence[dict], target: int, fault_ts: float,
                         until: Optional[float] = None) -> Optional[float]:
    """Failure declaration time - injected fault time (earliest declaration about `target`)."""
    times = [e["ts"] for e in node_events
             if e["event_type"] in DECLARATION_EVENTS and e.get("subject_node_id") == target
             and e["ts"] >= fault_ts and (until is None or e["ts"] <= until)]
    return _ms(min(times) - fault_ts) if times else None


def suspicion_latency_ms(node_events: Sequence[dict], target: int, fault_ts: float,
                         until: Optional[float] = None) -> Optional[float]:
    times = [e["ts"] for e in node_events
             if e["event_type"] == "NODE_SUSPECTED" and e.get("subject_node_id") == target
             and e["ts"] >= fault_ts and (until is None or e["ts"] <= until)]
    return _ms(min(times) - fault_ts) if times else None


# ----------------------------------------------------------------------------- elections
def election_summary(election_events: Sequence[dict], since: float, until: Optional[float] = None) -> Dict[str, Any]:
    """First completed election in the window: announcement time - election start time."""
    evs = [e for e in election_events if e["ts"] >= since and (until is None or e["ts"] <= until)]
    starts = sorted((e for e in evs if e["event_type"] == "ELECTION_STARTED"), key=lambda e: e["ts"])
    announces = sorted((e for e in evs if e["event_type"] == "COORDINATOR_ANNOUNCED"), key=lambda e: e["ts"])
    out: Dict[str, Any] = {
        "elections_started": len({e["election_id"] for e in starts}),
        "election_retries": sum(1 for e in evs if e["event_type"] == "ELECTION_RETRY"),
        "announcements": len(announces), "first_start_ts": None, "announce_ts": None, "winner": None,
        "epoch": None, "duration_ms": None,
    }
    if not starts or not announces:
        return out
    first_start = starts[0]["ts"]
    after = [a for a in announces if a["ts"] >= first_start]
    if not after:
        return out
    a = after[0]
    out.update(first_start_ts=first_start, announce_ts=a["ts"], winner=a["node_id"], epoch=a["epoch"],
               duration_ms=_ms(a["ts"] - first_start))
    return out


def failover_time_ms(election_events: Sequence[dict], fault_ts: float, until: Optional[float] = None,
                     exclude_node: Optional[int] = None) -> Optional[float]:
    """First new-coordinator announcement after the fault - fault time."""
    times = [e["ts"] for e in election_events
             if e["event_type"] == "COORDINATOR_ANNOUNCED" and e["ts"] >= fault_ts
             and (until is None or e["ts"] <= until) and e["node_id"] != exclude_node]
    return _ms(min(times) - fault_ts) if times else None


# ----------------------------------------------------------------------------- recovery
def recovery_time_ms(node_events: Sequence[dict], election_events: Sequence[dict], node: int,
                     restart_ts: float, until: Optional[float] = None) -> Optional[float]:
    """Time the node becomes healthy (or takes over as coordinator) - node restart time."""
    cands = [e["ts"] for e in node_events
             if e["event_type"] == "NODE_HEALTHY" and e.get("subject_node_id") == node and e["ts"] >= restart_ts
             and (until is None or e["ts"] <= until)]
    cands += [e["ts"] for e in election_events
              if e["event_type"] == "COORDINATOR_ANNOUNCED" and e["node_id"] == node and e["ts"] >= restart_ts
              and (until is None or e["ts"] <= until)]
    return _ms(min(cands) - restart_ts) if cands else None


# ----------------------------------------------------------------------------- false positives
def false_positive_stats(node_events: Sequence[dict], down_intervals: Dict[int, List[Tuple[float, Optional[float]]]],
                         window_s: float, since: float, until: Optional[float] = None) -> Dict[str, Any]:
    """A declaration about node S at time t is correct only if S was actually down shortly before t."""
    decls = [e for e in node_events if e["event_type"] in DECLARATION_EVENTS and e["ts"] >= since
             and (until is None or e["ts"] <= until) and e.get("subject_node_id") is not None]
    wrong = 0
    for e in decls:
        t, subject = e["ts"], e["subject_node_id"]
        ok = any(start <= t and (end is None or t <= end + window_s) for start, end in down_intervals.get(subject, []))
        wrong += 0 if ok else 1
    total = len(decls)
    return {"total": total, "incorrect": wrong, "rate_pct": (100.0 * wrong / total) if total else 0.0}


def false_suspicions(node_events: Sequence[dict], since: float, until: Optional[float] = None) -> int:
    return sum(1 for e in node_events if e["event_type"] == "SUSPICION_CLEARED" and e["ts"] >= since
               and (until is None or e["ts"] <= until))


# ----------------------------------------------------------------------------- availability
def availability_pct(samples: Sequence[Tuple[float, bool, Optional[int], int]]) -> Optional[float]:
    """Time-weighted share of the observation window in which a coordinator was reachable."""
    if len(samples) < 2:
        return None
    up = total = 0.0
    for (t0, ok, _, _), (t1, _, _, _) in zip(samples, samples[1:]):
        dt = max(0.0, t1 - t0)
        total += dt
        up += dt if ok else 0.0
    return 100.0 * up / total if total > 0 else None


# ----------------------------------------------------------------------------- aggregate
def compute_run_metrics(*, events: Dict[str, List[dict]], faults: Sequence[GroundFault],
                        restarts: Sequence[Tuple[int, float]], samples: Sequence[Tuple[float, bool, Optional[int], int]],
                        overhead: Tuple[int, Dict[str, int]], down_intervals: Dict[int, List[Tuple[float, Optional[float]]]],
                        since: float, until: float, failure_window_s: float) -> Dict[str, Any]:
    """Compute every metric for one experiment run.

    Returns {"metrics": {name: {"value", "unit"}}, "details": {...}}.
    """
    nev, eev = events["node_events"], events["election_events"]
    crashes = sorted((f for f in faults if f.fault_type == "crash"), key=lambda f: f.start_ts)
    metrics: Dict[str, Dict[str, Any]] = {}
    details: Dict[str, Any] = {"per_fault": []}

    detections, failovers, elections = [], [], []
    for i, f in enumerate(crashes):
        window_end = crashes[i + 1].start_ts if i + 1 < len(crashes) else until
        det = detection_latency_ms(nev, f.target, f.start_ts, window_end)
        entry = {"target": f.target, "was_coordinator": f.was_coordinator, "fault_ts": f.start_ts,
                 "detection_latency_ms": det}
        detections.append(det)
        if f.was_coordinator:
            fo = failover_time_ms(eev, f.start_ts, window_end, exclude_node=f.target)
            es = election_summary(eev, f.start_ts, window_end)
            entry.update(failover_time_ms=fo, election_duration_ms=es["duration_ms"], new_coordinator=es["winner"],
                         new_epoch=es["epoch"])
            failovers.append(fo)
            elections.append(es["duration_ms"])
        details["per_fault"].append(entry)

    def put(name: str, value: Optional[float], unit: str) -> None:
        metrics[name] = {"value": None if value is None else round(float(value), 3), "unit": unit}

    if crashes:
        put("detection_latency_ms", summarize(detections)["mean"], "ms")
    if failovers:
        put("failover_time_ms", summarize(failovers)["mean"], "ms")
        put("election_duration_ms", summarize(elections)["mean"], "ms")

    # suspicion latency (useful for delay / loss experiments)
    susp = [suspicion_latency_ms(nev, f.target, f.start_ts, until) for f in faults
            if f.fault_type in ("heartbeat_delay", "message_loss")]
    if any(s is not None for s in susp):
        put("suspicion_latency_ms", summarize(susp)["mean"], "ms")

    rec = [recovery_time_ms(nev, eev, node, ts, until) for node, ts in restarts]
    if rec:
        put("recovery_time_ms", summarize(rec)["mean"], "ms")
        details["recovery_per_restart"] = [{"node": n, "restart_ts": t, "recovery_time_ms": r}
                                           for (n, t), r in zip(restarts, rec)]

    fp = false_positive_stats(nev, down_intervals, failure_window_s, since, until)
    put("false_positive_rate_pct", fp["rate_pct"], "%")
    put("failure_declarations", fp["total"], "count")
    put("false_failure_declarations", fp["incorrect"], "count")
    put("false_suspicions", false_suspicions(nev, since, until), "count")

    all_es = election_summary(eev, since, until)
    put("elections_started", all_es["elections_started"], "count")
    put("election_retries", all_es["election_retries"], "count")
    if not crashes:
        put("election_duration_ms", all_es["duration_ms"], "ms") if all_es["duration_ms"] is not None else None

    put("monitoring_availability_pct", availability_pct(samples), "%")
    total_msgs, by_type = overhead
    put("message_overhead_msgs", total_msgs, "messages")
    details["messages_by_type"] = by_type
    details["window_s"] = round(until - since, 3)
    return {"metrics": metrics, "details": details}
