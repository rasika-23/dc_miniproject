"""Turn a coordinator snapshot into the data the dashboard and API return (pure functions)."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Sequence

from node.config import ServiceSpec
from sentinel.service_state import availability_pct


def view_from_snapshot(snapshot: Dict[str, Any], specs: Sequence[ServiceSpec], window_h: float = 24.0,
                       now: Optional[float] = None) -> Dict[str, Any]:
    now = now if now is not None else time.time()
    window_s = window_h * 3600.0
    incidents = snapshot.get("incidents", [])
    open_by_service = {i["service_id"]: i for i in incidents if i.get("state") == "open"}
    assignments = {int(k): v for k, v in (snapshot.get("assignments") or {}).items()}
    services: List[Dict[str, Any]] = []
    counts = {"UP": 0, "DEGRADED": 0, "DOWN": 0, "NO_DATA": 0, "UNKNOWN": 0}
    avail_values: List[float] = []
    covered = 0
    for spec in specs:
        st = (snapshot.get("services") or {}).get(spec.id, {})
        status = st.get("status", "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
        first_seen = st.get("first_seen", now)
        ws = max(now - window_s, first_seen)
        avail = availability_pct(st.get("down_periods", []), ws, now) if now - ws >= 5 else None
        if avail is not None:
            avail_values.append(avail)
        down_s = 0.0
        for a, b in st.get("down_periods", []):
            lo, hi = max(a, now - window_s), min(b if b is not None else now, now)
            down_s += max(0.0, hi - lo)
        allowed = (1 - spec.slo_target / 100.0) * window_s
        budget_left = None if allowed <= 0 or avail is None else max(0.0, 100.0 * (1 - down_s / allowed))
        probers = [n for n, sids in assignments.items() if spec.id in sids]
        covered += 1 if probers else 0
        services.append({
            "id": spec.id, "name": spec.name, "url": spec.url, "criticality": spec.criticality,
            "slo_target": spec.slo_target, "owner": spec.owner, "description": spec.description,
            "interval_s": spec.interval_s, "status": status, "since": st.get("since"),
            "last_check_ts": st.get("last_check_ts"), "last_ok_ts": st.get("last_ok_ts"),
            "last_error": st.get("last_error"), "consec_fail": st.get("consec_fail", 0),
            "probers": probers or st.get("probers", []), "p50": st.get("p50"), "p95": st.get("p95"),
            "availability_pct": avail, "error_budget_left_pct": budget_left,
            "open_incident": (open_by_service.get(spec.id) or {}).get("incident_id"),
            "recent": st.get("recent", []),
        })
    resolved = [i for i in incidents if i.get("state") == "resolved" and i.get("resolved_ts")]
    mttr = (sum(i["resolved_ts"] - i["opened_ts"] for i in resolved) / len(resolved)) if resolved else None
    return {
        "generated_at": snapshot.get("generated_at"), "coordinator": snapshot.get("coordinator"),
        "epoch": snapshot.get("epoch"), "services": services, "assignments": assignments,
        "summary": {**counts, "total": len(specs), "open_incidents": len(open_by_service),
                    "availability_pct": (sum(avail_values) / len(avail_values)) if avail_values else None,
                    "mttr_s": mttr, "coverage_pct": (100.0 * covered / len(specs)) if specs else None,
                    "resolved_incidents": len(resolved)},
    }
