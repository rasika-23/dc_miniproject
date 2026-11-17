"""Per-service health state machine (pure logic, no I/O).

UNKNOWN -> UP -> DEGRADED -> DOWN -> (recovery) -> UP, plus NO_DATA when checks stop arriving.
A failure only counts as DOWN after `failure_threshold` consecutive failed checks that were seen
by enough *distinct* probing nodes (quorum), so one node with a local network problem cannot
raise a false outage on its own.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from node.config import SentinelSettings, ServiceSpec

UNKNOWN, UP, DEGRADED, DOWN, NO_DATA = "UNKNOWN", "UP", "DEGRADED", "DOWN", "NO_DATA"


@dataclass(frozen=True)
class Transition:
    service_id: str
    prev: str
    new: str
    reason: str
    ts: float


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(pct / 100.0 * (len(ordered) - 1)))))
    return ordered[idx]


class ServiceTracker:
    def __init__(self, spec: ServiceSpec, settings: SentinelSettings, now: Optional[float] = None):
        self.spec, self.s = spec, settings
        now = now if now is not None else time.time()
        self.status, self.since, self.first_seen = UNKNOWN, now, now
        self.consec_fail = self.consec_ok = self.consec_slow = self.consec_fast = 0
        self.streak_nodes: Set[int] = set()
        self.last_check_ts: Optional[float] = None
        self.last_ok_ts: Optional[float] = None
        self.last_error: Optional[str] = None
        self.probers: List[int] = []
        self.recent: Deque[Tuple[float, bool, float, int]] = deque(maxlen=120)
        self.down_periods: List[List[Optional[float]]] = []
        self._seen: Deque[Tuple[int, float]] = deque(maxlen=600)
        self._seen_set: Set[Tuple[int, float]] = set()

    # ------------------------------------------------------------------ input
    def ingest(self, r: Dict[str, Any]) -> List[Transition]:
        key = (int(r["node_id"]), float(r["ts"]))
        if key in self._seen_set:                     # retransmitted batch
            return []
        if len(self._seen) == self._seen.maxlen:
            self._seen_set.discard(self._seen[0])
        self._seen.append(key)
        self._seen_set.add(key)

        ts, ok = float(r["ts"]), bool(r["ok"])
        latency = float(r.get("latency_ms") or 0.0)
        self.recent.append((ts, ok, latency, int(r["node_id"])))
        self.last_check_ts = ts if self.last_check_ts is None else max(self.last_check_ts, ts)
        prev, new, reason = self.status, self.status, ""
        s = self.s
        if ok:
            self.last_ok_ts = ts
            self.consec_fail, self.streak_nodes = 0, set()
            self.consec_ok += 1
            slow = latency > self.spec.latency_warn_ms
            self.consec_slow = self.consec_slow + 1 if slow else 0
            self.consec_fast = 0 if slow else self.consec_fast + 1
            if prev in (UNKNOWN, NO_DATA):
                new, reason = UP, "checks are passing" if prev == UNKNOWN else "check results resumed"
            elif prev == DOWN and self.consec_ok >= s.recovery_threshold:
                if self.consec_slow >= s.degraded_threshold:
                    new, reason = DEGRADED, "responding again but slowly"
                else:
                    new, reason = UP, f"{self.consec_ok} consecutive good checks"
            elif prev == UP and self.consec_slow >= s.degraded_threshold:
                new, reason = DEGRADED, (f"{self.consec_slow} consecutive checks slower than "
                                         f"{self.spec.latency_warn_ms:.0f} ms")
            elif prev == DEGRADED and self.consec_fast >= s.recovery_threshold:
                new, reason = UP, "latency back to normal"
        else:
            self.consec_ok = self.consec_slow = self.consec_fast = 0
            self.consec_fail += 1
            self.streak_nodes.add(int(r["node_id"]))
            self.last_error = r.get("error") or "check failed"
            need = min(s.quorum, max(1, len(self.probers)))
            if prev != DOWN and self.consec_fail >= s.failure_threshold and len(self.streak_nodes) >= need:
                new = DOWN
                reason = (f"{self.consec_fail} consecutive failed checks from nodes "
                          f"{sorted(self.streak_nodes)}: {self.last_error}")
        return self._move(prev, new, reason, ts)

    def check_no_data(self, now: float, grace_until: float = 0.0) -> List[Transition]:
        """Mark the service NO_DATA when its checks stop arriving (e.g. every prober is gone)."""
        if self.status not in (UP, DEGRADED) or now < grace_until:
            return []
        last = self.last_check_ts or self.since
        limit = self.s.no_data_factor * self.spec.interval_s
        if now - last > limit:
            return self._move(self.status, NO_DATA, f"no check results for {now - last:.0f} s", now)
        return []

    def _move(self, prev: str, new: str, reason: str, ts: float) -> List[Transition]:
        if new == prev:
            return []
        if new == DOWN:
            self.down_periods.append([ts, None])
        elif prev == DOWN and self.down_periods and self.down_periods[-1][1] is None:
            self.down_periods[-1][1] = ts
        self.status, self.since = new, ts
        return [Transition(self.spec.id, prev, new, reason, ts)]

    # ------------------------------------------------------------------ views
    def latency_stats(self, n: int = 60) -> Dict[str, Optional[float]]:
        vals = [lat for _, ok, lat, _ in list(self.recent)[-n:] if ok]
        return {"p50": percentile(vals, 50), "p95": percentile(vals, 95)}

    def to_dict(self) -> Dict[str, Any]:
        st = self.latency_stats()
        return {"id": self.spec.id, "status": self.status, "since": self.since, "first_seen": self.first_seen,
                "consec_fail": self.consec_fail, "consec_ok": self.consec_ok, "consec_slow": self.consec_slow,
                "consec_fast": self.consec_fast, "streak_nodes": sorted(self.streak_nodes), "last_check_ts": self.last_check_ts,
                "last_ok_ts": self.last_ok_ts, "last_error": self.last_error, "probers": list(self.probers),
                "down_periods": [list(p) for p in self.down_periods], "p50": st["p50"], "p95": st["p95"],
                "recent": [list(x) for x in list(self.recent)[-40:]]}

    def restore(self, d: Dict[str, Any]) -> None:
        self.status, self.since = d.get("status", UNKNOWN), d.get("since", self.since)
        self.first_seen = d.get("first_seen", self.first_seen)
        self.consec_fail, self.consec_ok = d.get("consec_fail", 0), d.get("consec_ok", 0)
        self.consec_slow, self.consec_fast = d.get("consec_slow", 0), d.get("consec_fast", 0)
        self.streak_nodes = set(d.get("streak_nodes", []))
        self.last_check_ts, self.last_ok_ts = d.get("last_check_ts"), d.get("last_ok_ts")
        self.last_error, self.probers = d.get("last_error"), list(d.get("probers", []))
        self.down_periods = [list(p) for p in d.get("down_periods", [])]
        self.recent.extend(tuple(x) for x in d.get("recent", []))


def availability_pct(down_periods: List[List[Optional[float]]], window_start: float, now: float) -> Optional[float]:
    """Share of [window_start, now] during which the service was not DOWN."""
    span = now - window_start
    if span <= 0:
        return None
    down = 0.0
    for start, end in down_periods:
        a, b = max(start, window_start), min(end if end is not None else now, now)
        if b > a:
            down += b - a
    return max(0.0, min(100.0, 100.0 * (1 - down / span)))
