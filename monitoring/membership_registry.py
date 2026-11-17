"""Coordinator-side membership view.

The membership table lives in coordinator memory only. A new coordinator rebuilds it from
REGISTER and HEARTBEAT messages; SQLite is the event log, not a replicated membership store.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from communication.peer_registry import PeerRegistry
from node.config import Timing
from node.node_state import Health

EWMA_ALPHA = 0.3


@dataclass
class Member:
    node_id: int
    priority: int
    health: Health = Health.UNKNOWN
    incarnation: int = 0
    registered: bool = False
    started_at: Optional[float] = None
    last_seen_mono: float = 0.0
    last_seen_wall: Optional[float] = None
    response_time_ms: Optional[float] = None
    validation_beats: int = 0
    suspected_at: Optional[float] = None
    failed_at: Optional[float] = None
    failure_count: int = 0
    last_failure_ts: Optional[float] = None
    history: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Transition:
    node_id: int
    prev: Health
    new: Health
    kind: str                        # registered | suspected | failed | recovering | healthy | suspicion_cleared
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HeartbeatOutcome:
    accepted: bool
    reason: str = "ok"
    need_register: bool = False
    transitions: List[Transition] = field(default_factory=list)
    health: Optional[Health] = None


class MembershipRegistry:
    def __init__(self, registry: PeerRegistry, self_id: int, timing: Timing):
        self.registry, self.self_id, self.timing = registry, self_id, timing
        self.members: Dict[int, Member] = {}
        self.active = False
        self.since: Optional[float] = None

    # ------------------------------------------------------------------ lifecycle
    def reset(self, self_incarnation: int) -> None:
        """Called when this node becomes coordinator: start from an empty view."""
        now = time.monotonic()
        self.members = {}
        for spec in self.registry.all():
            m = Member(node_id=spec.id, priority=spec.priority, last_seen_mono=now)
            if spec.id == self.self_id:
                m.health, m.registered, m.incarnation = Health.HEALTHY, True, self_incarnation
                m.started_at = self_incarnation / 1000.0
                m.last_seen_wall = time.time()
            self.members[spec.id] = m
        self.active, self.since = True, time.time()

    def deactivate(self) -> None:
        self.active = False
        self.members = {}

    # ------------------------------------------------------------------ inputs
    def on_register(self, node_id: int, incarnation: int, now: Optional[float] = None) -> HeartbeatOutcome:
        now = now if now is not None else time.monotonic()
        m = self.members.get(node_id)
        if m is None:
            return HeartbeatOutcome(False, "unknown_node")
        if m.incarnation and incarnation < m.incarnation:
            return HeartbeatOutcome(False, "stale_incarnation")
        prev = m.health
        new_incarnation = incarnation != m.incarnation
        m.incarnation, m.started_at = incarnation, incarnation / 1000.0
        m.registered, m.last_seen_mono, m.last_seen_wall = True, now, time.time()
        trs: List[Transition] = []
        if prev == Health.UNKNOWN:
            m.health = Health.HEALTHY
            trs.append(Transition(node_id, prev, m.health, "registered", {"incarnation": incarnation}))
        elif new_incarnation or prev == Health.FAILED:
            # a restarted (or previously failed) node must prove itself before it is HEALTHY again
            m.health, m.validation_beats = Health.RECOVERING, 0
            trs.append(Transition(node_id, prev, m.health, "recovering",
                                  {"incarnation": incarnation, "restarted": new_incarnation}))
        elif prev == Health.SUSPECTED:
            m.health, m.suspected_at = Health.HEALTHY, None
            trs.append(Transition(node_id, prev, m.health, "suspicion_cleared", {"via": "register"}))
        elif prev == Health.RECOVERING:
            m.validation_beats = 0
        return HeartbeatOutcome(True, transitions=trs, health=m.health)

    def on_heartbeat(self, node_id: int, incarnation: int, latency_ms: float,
                     now: Optional[float] = None) -> HeartbeatOutcome:
        now = now if now is not None else time.monotonic()
        m = self.members.get(node_id)
        if m is None:
            return HeartbeatOutcome(False, "unknown_node")
        if m.incarnation and incarnation < m.incarnation:
            return HeartbeatOutcome(False, "stale_incarnation")
        trs: List[Transition] = []
        need_register = False
        silent_for = now - m.last_seen_mono
        m.last_seen_mono, m.last_seen_wall = now, time.time()
        m.response_time_ms = latency_ms if m.response_time_ms is None else (
            EWMA_ALPHA * latency_ms + (1 - EWMA_ALPHA) * m.response_time_ms)
        prev = m.health
        if incarnation != m.incarnation or prev == Health.UNKNOWN:
            need_register = True                       # we do not know this incarnation yet
        elif prev == Health.SUSPECTED:
            m.health, m.suspected_at = Health.HEALTHY, None
            trs.append(Transition(node_id, prev, m.health, "suspicion_cleared",
                                  {"silent_for_s": round(silent_for, 3), "false_suspicion": True}))
        elif prev == Health.FAILED:
            m.health, m.validation_beats, m.registered = Health.RECOVERING, 0, False
            need_register = True
            trs.append(Transition(node_id, prev, m.health, "recovering",
                                  {"silent_for_s": round(silent_for, 3), "restarted": False}))
        elif prev == Health.RECOVERING:
            if not m.registered:
                need_register = True
            else:
                m.validation_beats += 1
                if m.validation_beats >= self.timing.recovery_validation_beats:
                    m.health = Health.HEALTHY
                    down_s = (time.time() - m.failed_at) if m.failed_at else None
                    since_start = (time.time() - m.started_at) if m.started_at else None
                    trs.append(Transition(node_id, prev, m.health, "healthy",
                                          {"validated_beats": m.validation_beats,
                                           "downtime_s": None if down_s is None else round(down_s, 3),
                                           "since_start_s": None if since_start is None else round(since_start, 3)}))
        return HeartbeatOutcome(True, need_register=need_register, transitions=trs, health=m.health)

    def check_timeouts(self, now: Optional[float] = None) -> List[Transition]:
        """Failure detector tick: HEALTHY/RECOVERING/UNKNOWN -> SUSPECTED -> FAILED."""
        now = now if now is not None else time.monotonic()
        t = self.timing
        trs: List[Transition] = []
        for m in self.members.values():
            if m.node_id == self.self_id:
                continue
            silent = now - m.last_seen_mono
            if m.health in (Health.UNKNOWN, Health.HEALTHY, Health.RECOVERING) and silent > t.suspicion_timeout:
                prev, m.health = m.health, Health.SUSPECTED
                m.suspected_at = time.time()
                trs.append(Transition(m.node_id, prev, m.health, "suspected",
                                      {"silent_for_s": round(silent, 3), "never_heard": m.last_seen_wall is None}))
            elif m.health == Health.SUSPECTED and silent > t.failure_timeout:
                prev, m.health = m.health, Health.FAILED
                m.failed_at = m.last_failure_ts = time.time()
                m.failure_count += 1
                m.registered = False
                m.history.append({"failed_at": m.failed_at, "silent_for_s": round(silent, 3)})
                trs.append(Transition(m.node_id, prev, m.health, "failed",
                                      {"silent_for_s": round(silent, 3), "failure_count": m.failure_count}))
        return trs

    # ------------------------------------------------------------------ views
    def snapshot(self, coordinator_id: Optional[int]) -> List[Dict[str, Any]]:
        now_m, now_w = time.monotonic(), time.time()
        rows = []
        for m in sorted(self.members.values(), key=lambda x: x.node_id):
            is_self = m.node_id == self.self_id
            rows.append({
                "node_id": m.node_id, "priority": m.priority, "health": m.health.value,
                "role": "coordinator" if m.node_id == coordinator_id else "worker",
                "incarnation": m.incarnation, "registered": m.registered,
                "started_at": m.started_at,
                "uptime_s": (now_w - m.started_at) if m.started_at else None,
                "last_heartbeat_ts": now_w if is_self else m.last_seen_wall,
                "last_heartbeat_age_s": 0.0 if is_self else (
                    round(now_m - m.last_seen_mono, 3) if m.last_seen_wall else None),
                "response_time_ms": 0.0 if is_self else m.response_time_ms,
                "failure_count": m.failure_count, "last_failure_ts": m.last_failure_ts,
                "validation_beats": m.validation_beats,
            })
        return rows

    def health_of(self, node_id: int) -> Optional[Health]:
        m = self.members.get(node_id)
        return m.health if m else None
