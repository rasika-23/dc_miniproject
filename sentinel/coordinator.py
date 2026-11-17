"""Coordinator-side service monitoring: states, incidents, alerts and probe assignment.

Only the node that currently holds the coordinator role is *active*. Everything it knows is
replicated to the workers inside STATUS_SYNC (and immediately before an alert is sent), so after
a failover the new coordinator restores open incidents and the list of alerts already sent,
instead of starting blind and alerting twice (or never).

Delivery semantics: alerts are at-least-once with best-effort de-duplication. An alert carries an
idempotency key; a crash between sending an alert and replicating the fact that it was sent can
cause one duplicate, which the experiments measure.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Set

from communication.http_client import PeerError
from communication.messages import StatusSyncMessage
from node.node_state import Health
from sentinel import alerts as alert_channels
from sentinel.assignment import assign, diff, probers_of
from sentinel.service_state import DEGRADED, DOWN, NO_DATA, UP, ServiceTracker, Transition
from sentinel.views import view_from_snapshot

SEVERITY_DOWN = {"critical": "critical", "high": "major", "medium": "minor", "low": "minor"}


class SentinelCoordinator:
    def __init__(self, rt):
        self.rt = rt
        self.cfg = rt.cfg
        self.settings = rt.cfg.sentinel
        self.active = False
        self.trackers: Dict[str, ServiceTracker] = {}
        self.incidents: Dict[str, Dict[str, Any]] = {}
        self.open_by_service: Dict[str, str] = {}
        self.alerts_sent: Dict[str, float] = {}
        self.alerts_log: List[Dict[str, Any]] = []
        self.assignments: Dict[int, List[str]] = {}
        self.version = 0
        self.takeover_ts = 0.0
        self.prior_nodes: Set[int] = set()
        self.synced: Dict[str, Any] = {}               # last snapshot received from a coordinator
        self._sig: Optional[tuple] = None
        self._inflight: Set[str] = set()
        self._last_synced_incident_ts = 0.0

    # ================================================================== lifecycle
    def activate(self) -> None:
        """Called when this node becomes coordinator: restore what the previous coordinator replicated."""
        if self.active or not self.cfg.services:
            return
        rt, now = self.rt, time.time()
        seed = self.synced or {}
        self.trackers = {s.id: ServiceTracker(s, self.settings, now) for s in self.cfg.services}
        restored_from = "none"
        if seed:
            restored_from = "replicated snapshot"
            for sid, d in (seed.get("services") or {}).items():
                if sid in self.trackers:
                    self.trackers[sid].restore(d)
            self.incidents = {i["incident_id"]: i for i in seed.get("incidents", [])}
            self.alerts_sent = dict(seed.get("alerts_sent", {}))
            self.alerts_log = list(seed.get("alerts", []))
            self.assignments = {int(k): v for k, v in (seed.get("assignments") or {}).items()}
            self.version = int(seed.get("version", 0))
        else:                                              # cold start: fall back to the local journal
            local = rt.sentinel_repo.incidents(limit=200)
            self.incidents = {i["incident_id"]: i for i in local}
            self.alerts_sent = {a["dedupe_key"]: a["ts"] for a in rt.sentinel_repo.alerts(500)}
            if local:
                restored_from = "local journal"
        self.open_by_service = {i["service_id"]: i["incident_id"] for i in self.incidents.values()
                                if i.get("state") == "open"}
        self.prior_nodes = set(self.assignments)
        self.takeover_ts, self.version, self._sig, self.active = now, self.version + 1, None, True
        rt.events.node_event("SENTINEL_ACTIVATED", restored_from=restored_from, services=len(self.trackers),
                             open_incidents=len(self.open_by_service), alerts_known=len(self.alerts_sent))
        self.maybe_rebalance("takeover")
        for inc in list(self.incidents.values()):          # alerts that were never confirmed as sent
            if inc.get("state") == "open" or inc.get("resolved_ts", 0) > now - 60:
                for ev in inc.get("events_reached", []):
                    self._alert(inc, ev, redispatch=True)

    async def seed_from_peers(self) -> None:
        """Before taking over, fetch the freshest replicated state from every reachable peer.

        Without this, a restarted node could win the election holding only its own old journal and
        resurrect incidents that were resolved while it was down."""
        rt = self.rt
        if not self.cfg.services or self.active:
            return

        async def fetch(peer_id: int):
            try:
                return (await rt.client.get(peer_id, "/sentinel/snapshot", timeout=0.5)).get("snapshot")
            except Exception:
                return None

        snaps = await asyncio.gather(*[fetch(p.id) for p in rt.registry.peers()])
        best = self.synced
        for sn in snaps:
            if sn and (sn.get("epoch", 0), sn.get("generated_at", 0)) > (best.get("epoch", 0) if best else -1,
                                                                        best.get("generated_at", 0) if best else -1):
                best = sn
        if best:
            self.synced = best

    def deactivate(self) -> None:
        if self.active:
            self.synced = self.snapshot()                  # keep our last view in case we are elected again
        self.active = False
        self._inflight.clear()

    async def run(self) -> None:
        while not self.rt.stopping:
            await asyncio.sleep(1.0)
            try:
                self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.rt.log.exception("sentinel tick failed")

    def tick(self) -> None:
        if not self.active:
            return
        self.maybe_rebalance("membership change")
        now = time.time()
        grace_until = self.takeover_ts + self.settings.takeover_grace_s
        for tr in self.trackers.values():
            for t in tr.check_no_data(now, grace_until):
                self._on_transition(tr, t)

    # ================================================================== probe assignment
    def _eligible_nodes(self) -> List[int]:
        rt, now = self.rt, time.time()
        grace = self.settings.takeover_grace_s + rt.timing.failure_timeout
        eligible = []
        for nid, m in rt.membership.members.items():
            if nid == rt.state.node_id or m.health == Health.HEALTHY:
                eligible.append(nid)
            elif m.health == Health.UNKNOWN and nid in self.prior_nodes and now - self.takeover_ts < grace:
                eligible.append(nid)                       # keep the previous probers until they prove dead
        return sorted(eligible)

    def maybe_rebalance(self, reason: str) -> bool:
        rt = self.rt
        eligible = self._eligible_nodes()
        if tuple(eligible) == self._sig:
            return False
        old = self.assignments
        new = assign(self.cfg.service_ids, eligible, self.settings.replication, previous=old)
        changes = diff(old, new)
        self._sig, self.assignments, self.version = tuple(eligible), new, self.version + 1
        for sid, tr in self.trackers.items():
            tr.probers = probers_of(new, sid)
        rt.probes.apply_assignment(new.get(rt.state.node_id, []), (rt.coord.epoch, self.version), source="local")
        rt.events.node_event("ASSIGNMENT_REBALANCED", reason=reason, eligible=eligible, changes=changes,
                             version=self.version)
        rt.spawn(self.push_sync(wait=False))
        return True

    # ================================================================== results -> states -> incidents
    def ingest(self, results: List[Dict[str, Any]]) -> int:
        if not self.active:
            return 0
        n = 0
        for r in sorted(results, key=lambda x: x["ts"]):
            tr = self.trackers.get(r.get("service_id"))
            if tr is None:
                continue
            for t in tr.ingest(r):
                self._on_transition(tr, t)
            n += 1
        return n

    def _on_transition(self, tr: ServiceTracker, t: Transition) -> None:
        rt, spec = self.rt, tr.spec
        rt.events.node_event("SERVICE_STATE_CHANGED", service=spec.id, **{"from": t.prev, "to": t.new},
                             reason=t.reason)
        inc = self.incidents.get(self.open_by_service.get(spec.id, ""))
        if t.new == DOWN:
            sev = SEVERITY_DOWN.get(spec.criticality, "major")
            if inc:
                inc.update(kind="down", severity=sev, reason=t.reason)
                self._note(inc, "escalated", f"service is now down: {t.reason}", t.ts)
                self._touch(inc, "escalated")
                rt.events.node_event("INCIDENT_UPDATED", incident_id=inc["incident_id"], service=spec.id,
                                     kind="down", change="escalated")
                self._alert(inc, "escalated")
            else:
                self._open(spec.id, "down", sev, t.reason, t.ts)
        elif t.new == DEGRADED:
            if inc:
                if inc["kind"] == "down":
                    inc.update(kind="degraded", severity="warning", reason=t.reason)
                    self._note(inc, "improved", "service answers again but is degraded", t.ts)
                    self._touch(inc)
            else:
                self._open(spec.id, "degraded", "warning", t.reason, t.ts)
        elif t.new == UP and inc:
            self._resolve(inc, t.reason, t.ts)
        elif t.new == NO_DATA:
            rt.events.node_event("MONITORING_GAP", service=spec.id, reason=t.reason)

    # ------------------------------------------------------------------ incidents
    def _note(self, inc: Dict[str, Any], event: str, text: str, ts: float) -> None:
        inc.setdefault("timeline", []).append({"ts": ts, "event": event, "text": text})

    def _touch(self, inc: Dict[str, Any], reached: Optional[str] = None) -> None:
        inc["updated_ts"] = time.time()
        if reached and reached not in inc.setdefault("events_reached", []):
            inc["events_reached"].append(reached)
        self.rt.sentinel_repo.upsert_incident(inc)

    def _open(self, service_id: str, kind: str, severity: str, reason: str, ts: float) -> Dict[str, Any]:
        rt = self.rt
        inc = {"incident_id": "INC-" + uuid.uuid4().hex[:6].upper(), "service_id": service_id, "kind": kind,
               "severity": severity, "state": "open", "opened_ts": ts, "resolved_ts": None, "reason": reason,
               "opened_by_node": rt.state.node_id, "epoch": rt.coord.epoch, "updated_ts": ts,
               "timeline": [{"ts": ts, "event": "opened", "text": reason}], "events_reached": [],
               "down_periods": []}
        self.incidents[inc["incident_id"]] = inc
        self.open_by_service[service_id] = inc["incident_id"]
        self._touch(inc, "opened")
        rt.events.node_event("INCIDENT_OPENED", incident_id=inc["incident_id"], service=service_id, kind=kind,
                             severity=severity, reason=reason)
        self._alert(inc, "opened")
        return inc

    def _resolve(self, inc: Dict[str, Any], reason: str, ts: float) -> None:
        inc.update(state="resolved", resolved_ts=ts)
        self._note(inc, "resolved", reason, ts)
        self._touch(inc, "resolved")
        self.open_by_service.pop(inc["service_id"], None)
        self.rt.events.node_event("INCIDENT_RESOLVED", incident_id=inc["incident_id"], service=inc["service_id"],
                                  duration_s=round(ts - inc["opened_ts"], 2))
        self._alert(inc, "resolved")

    # ------------------------------------------------------------------ alerts
    def _alert(self, inc: Dict[str, Any], event: str, redispatch: bool = False) -> None:
        key = f"{inc['incident_id']}:{event}"
        if key in self.alerts_sent or key in self._inflight:
            return
        self._inflight.add(key)
        self.rt.spawn(self._dispatch(inc, event, key, redispatch))

    async def _dispatch(self, inc: Dict[str, Any], event: str, key: str, redispatch: bool) -> None:
        rt = self.rt
        try:
            await self.push_sync(wait=True, timeout=1.0)       # replicate the incident before alerting about it
            if not self.active:
                return                                         # lost leadership: the new coordinator re-dispatches
            spec = self.cfg.service(inc["service_id"])
            alert = {"dedupe_key": key, "incident_id": inc["incident_id"], "service_id": spec.id,
                     "service_name": spec.name, "event": event, "severity": inc["severity"], "ts": time.time(),
                     "message": alert_channels.message_for(event, inc, spec.name), "node_id": rt.state.node_id,
                     "epoch": rt.coord.epoch}
            alert["channels"] = await alert_channels.deliver(alert, self.settings.webhook_url)
            self.alerts_sent[key] = alert["ts"]
            self.alerts_log.append(alert)
            del self.alerts_log[:-200]
            rt.sentinel_repo.add_alert(alert)
            rt.events.node_event("ALERT_DISPATCHED", dedupe_key=key, incident_id=inc["incident_id"],
                                 service=spec.id, event=event, severity=inc["severity"],
                                 channels=alert["channels"], redispatch=redispatch, message=alert["message"])
            rt.spawn(self.push_sync(wait=False))
        finally:
            self._inflight.discard(key)

    # ================================================================== replication
    def snapshot(self) -> Dict[str, Any]:
        incidents = sorted(self.incidents.values(), key=lambda i: i["opened_ts"], reverse=True)[:200]
        sent = dict(sorted(self.alerts_sent.items(), key=lambda kv: kv[1], reverse=True)[:500])
        return {"generated_at": time.time(), "coordinator": self.rt.state.node_id, "epoch": self.rt.coord.epoch,
                "version": self.version, "services": {sid: tr.to_dict() for sid, tr in self.trackers.items()},
                "incidents": incidents, "alerts_sent": sent, "alerts": self.alerts_log[-100:],
                "assignments": {str(n): v for n, v in self.assignments.items()}}

    async def push_sync(self, wait: bool = False, timeout: float = 1.0) -> None:
        """Send the current state to every peer (used for event-driven replication)."""
        rt = self.rt
        if not self.active or rt.stopping:
            return
        members, snap = rt.membership.snapshot(rt.state.node_id), self.snapshot()

        async def one(peer_id: int) -> None:
            msg = StatusSyncMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation,
                                    epoch=rt.coord.epoch, coordinator_id=rt.state.node_id, membership=members,
                                    sentinel=snap)
            try:
                await rt.client.send(peer_id, "/status-sync", msg)
            except PeerError:
                pass

        tasks = [rt.spawn(one(p.id)) for p in rt.registry.peers()]
        if wait and tasks:
            await asyncio.wait(tasks, timeout=timeout)

    def on_snapshot(self, sn: Dict[str, Any], epoch: int) -> None:
        """Worker side: remember the coordinator's state and apply this node's probe assignment."""
        if not sn:
            return
        rt = self.rt
        self.synced = sn
        mine = (sn.get("assignments") or {}).get(str(rt.state.node_id), [])
        rt.probes.apply_assignment(mine, (epoch, int(sn.get("version", 0))))
        newest = self._last_synced_incident_ts
        for inc in sn.get("incidents", []):
            if inc.get("updated_ts", 0) > self._last_synced_incident_ts:
                rt.sentinel_repo.upsert_incident(inc)
                newest = max(newest, inc["updated_ts"])
        self._last_synced_incident_ts = newest

    # ================================================================== views
    def status_view(self) -> Dict[str, Any]:
        return view_from_snapshot(self.snapshot(), self.cfg.services, self.settings.availability_window_h)
