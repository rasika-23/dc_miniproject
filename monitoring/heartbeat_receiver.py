"""Handling of incoming heartbeat-type messages."""
from __future__ import annotations

import time
from collections import Counter
from typing import Any, Dict, List

from communication.messages import (CoordinatorHeartbeatMessage, HeartbeatMessage, StatusSyncMessage)
from monitoring.membership_registry import Transition
from node.node_state import Role

_TRANSITION_EVENT = {
    "registered": "NODE_REGISTERED",
    "suspected": "NODE_SUSPECTED",
    "failed": "NODE_FAILED",
    "recovering": "NODE_RECOVERING",
    "healthy": "NODE_HEALTHY",
    "suspicion_cleared": "SUSPICION_CLEARED",
}


def log_transitions(rt, transitions: List[Transition]) -> None:
    """Persist state transitions observed by the coordinator."""
    for tr in transitions:
        rt.events.node_event(_TRANSITION_EVENT[tr.kind], tr.node_id,
                             **{"from": tr.prev.value, "to": tr.new.value, **tr.details})
        if tr.kind == "registered":
            # a first registration is also the node's first HEALTHY state
            rt.events.node_event("NODE_HEALTHY", tr.node_id, **{"from": tr.prev.value, "to": tr.new.value,
                                                                "first_seen": True}, quiet=True)


class HeartbeatReceiver:
    def __init__(self, rt):
        self.rt = rt
        self._received: Counter = Counter()

    # ----------------------------------------------------- worker -> coordinator
    def handle_heartbeat(self, msg: HeartbeatMessage) -> Dict[str, Any]:
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        if rt.state.role != Role.COORDINATOR or not rt.membership.active:
            return {"accepted": False, "reason": "not_coordinator",
                    "coordinator_id": rt.coord.coordinator_id, "epoch": rt.coord.epoch}
        latency_ms = max(0.0, (time.time() - msg.sent_at) * 1000.0)
        out = rt.membership.on_heartbeat(msg.sender_id, msg.incarnation, latency_ms)
        if not out.accepted:
            rt.events.node_event("STALE_MESSAGE_REJECTED", msg.sender_id, msg_type=msg.msg_type, reason=out.reason,
                                 msg_incarnation=msg.incarnation)
            return {"accepted": False, "reason": out.reason, "coordinator_id": rt.state.node_id,
                    "epoch": rt.coord.epoch}
        log_transitions(rt, out.transitions)
        self._sampled_log("HEARTBEAT_RECEIVED", msg.sender_id, latency_ms)
        return {"accepted": True, "need_register": out.need_register, "coordinator_id": rt.state.node_id,
                "epoch": rt.coord.epoch, "health": out.health.value if out.health else None}

    # ----------------------------------------------------- coordinator -> workers
    async def handle_coordinator_heartbeat(self, msg: CoordinatorHeartbeatMessage) -> Dict[str, Any]:
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        resp = await rt.election.handle_coordinator_claim(
            msg.coordinator_id, msg.epoch, source="heartbeat", lease_expires_at=msg.lease_expires_at)
        if resp.get("accepted"):
            self._sampled_log("COORDINATOR_HEARTBEAT_RECEIVED", msg.coordinator_id,
                              max(0.0, (time.time() - msg.sent_at) * 1000.0))
        return resp

    def handle_status_sync(self, msg: StatusSyncMessage) -> Dict[str, Any]:
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        if msg.sender_id == rt.coord.coordinator_id and msg.epoch >= rt.coord.epoch:
            rt.synced_membership = msg.membership
            rt.synced_at = time.time()
            return {"accepted": True}
        return {"accepted": False, "reason": "not_my_coordinator", "coordinator_id": rt.coord.coordinator_id,
                "epoch": rt.coord.epoch}

    def _sampled_log(self, event_type: str, peer: int, latency_ms: float) -> None:
        every = max(1, self.rt.timing.heartbeat_log_every)
        key = (event_type, peer)
        self._received[key] += 1
        if self._received[key] % every == 1 or every == 1:
            self.rt.events.node_event(event_type, peer, quiet=True, latency_ms=round(latency_ms, 2),
                                      count=self._received[key])
