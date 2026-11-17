"""Joining, re-joining and health restoration.

Worker side: (re-)register with the current coordinator.
Coordinator side: handle REGISTER and move a returning node RECOVERING -> HEALTHY once it has
re-registered and delivered enough valid heartbeats (see membership_registry).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

from communication.http_client import PeerError
from communication.messages import RegisterMessage
from monitoring.heartbeat_receiver import log_transitions
from node.node_state import Role


class RecoveryManager:
    def __init__(self, rt):
        self.rt = rt
        self._lock = asyncio.Lock()

    async def register(self) -> bool:
        """Announce this node (and its incarnation) to the coordinator it follows."""
        rt = self.rt
        cid = rt.coord.coordinator_id
        if cid is None or cid == rt.state.node_id:
            return False
        async with self._lock:
            if rt.coord.coordinator_id != cid:
                return False
            if rt.coord.registered:
                return True
            msg = RegisterMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation,
                                  epoch=rt.coord.epoch, priority=rt.state.priority,
                                  host=rt.state.host, port=rt.state.port)
            try:
                resp = await rt.client.send(cid, "/register", msg)
            except PeerError:
                return False
            if resp.get("accepted"):
                if rt.coord.coordinator_id == cid:
                    rt.coord.registered = True
                    rt.synced_membership = resp.get("membership", rt.synced_membership)
                    rt.synced_at = time.time()
                return True
            await rt.election.consider_hint(resp.get("coordinator_id"), resp.get("epoch", 0))
            return False

    def handle_register(self, msg: RegisterMessage) -> Dict[str, Any]:
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        if rt.state.role != Role.COORDINATOR or not rt.membership.active:
            return {"accepted": False, "reason": "not_coordinator",
                    "coordinator_id": rt.coord.coordinator_id, "epoch": rt.coord.epoch}
        out = rt.membership.on_register(msg.sender_id, msg.incarnation)
        if not out.accepted:
            rt.events.node_event("STALE_MESSAGE_REJECTED", msg.sender_id, msg_type=msg.msg_type, reason=out.reason,
                                 msg_incarnation=msg.incarnation)
            return {"accepted": False, "reason": out.reason, "coordinator_id": rt.state.node_id,
                    "epoch": rt.coord.epoch}
        log_transitions(rt, out.transitions)
        return {"accepted": True, "coordinator_id": rt.state.node_id, "epoch": rt.coord.epoch,
                "health": out.health.value if out.health else None,
                "membership": rt.membership.snapshot(rt.state.node_id)}
