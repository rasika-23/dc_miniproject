"""Election lifecycle: triggers, timeouts, retries, announcements and stepping down."""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, Dict, Optional

from communication.http_client import PeerError
from communication.messages import CoordinatorMessage, ElectionMessage, OkMessage
from election.bully import evaluate_announcement, next_epoch
from node.node_state import Role


class ElectionManager:
    def __init__(self, rt):
        self.rt = rt
        self.in_progress = False
        self.last_finished = 0.0
        self.elections_started = 0
        self._announce_seq = 0
        self._coordinator_event = asyncio.Event()
        self._last_stale_log = 0.0

    # ================================================================== triggers
    def start_election(self, reason: str) -> bool:
        """Begin an election in the background. No-op if one is already running."""
        rt = self.rt
        rt.state.startup_done = True
        if self.in_progress or rt.stopping:
            return False
        self.in_progress = True
        self.elections_started += 1
        rt.spawn(self._run_election(reason))
        return True

    # ================================================================== election run
    async def _run_election(self, reason: str) -> None:
        rt = self.rt
        election_id = f"{rt.state.node_id}-{uuid.uuid4().hex[:8]}"
        t_start = time.time()
        seq_at_start = self._announce_seq
        try:
            if rt.coord.coordinator_id is None and rt.state.role != Role.COORDINATOR:
                self._set_role(Role.CANDIDATE, f"election:{reason}")
            rt.events.election_event("ELECTION_STARTED", election_id, reason=reason,
                                     previous_coordinator=rt.coord.previous_coordinator_id
                                     if rt.coord.coordinator_id is None else rt.coord.coordinator_id,
                                     epoch_seen=rt.coord.highest_epoch_seen)
            for attempt in range(1, rt.timing.election_max_retries + 1):
                if self._announce_seq != seq_at_start:
                    break                                 # someone else already announced
                self._coordinator_event.clear()
                result = await rt.bully.run_round(election_id)
                if self._announce_seq != seq_at_start:
                    break
                if result.won:
                    await self._win(election_id, t_start, reason, result)
                    return
                rt.events.election_event("OK_RECEIVED", election_id, ok_from=result.ok_from, attempt=attempt)
                try:
                    await asyncio.wait_for(self._coordinator_event.wait(), timeout=rt.timing.coordinator_wait_timeout)
                    break                                 # a higher node announced itself
                except asyncio.TimeoutError:
                    rt.events.election_event("ELECTION_RETRY", election_id, attempt=attempt,
                                             waited_for=result.ok_from)
            else:
                rt.events.election_event("ELECTION_FAILED", election_id, attempts=rt.timing.election_max_retries)
                if rt.coord.coordinator_id is None and rt.state.role == Role.CANDIDATE:
                    self._set_role(Role.WORKER, "election_failed")
                return
            rt.events.election_event("ELECTION_LOST", election_id,
                                     duration_ms=(time.time() - t_start) * 1000.0,
                                     winner=rt.coord.coordinator_id)
        finally:
            self.in_progress = False
            self.last_finished = time.monotonic()

    async def _win(self, election_id: str, t_start: float, reason: str, result) -> None:
        rt = self.rt
        previous = rt.coord.previous_coordinator_id if rt.coord.coordinator_id is None else rt.coord.coordinator_id
        new_epoch = next_epoch(rt.coord.highest_epoch_seen)
        old_role = rt.state.role
        rt.coord.set(rt.state.node_id, rt.state.priority, new_epoch)
        rt.coord.registered = True
        rt.membership.reset(rt.state.incarnation)
        rt.state.role = Role.COORDINATOR
        if old_role != Role.COORDINATOR:
            rt.events.node_event("ROLE_CHANGED", **{"from": old_role.value, "to": Role.COORDINATOR.value},
                                 reason=f"election:{reason}")
        rt.events.node_event("COORDINATOR_CHANGED", rt.state.node_id, old=previous, new=rt.state.node_id,
                             epoch=new_epoch, reason=reason)
        rt.events.election_event("ELECTION_WON", election_id, epoch=new_epoch,
                                 duration_ms=(time.time() - t_start) * 1000.0, reason=reason,
                                 previous_coordinator=previous, unreachable=result.contacted)
        msg = CoordinatorMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation, epoch=new_epoch,
                                 coordinator_id=rt.state.node_id, coordinator_priority=rt.state.priority,
                                 election_id=election_id)
        sends = [(p.id, rt.spawn(rt.client.send(p.id, "/coordinator", msg))) for p in rt.registry.peers()]
        rt.events.election_event("COORDINATOR_ANNOUNCED", election_id, epoch=new_epoch,
                                 duration_ms=(time.time() - t_start) * 1000.0, previous_coordinator=previous)
        results = await asyncio.gather(*[t for _, t in sends], return_exceptions=True)
        for r in results:
            if isinstance(r, dict) and not r.get("accepted", True):
                await self.consider_hint(r.get("coordinator_id"), r.get("epoch", 0))

    # ================================================================== incoming messages
    async def handle_election(self, msg: ElectionMessage) -> Dict[str, Any]:
        """A lower-priority node asks us to take over. Answer OK and (usually) run our own election."""
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        if rt.registry.priority(msg.sender_id) >= rt.state.priority:
            return {"received": False, "reason": "sender_not_lower_priority"}
        rt.events.election_event("ELECTION_RECEIVED", msg.election_id, from_node=msg.sender_id)
        rt.spawn(self._reply_ok(msg))
        if rt.state.role == Role.COORDINATOR:
            # We already lead: re-assert (same epoch) instead of bumping the epoch for everyone.
            rt.spawn(self._reassert(msg.sender_id, msg.election_id))
        else:
            self.start_election("election_request")
        return {"received": True}

    def handle_ok(self, msg: OkMessage) -> Dict[str, Any]:
        rt = self.rt
        rt.coord.observe_epoch(msg.epoch)
        matched = rt.bully.record_ok(msg.election_id, msg.sender_id)
        return {"accepted": matched}

    async def handle_coordinator_claim(self, coordinator_id: int, epoch: int, *, source: str = "announcement",
                                       election_id: Optional[str] = None,
                                       lease_expires_at: float = 0.0) -> Dict[str, Any]:
        """Common path for COORDINATOR, COORDINATOR_HEARTBEAT and discovery hints."""
        rt = self.rt
        rt.coord.observe_epoch(epoch)
        if coordinator_id not in rt.registry:
            return self._view(False, "unknown_coordinator")
        ann_priority = rt.registry.priority(coordinator_id)
        decision = evaluate_announcement(
            my_id=rt.state.node_id, my_priority=rt.state.priority,
            cur_coordinator_id=rt.coord.coordinator_id, cur_coordinator_priority=rt.coord.coordinator_priority,
            cur_epoch=rt.coord.epoch, ann_coordinator_id=coordinator_id, ann_priority=ann_priority, ann_epoch=epoch)
        if decision.accept:
            if rt.coord.coordinator_id == coordinator_id and epoch == rt.coord.epoch:
                rt.coord.touch(lease_expires_at)          # routine heartbeat from the known coordinator
                if self.in_progress and rt.state.role != Role.COORDINATOR:
                    # our election was unnecessary: the coordinator is alive and has just confirmed it
                    self._announce_seq += 1
                    self._coordinator_event.set()
            else:
                await self._adopt(coordinator_id, ann_priority, epoch, election_id, decision.reason, source)
                rt.coord.touch(lease_expires_at)
            return self._view(True, decision.reason)
        if decision.reason == "lower_priority_than_receiver":
            rt.events.node_event("LOWER_PRIORITY_CLAIM_REJECTED", coordinator_id, claim_epoch=epoch, source=source)
            self.start_election("lower_priority_claim")
        elif decision.reason == "stale_epoch" and time.monotonic() - self._last_stale_log > 2.0:
            self._last_stale_log = time.monotonic()
            rt.events.node_event("STALE_MESSAGE_REJECTED", coordinator_id, reason="stale_epoch", msg_epoch=epoch,
                                 my_epoch=rt.coord.epoch, source=source, quiet=True)
        return self._view(False, decision.reason)

    async def consider_hint(self, coordinator_id: Optional[int], epoch: int) -> None:
        """A peer's reply told us who it follows. Adopt it if it beats what we know."""
        if coordinator_id is None or coordinator_id == self.rt.state.node_id:
            return
        await self.handle_coordinator_claim(coordinator_id, epoch, source="reply")

    # ================================================================== helpers
    async def _adopt(self, coordinator_id: int, priority: int, epoch: int, election_id: Optional[str],
                     reason: str, source: str) -> None:
        rt = self.rt
        old, old_role = rt.coord.coordinator_id, rt.state.role
        rt.coord.set(coordinator_id, priority, epoch)
        if old_role in (Role.COORDINATOR, Role.CANDIDATE):
            self._set_role(Role.WORKER, f"accepted_coordinator_{coordinator_id}")
        if old_role == Role.COORDINATOR:
            rt.membership.deactivate()
            rt.events.node_event("STEPPED_DOWN", coordinator_id, epoch=epoch, reason=reason)
        rt.events.node_event("COORDINATOR_CHANGED", coordinator_id, old=old, new=coordinator_id, epoch=epoch,
                             reason=reason, source=source)
        rt.events.election_event("COORDINATOR_ACCEPTED", election_id, epoch=epoch, coordinator=coordinator_id,
                                 source=source)
        self._announce_seq += 1
        self._coordinator_event.set()
        rt.spawn(rt.recovery.register())

    def _set_role(self, role: Role, reason: str) -> None:
        old = self.rt.state.role
        if old == role:
            return
        self.rt.state.role = role
        self.rt.events.node_event("ROLE_CHANGED", **{"from": old.value, "to": role.value}, reason=reason)

    def _view(self, accepted: bool, reason: str) -> Dict[str, Any]:
        c = self.rt.coord
        return {"accepted": accepted, "reason": reason, "coordinator_id": c.coordinator_id, "epoch": c.epoch}

    async def _reply_ok(self, msg: ElectionMessage) -> None:
        rt = self.rt
        ok = OkMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation, epoch=rt.coord.epoch,
                       election_id=msg.election_id)
        try:
            await rt.client.send(msg.sender_id, "/election/ok", ok)
        except PeerError:
            pass

    async def _reassert(self, peer_id: int, election_id: str) -> None:
        rt = self.rt
        msg = CoordinatorMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation, epoch=rt.coord.epoch,
                                 coordinator_id=rt.state.node_id, coordinator_priority=rt.state.priority,
                                 election_id=election_id)
        try:
            await rt.client.send(peer_id, "/coordinator", msg)
        except PeerError:
            pass
