"""Periodic heartbeat tasks.

Workers send HEARTBEAT to the coordinator; the coordinator sends COORDINATOR_HEARTBEAT (and
periodic STATUS_SYNC) to every peer. Each message is sent from its own task, so an injected
delay behaves like network latency instead of slowing the heartbeat loop itself.
"""
from __future__ import annotations

import asyncio
import time

from communication.http_client import PeerError
from communication.messages import (CoordinatorHeartbeatMessage, HeartbeatMessage, StatusSyncMessage)
from node.node_state import Role


class HeartbeatSender:
    def __init__(self, rt):
        self.rt = rt
        self._seq = 0
        self._last_sync = 0.0

    # ----------------------------------------------------------------- worker side
    async def run_worker_loop(self) -> None:
        rt = self.rt
        while True:
            await asyncio.sleep(rt.timing.heartbeat_interval)
            try:
                await self._worker_tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                rt.log.exception("worker heartbeat tick failed")

    async def _worker_tick(self) -> None:
        rt = self.rt
        if not rt.state.startup_done or rt.stopping:
            return
        cid = rt.coord.coordinator_id
        if cid is None or cid == rt.state.node_id:
            return
        if not rt.coord.registered:
            await rt.recovery.register()          # registration doubles as the first liveness proof
            return
        self._seq += 1
        rt.spawn(self._send_heartbeat(cid, self._seq))

    async def _send_heartbeat(self, cid: int, seq: int) -> None:
        rt = self.rt
        msg = HeartbeatMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation,
                               epoch=rt.coord.epoch, seq=seq)
        try:
            resp = await rt.client.send(cid, "/heartbeat", msg)
        except PeerError:
            return
        if not resp.get("accepted"):
            if resp.get("reason") == "not_coordinator" and rt.coord.coordinator_id == cid:
                # the node we follow no longer acts as coordinator
                await rt.election.consider_hint(resp.get("coordinator_id"), resp.get("epoch", 0))
                if rt.coord.coordinator_id == cid:
                    rt.coord.clear()
                    rt.election.start_election("stale_coordinator")
            return
        if resp.get("need_register"):
            rt.coord.registered = False

    # ----------------------------------------------------------------- coordinator side
    async def run_coordinator_loop(self) -> None:
        rt = self.rt
        while True:
            await asyncio.sleep(rt.timing.coordinator_heartbeat_interval)
            try:
                self._coordinator_tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                rt.log.exception("coordinator heartbeat tick failed")

    def _coordinator_tick(self) -> None:
        rt = self.rt
        if rt.state.role != Role.COORDINATOR or rt.stopping:
            return
        self._seq += 1
        lease = time.time() + rt.timing.coordinator_timeout
        for peer in rt.registry.peers():
            msg = CoordinatorHeartbeatMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation,
                                              epoch=rt.coord.epoch, coordinator_id=rt.state.node_id,
                                              lease_expires_at=lease, seq=self._seq)
            rt.spawn(self._send_coordinator_heartbeat(peer.id, msg))
        if time.monotonic() - self._last_sync >= rt.timing.status_sync_interval:
            self._last_sync = time.monotonic()
            snapshot = rt.membership.snapshot(rt.state.node_id)
            for peer in rt.registry.peers():
                sync = StatusSyncMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation,
                                         epoch=rt.coord.epoch, coordinator_id=rt.state.node_id, membership=snapshot)
                rt.spawn(self._send_status_sync(peer.id, sync))

    async def _send_coordinator_heartbeat(self, peer_id: int, msg: CoordinatorHeartbeatMessage) -> None:
        try:
            resp = await self.rt.client.send(peer_id, "/coordinator-heartbeat", msg)
        except PeerError:
            return
        if not resp.get("accepted"):
            await self.rt.election.consider_hint(resp.get("coordinator_id"), resp.get("epoch", 0))

    async def _send_status_sync(self, peer_id: int, msg: StatusSyncMessage) -> None:
        try:
            await self.rt.client.send(peer_id, "/status-sync", msg)
        except PeerError:
            return
