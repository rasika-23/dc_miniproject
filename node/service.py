"""One node: wires every manager together and exposes them through FastAPI."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI

from communication.http_client import MessageCounters, PeerClient
from communication.peer_registry import PeerRegistry
from election.bully import BullyElection
from election.coordinator_state import CoordinatorState
from election.election_manager import ElectionManager
from election.leader_discovery import discover_async
from fault_injection.heartbeat_faults import ActiveFault, FaultInjectionManager
from monitoring.failure_detector import FailureDetector
from monitoring.heartbeat_receiver import HeartbeatReceiver
from monitoring.heartbeat_sender import HeartbeatSender
from monitoring.membership_registry import MembershipRegistry
from monitoring.recovery_manager import RecoveryManager
from node.config import ClusterConfig
from node.node_state import NodeState, Role
from storage.database import Database
from storage.event_repository import EventLogger, EventRepository


class NodeRuntime:
    """All state and background tasks of a single node process."""

    def __init__(self, config: ClusterConfig, node_id: int):
        self.cfg = config
        self.timing = config.timing
        spec = config.node(node_id)
        self.log = logging.getLogger(f"dmp.node{node_id}")
        self.registry = PeerRegistry(config, node_id)
        self.state = NodeState(node_id=node_id, priority=spec.priority, host=spec.host, port=spec.port)
        self.coord = CoordinatorState()

        self.db = Database(config.node_db_path(node_id))
        self.repo = EventRepository(self.db)
        # epochs survive restarts through the local event journal
        self.coord.highest_epoch_seen = self.repo.max_epoch()
        self.events = EventLogger(self.repo, node_id, self.state.incarnation,
                                  epoch_provider=lambda: self.coord.epoch, name=f"node{node_id}")

        self.faults = FaultInjectionManager(enabled=config.faults_enabled, on_start=self._fault_started,
                                            on_end=self._fault_ended)
        self.counters = MessageCounters()
        self.client = PeerClient(self.registry, faults=self.faults, timeout=self.timing.request_timeout,
                                 counters=self.counters)
        self.membership = MembershipRegistry(self.registry, node_id, self.timing)
        self.bully = BullyElection(node_id=node_id, priority=spec.priority, registry=self.registry,
                                   client=self.client, state=self.state, coord=self.coord,
                                   election_timeout=self.timing.election_timeout)
        self.election = ElectionManager(self)
        self.sender = HeartbeatSender(self)
        self.receiver = HeartbeatReceiver(self)
        self.detector = FailureDetector(self)
        self.recovery = RecoveryManager(self)

        self.synced_membership: List[Dict[str, Any]] = []
        self.synced_at: Optional[float] = None
        self.stopping = False
        self._tasks: set[asyncio.Task] = set()
        self._fault_rows: Dict[str, str] = {}

    # ------------------------------------------------------------------ task helpers
    def spawn(self, coro) -> asyncio.Task:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.log.error("background task failed", exc_info=task.exception())

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        self.events.node_event("NODE_STARTED", incarnation=self.state.incarnation, port=self.state.port,
                               priority=self.state.priority, recovered_epoch=self.coord.highest_epoch_seen)
        self.spawn(self.sender.run_worker_loop())
        self.spawn(self.sender.run_coordinator_loop())
        self.spawn(self.detector.run())
        self.spawn(self._startup())

    async def stop(self) -> None:
        """Abrupt stop (no goodbye messages): peers must detect it through missing heartbeats."""
        self.stopping = True
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*list(self._tasks), return_exceptions=True)
        await self.client.close()
        self.db.close()

    async def _startup(self) -> None:
        """Discover peers; follow an existing higher-priority coordinator, otherwise elect."""
        await asyncio.sleep(self.timing.startup_grace)
        if self.election.in_progress or self.coord.has_coordinator:
            self.state.startup_done = True
            return
        disc = await discover_async(self.client, self.registry)
        self.coord.observe_epoch(disc.highest_epoch)
        self.events.node_event("PEERS_DISCOVERED", reachable=disc.reachable, coordinator=disc.coordinator_id,
                               highest_epoch=disc.highest_epoch, quiet=True)
        if disc.coordinator_id is not None and self.registry.priority(disc.coordinator_id) > self.state.priority:
            self.state.startup_done = True
            await self.election.handle_coordinator_claim(disc.coordinator_id, disc.epoch, source="discovery")
            if self.coord.has_coordinator:
                return
        self.election.start_election("startup")

    # ------------------------------------------------------------------ faults
    def _fault_started(self, f: ActiveFault) -> None:
        params = {"delay_ms": f.delay_ms, "loss_pct": f.loss_pct, "scope": f.scope,
                  "expires_at": f.expires_at}
        self._fault_rows[f.fault_id] = self.repo.start_fault(
            fault_type=f.kind, target_node_id=self.state.node_id, params=params,
            experiment_id=f.experiment_id, source="node", start_ts=f.started_at, fault_id=f.fault_id)
        self.events.node_event("FAULT_APPLIED", self.state.node_id, fault=f.kind, **params)

    def _fault_ended(self, f: ActiveFault, reason: str) -> None:
        self.repo.end_fault(f.fault_id)
        self.events.node_event("FAULT_CLEARED", self.state.node_id, fault=f.kind, reason=reason)

    # ------------------------------------------------------------------ views
    def health_payload(self) -> Dict[str, Any]:
        return {
            "status": "ok", "node_id": self.state.node_id, "priority": self.state.priority,
            "port": self.state.port, "role": self.state.role.value, "incarnation": self.state.incarnation,
            "started_at": self.state.started_at, "uptime_s": round(self.state.uptime, 3),
            "epoch": self.coord.epoch, "highest_epoch_seen": self.coord.highest_epoch_seen,
            "coordinator_id": self.coord.coordinator_id, "coordinator_since": self.coord.since,
            "election_in_progress": self.election.in_progress, "registered": self.coord.registered,
            "coordinator_silence_s": round(self.coord.silence(), 3) if self.coord.has_coordinator else None,
            "faults": self.faults.active(), "time": time.time(),
        }

    def status_payload(self) -> Dict[str, Any]:
        payload = self.health_payload()
        if self.state.role == Role.COORDINATOR and self.membership.active:
            payload["membership"] = self.membership.snapshot(self.state.node_id)
            payload["membership_source"] = "authoritative"
        else:
            payload["membership"] = self.synced_membership
            payload["membership_source"] = "synced"
            payload["membership_synced_at"] = self.synced_at
        payload["timing"] = {k: getattr(self.timing, k) for k in (
            "heartbeat_interval", "suspicion_timeout", "failure_confirmation",
            "coordinator_heartbeat_interval", "coordinator_timeout")}
        return payload

    def metrics_payload(self) -> Dict[str, Any]:
        return {"node_id": self.state.node_id, "incarnation": self.state.incarnation, "role": self.state.role.value,
                "epoch": self.coord.epoch, "counters": self.counters.snapshot(),
                "elections_started": self.election.elections_started, "time": time.time()}


def create_app(runtime: NodeRuntime) -> FastAPI:
    from api.routes import router

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title=f"Node {runtime.state.node_id}", lifespan=lifespan,
                  docs_url="/docs", redoc_url=None)
    app.state.runtime = runtime
    app.include_router(router)
    return app
