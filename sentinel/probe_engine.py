"""Runs on EVERY node: executes the health checks assigned to this node and reports results.

* The coordinator tells each node which services to probe (STATUS_SYNC / REGISTER reply).
* A node keeps probing its last assignment even while there is no coordinator (during a failover),
  buffering results and flushing them to the new coordinator, so the monitoring gap stays small.
"""
from __future__ import annotations

import asyncio
import hashlib
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx

from communication.http_client import PeerError
from communication.messages import ProbeReportMessage
from node.node_state import Role
from sentinel.probes import run_probe


class ProbeEngine:
    def __init__(self, rt):
        self.rt = rt
        self.settings = rt.cfg.sentinel
        self.specs = {s.id: s for s in rt.cfg.services}
        self.assigned: List[str] = []
        self.applied_key: Tuple[int, int] = (-1, -1)          # (epoch, assignment version)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._http: Optional[httpx.AsyncClient] = None
        self.buffer: Deque[Dict[str, Any]] = deque(maxlen=self.settings.buffer_max)
        self.dropped = 0
        self.probes_run = 0
        self._seq = 0

    # ------------------------------------------------------------------ assignment
    def apply_assignment(self, service_ids: List[str], key: Tuple[int, int], source: str = "sync") -> bool:
        """Start/stop probe loops so they match `service_ids`. Ignores stale assignments."""
        if key <= self.applied_key and key != (-1, -1):
            return False
        self.applied_key = key
        wanted = sorted(s for s in service_ids if s in self.specs)
        if wanted == self.assigned:
            return False
        added, removed = sorted(set(wanted) - set(self.assigned)), sorted(set(self.assigned) - set(wanted))
        self.assigned = wanted
        for sid in removed:
            t = self._tasks.pop(sid, None)
            if t:
                t.cancel()
        for sid in added:
            self._tasks[sid] = self.rt.spawn(self._probe_loop(sid))
        self.rt.events.node_event("ASSIGNMENT_APPLIED", added=added, removed=removed, services=wanted,
                                  version=key[1], source=source)
        return True

    # ------------------------------------------------------------------ probing
    async def _probe_loop(self, service_id: str) -> None:
        rt, spec = self.rt, self.specs[service_id]
        if self._http is None:
            self._http = httpx.AsyncClient()
        jitter = int(hashlib.sha256(f"{service_id}{rt.state.node_id}".encode()).hexdigest(), 16) % 1000 / 1000.0
        await asyncio.sleep(jitter * spec.interval_s)
        while not rt.stopping:
            try:
                result = await run_probe(self._http, spec, rt.state.node_id)
                self.probes_run += 1
                rt.sentinel_repo.add_probe_results([result], self.settings.results_retention)
                self._deliver(result)
            except asyncio.CancelledError:
                raise
            except Exception:
                rt.log.exception("probe of %s failed unexpectedly", service_id)
            await asyncio.sleep(spec.interval_s)

    def _deliver(self, result: Dict[str, Any]) -> None:
        rt = self.rt
        if rt.state.role == Role.COORDINATOR and rt.sentinel.active:
            rt.sentinel.ingest([result])
        else:
            if len(self.buffer) == self.buffer.maxlen:
                self.dropped += 1
            self.buffer.append(result)

    # ------------------------------------------------------------------ reporting
    async def report_loop(self) -> None:
        rt = self.rt
        while not rt.stopping:
            await asyncio.sleep(self.settings.report_interval_s)
            try:
                await self._flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                rt.log.exception("probe report failed")

    async def _flush(self) -> None:
        rt = self.rt
        if not self.buffer:
            return
        if rt.state.role == Role.COORDINATOR and rt.sentinel.active:
            batch = list(self.buffer)
            self.buffer.clear()
            rt.sentinel.ingest(batch)
            return
        cid = rt.coord.coordinator_id
        if cid is None or cid == rt.state.node_id or not rt.coord.registered:
            return
        batch = list(self.buffer)[:200]
        self._seq += 1
        msg = ProbeReportMessage(sender_id=rt.state.node_id, incarnation=rt.state.incarnation, epoch=rt.coord.epoch,
                                 results=batch, seq=self._seq)
        try:
            resp = await rt.client.send(cid, "/probe-report", msg)
        except PeerError:
            return
        if resp.get("accepted"):
            for _ in range(min(len(batch), len(self.buffer))):
                self.buffer.popleft()
        else:
            await rt.election.consider_hint(resp.get("coordinator_id"), resp.get("epoch", 0))

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
