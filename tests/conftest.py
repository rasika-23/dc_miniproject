"""Test harness: runs real node services (FastAPI + uvicorn + HTTP) inside one asyncio loop.

Nodes talk to each other over real localhost HTTP, but with short timeouts so a whole failover
takes about a second. "Crashing" a node shuts its server down without any goodbye message.
"""
from __future__ import annotations

import asyncio
import socket
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

import httpx
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from node.config import ClusterConfig, NodeSpec, Timing  # noqa: E402
from node.service import NodeRuntime, create_app  # noqa: E402

FAST = Timing(heartbeat_interval=0.15, suspicion_timeout=0.6, failure_confirmation=0.4,
              coordinator_heartbeat_interval=0.15, coordinator_timeout=0.8, election_timeout=0.4,
              coordinator_wait_timeout=0.8, election_max_retries=3, election_stagger=0.05, startup_grace=0.3,
              request_timeout=0.5, detector_tick=0.1, status_sync_interval=0.5, recovery_validation_beats=2,
              heartbeat_log_every=5)


def run(coro: Awaitable) -> Any:
    return asyncio.run(coro)


def _free_ports(n: int) -> List[int]:
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


async def wait_until(fn: Callable[[], Any], timeout: float = 10.0, interval: float = 0.05) -> Any:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if asyncio.iscoroutine(last):
            last = await last
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s (last value: {last!r})")


class InProcessCluster:
    def __init__(self, tmp_path: Path, n: int = 5, timing: Optional[Timing] = None):
        ports = _free_ports(n)
        self.cfg = ClusterConfig(nodes=[NodeSpec(i + 1, i + 1, "127.0.0.1", ports[i]) for i in range(n)],
                                 timing=timing or FAST, data_dir=tmp_path / "data", runtime_dir=tmp_path / "rt")
        self.runtimes: Dict[int, NodeRuntime] = {}
        self._servers: Dict[int, uvicorn.Server] = {}
        self._tasks: Dict[int, asyncio.Task] = {}
        self.http = httpx.AsyncClient(timeout=2.0)

    def url(self, node_id: int, path: str = "") -> str:
        return self.cfg.node(node_id).base_url + path

    # ---------------------------------------------------------------- lifecycle
    async def start(self, node_id: int) -> NodeRuntime:
        rt = NodeRuntime(self.cfg, node_id)
        spec = self.cfg.node(node_id)
        server = uvicorn.Server(uvicorn.Config(create_app(rt), host=spec.host, port=spec.port, log_level="critical",
                                               access_log=False, timeout_graceful_shutdown=1))
        task = asyncio.get_running_loop().create_task(server.serve())
        self.runtimes[node_id], self._servers[node_id], self._tasks[node_id] = rt, server, task
        await wait_until(lambda: server.started, timeout=10)
        return rt

    async def start_all(self, ids: Optional[Iterable[int]] = None) -> None:
        for i in ids or self.cfg.node_ids:
            await self.start(i)

    async def crash(self, node_id: int) -> float:
        """Stop a node abruptly. Returns the ground-truth crash time."""
        ts = time.time()
        server, task = self._servers.pop(node_id), self._tasks.pop(node_id)
        server.should_exit = True
        try:
            await asyncio.wait_for(task, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            task.cancel()
        self.runtimes.pop(node_id, None)
        return ts

    async def stop_all(self) -> None:
        for i in list(self._servers):
            await self.crash(i)
        await self.http.aclose()

    # ---------------------------------------------------------------- observation
    async def get(self, node_id: int, path: str) -> Optional[dict]:
        try:
            r = await self.http.get(self.url(node_id, path))
            return r.json()
        except httpx.HTTPError:
            return None

    async def post(self, node_id: int, path: str, body: dict) -> httpx.Response:
        return await self.http.post(self.url(node_id, path), json=body)

    def live(self) -> List[int]:
        return sorted(self.runtimes)

    async def views(self) -> Dict[int, dict]:
        out = {}
        for i in self.live():
            h = await self.get(i, "/health")
            if h:
                out[i] = h
        return out

    async def agreed_coordinator(self, expected: Optional[int] = None, min_epoch: int = 0) -> Optional[int]:
        """Coordinator id if every live node agrees on it (and it acts as coordinator), else None."""
        views = await self.views()
        if not views or any(v["election_in_progress"] for v in views.values()):
            return None
        ids = {v["coordinator_id"] for v in views.values()}
        if len(ids) != 1 or None in ids:
            return None
        cid = ids.pop()
        if cid not in views or views[cid]["role"] != "coordinator" or views[cid]["epoch"] < min_epoch:
            return None
        if expected is not None and cid != expected:
            return None
        return cid

    async def membership(self) -> Dict[int, dict]:
        for i in self.live():
            st = await self.get(i, "/cluster/status")
            if st and st["role"] == "coordinator" and st["membership_source"] == "authoritative":
                return {m["node_id"]: m for m in st["membership"]}
        return {}

    async def health_of(self, node_id: int) -> Optional[str]:
        m = await self.membership()
        return m[node_id]["health"] if node_id in m else None

    async def events(self, node_id: int, since: float = 0.0) -> Dict[str, List[dict]]:
        return await self.get(node_id, f"/cluster/events?since={since}&limit=5000") or {"node_events": [], "election_events": []}

    async def all_events(self, since: float = 0.0) -> Dict[str, List[dict]]:
        node_ev, elec_ev = {}, {}
        for i in self.live():
            ev = await self.events(i, since)
            node_ev.update({e["event_id"]: e for e in ev["node_events"]})
            elec_ev.update({e["event_id"]: e for e in ev["election_events"]})
        return {"node_events": sorted(node_ev.values(), key=lambda e: e["ts"]),
                "election_events": sorted(elec_ev.values(), key=lambda e: e["ts"])}


@asynccontextmanager
async def running_cluster(tmp_path: Path, ids: Optional[Iterable[int]] = None, n: int = 5):
    cl = InProcessCluster(tmp_path, n=n)
    try:
        await cl.start_all(ids)
        yield cl
    finally:
        await cl.stop_all()
