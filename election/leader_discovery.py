"""Find the active coordinator by asking the configured node addresses.

Used by nodes at startup (async) and by the dashboard / experiment controller (sync).
Nothing here assumes a particular node is the coordinator.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from communication.peer_registry import PeerRegistry

STATE_OPERATIONAL = "operational"
STATE_ELECTING = "election_in_progress"
STATE_NO_COORDINATOR = "no_coordinator"
STATE_UNAVAILABLE = "unavailable"
STATE_SPLIT = "split_brain"


@dataclass
class Discovery:
    health: Dict[int, Optional[dict]]
    coordinator_id: Optional[int] = None
    epoch: int = 0
    conflicting: List[int] = field(default_factory=list)
    state: str = STATE_UNAVAILABLE
    reachable: List[int] = field(default_factory=list)
    highest_epoch: int = 0
    probed_at: float = field(default_factory=time.time)

    @property
    def has_coordinator(self) -> bool:
        return self.coordinator_id is not None

    def to_dict(self) -> Dict[str, Any]:
        return {"coordinator_id": self.coordinator_id, "epoch": self.epoch, "state": self.state,
                "reachable": self.reachable, "conflicting": self.conflicting, "highest_epoch": self.highest_epoch,
                "probed_at": self.probed_at}


def classify(health: Dict[int, Optional[dict]]) -> Discovery:
    """Turn raw /health responses into a cluster-level verdict (pure function)."""
    reachable = sorted(i for i, h in health.items() if h)
    d = Discovery(health=health, reachable=reachable, probed_at=time.time())
    if not reachable:
        d.state = STATE_UNAVAILABLE
        return d
    d.highest_epoch = max(int(health[i].get("epoch", 0) or 0) for i in reachable)
    claimants = [i for i in reachable if health[i].get("role") == "coordinator"
                 and health[i].get("coordinator_id") == i]
    if claimants:
        claimants.sort(key=lambda i: (health[i].get("epoch", 0), health[i].get("priority", 0)), reverse=True)
        d.coordinator_id = claimants[0]
        d.epoch = int(health[claimants[0]].get("epoch", 0))
        d.conflicting = claimants[1:]
        d.state = STATE_SPLIT if d.conflicting else STATE_OPERATIONAL
        return d
    electing = any(health[i].get("election_in_progress") or health[i].get("role") == "candidate" for i in reachable)
    d.state = STATE_ELECTING if electing else STATE_NO_COORDINATOR
    return d


async def discover_async(client, registry: PeerRegistry, timeout: float = 0.8) -> Discovery:
    """Async variant used by node processes (skips itself)."""
    import asyncio

    async def probe(node_id: int):
        try:
            return node_id, await client.get(node_id, "/health", timeout=timeout)
        except Exception:
            return node_id, None

    results = await asyncio.gather(*[probe(p.id) for p in registry.peers()])
    return classify(dict(results))


def discover_sync(registry: PeerRegistry, timeout: float = 0.8, http: Optional[httpx.Client] = None,
                  pool: Optional[ThreadPoolExecutor] = None) -> Discovery:
    """Sync variant used by the dashboard and the experiment controller."""
    own_http = http is None
    client = http or httpx.Client(timeout=timeout)

    def probe(node_id: int):
        try:
            r = client.get(registry.url(node_id, "/health"), timeout=timeout)
            r.raise_for_status()
            return node_id, r.json()
        except Exception:
            return node_id, None

    try:
        nodes = registry.all()
        if pool is not None:
            results = list(pool.map(lambda n: probe(n.id), nodes))
        else:
            with ThreadPoolExecutor(max_workers=len(nodes)) as ex:
                results = list(ex.map(lambda n: probe(n.id), nodes))
        return classify(dict(results))
    finally:
        if own_http:
            client.close()
