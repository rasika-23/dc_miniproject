"""Which node probes which service.

Goals: every service is probed by `replication` different nodes, the load is spread evenly
(no node carries more than one service more than any other), and when the set of eligible
nodes changes as little work as possible moves.

How: keep every previous (service, node) pair that is still valid, fill the missing slots with
the least-loaded node (rendezvous hash as the deterministic tie-break), then move single
services from the busiest to the idlest node until the spread is at most one.
"""
from __future__ import annotations

import hashlib
from typing import Dict, Iterable, List, Optional


def _score(service_id: str, node_id: int) -> int:
    return int.from_bytes(hashlib.sha256(f"{service_id}|{node_id}".encode()).digest()[:8], "big")


def assign(service_ids: Iterable[str], nodes: Iterable[int], replication: int = 2,
           previous: Optional[Dict[int, List[str]]] = None) -> Dict[int, List[str]]:
    """Return {node_id: [service ids]} for the given eligible nodes."""
    nodes = sorted(set(nodes))
    services = sorted(set(service_ids))
    out: Dict[int, List[str]] = {n: [] for n in nodes}
    if not nodes:
        return out
    r = max(1, min(replication, len(nodes)))
    load = {n: 0 for n in nodes}
    holders: Dict[str, List[int]] = {sid: [] for sid in services}

    prev_by_service: Dict[str, List[int]] = {}
    for n, sids in (previous or {}).items():
        for sid in sids:
            prev_by_service.setdefault(sid, []).append(n)
    for sid in services:                                    # 1. keep what is still valid
        for n in sorted(prev_by_service.get(sid, [])):
            if n in load and len(holders[sid]) < r:
                holders[sid].append(n)
                load[n] += 1
    for sid in services:                                    # 2. fill missing slots with the least-loaded nodes
        while len(holders[sid]) < r:
            cands = [n for n in nodes if n not in holders[sid]]
            pick = min(cands, key=lambda n: (load[n], -_score(sid, n)))
            holders[sid].append(pick)
            load[pick] += 1
    for _ in range(len(services) * r + 1):                  # 3. even out the spread
        busiest = max(nodes, key=lambda n: (load[n], n))
        idlest = min(nodes, key=lambda n: (load[n], n))
        if load[busiest] - load[idlest] <= 1:
            break
        movable = [sid for sid in services if busiest in holders[sid] and idlest not in holders[sid]]
        if not movable:
            break
        sid = max(movable, key=lambda s: _score(s, idlest))
        holders[sid].remove(busiest)
        holders[sid].append(idlest)
        load[busiest] -= 1
        load[idlest] += 1
    for sid, hs in holders.items():
        for n in hs:
            out[n].append(sid)
    return {n: sorted(v) for n, v in out.items()}


def probers_of(assignment: Dict[int, List[str]], service_id: str) -> List[int]:
    return sorted(n for n, sids in assignment.items() if service_id in sids)


def diff(old: Dict[int, List[str]], new: Dict[int, List[str]]) -> Dict[str, Dict[str, List[int]]]:
    """Per service: which nodes started / stopped probing it."""
    changes: Dict[str, Dict[str, List[int]]] = {}
    services = {s for v in old.values() for s in v} | {s for v in new.values() for s in v}
    for sid in sorted(services):
        before, after = set(probers_of(old, sid)), set(probers_of(new, sid))
        if before != after:
            changes[sid] = {"added": sorted(after - before), "removed": sorted(before - after)}
    return changes
