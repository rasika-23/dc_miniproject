"""Bully election algorithm: pure decision rules plus one election round.

Rules in short:
  * A candidate sends ELECTION to every higher-priority node.
  * If nobody answers OK, the candidate wins and announces COORDINATOR with a new, higher epoch.
  * If someone answers OK, the candidate steps back and waits for that node's announcement.
Limitation: timeouts cannot tell a crashed node from a slow or partitioned one, and epochs only
reject *stale* announcements. This is not consensus and does not guarantee split-brain freedom.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Set, Tuple

from communication.messages import ElectionMessage
from communication.peer_registry import PeerRegistry
from node.config import NodeSpec


def higher_priority_peers(my_priority: int, peers: Iterable[NodeSpec]) -> List[NodeSpec]:
    return [p for p in peers if p.priority > my_priority]


def next_epoch(highest_epoch_seen: int) -> int:
    return highest_epoch_seen + 1


def claim_key(epoch: int, priority: int) -> Tuple[int, int]:
    """Total order on leadership claims: newer epoch wins, then higher priority."""
    return (epoch, priority)


@dataclass(frozen=True)
class AnnouncementDecision:
    accept: bool
    reason: str


def evaluate_announcement(*, my_id: int, my_priority: int, cur_coordinator_id: Optional[int],
                          cur_coordinator_priority: Optional[int], cur_epoch: int,
                          ann_coordinator_id: int, ann_priority: int, ann_epoch: int) -> AnnouncementDecision:
    """Decide whether to follow an announced coordinator (COORDINATOR or COORDINATOR_HEARTBEAT)."""
    if ann_coordinator_id == my_id:
        return AnnouncementDecision(False, "self")
    if ann_epoch < cur_epoch:
        return AnnouncementDecision(False, "stale_epoch")
    if ann_priority < my_priority:
        # A higher-priority node never follows a lower-priority claimant (Bully property).
        return AnnouncementDecision(False, "lower_priority_than_receiver")
    if cur_coordinator_id is None:
        return AnnouncementDecision(True, "no_coordinator")
    if cur_coordinator_id == ann_coordinator_id:
        return AnnouncementDecision(True, "same_coordinator")
    if ann_epoch > cur_epoch:
        return AnnouncementDecision(True, "newer_epoch")
    # equal epoch, different coordinator: the higher priority wins
    if cur_coordinator_priority is not None and ann_priority > cur_coordinator_priority:
        return AnnouncementDecision(True, "higher_priority_at_equal_epoch")
    return AnnouncementDecision(False, "equal_epoch_lower_priority")


@dataclass
class RoundResult:
    won: bool                                   # nobody higher answered: we may become coordinator
    contacted: List[int] = field(default_factory=list)
    delivered: List[int] = field(default_factory=list)
    ok_from: List[int] = field(default_factory=list)


class BullyElection:
    """Runs a single ELECTION round and tracks OK acknowledgements."""

    def __init__(self, *, node_id: int, priority: int, registry: PeerRegistry, client, state, coord,
                 election_timeout: float):
        self.node_id, self.priority = node_id, priority
        self.registry, self.client, self.state, self.coord = registry, client, state, coord
        self.election_timeout = election_timeout
        self._ok_events: dict[str, asyncio.Event] = {}
        self._ok_from: dict[str, Set[int]] = {}

    def record_ok(self, election_id: str, sender_id: int) -> bool:
        ev = self._ok_events.get(election_id)
        if ev is None:
            return False
        self._ok_from.setdefault(election_id, set()).add(sender_id)
        ev.set()
        return True

    async def run_round(self, election_id: str) -> RoundResult:
        higher = higher_priority_peers(self.priority, self.registry.peers())
        if not higher:
            return RoundResult(won=True)
        ev = self._ok_events[election_id] = asyncio.Event()
        self._ok_from[election_id] = set()
        msg_epoch = self.coord.highest_epoch_seen
        try:
            results = await asyncio.gather(
                *[self.client.send(p.id, "/election", ElectionMessage(
                    sender_id=self.node_id, incarnation=self.state.incarnation, epoch=msg_epoch,
                    election_id=election_id, candidate_priority=self.priority)) for p in higher],
                return_exceptions=True)
            delivered = [p.id for p, r in zip(higher, results) if not isinstance(r, BaseException)]
            if not delivered:
                # every higher-priority node is unreachable (or the messages were lost)
                return RoundResult(won=True, contacted=[p.id for p in higher])
            try:
                await asyncio.wait_for(ev.wait(), timeout=self.election_timeout)
            except asyncio.TimeoutError:
                return RoundResult(won=True, contacted=[p.id for p in higher], delivered=delivered)
            return RoundResult(won=False, contacted=[p.id for p in higher], delivered=delivered,
                               ok_from=sorted(self._ok_from.get(election_id, ())))
        finally:
            self._ok_events.pop(election_id, None)
            self._ok_from.pop(election_id, None)
