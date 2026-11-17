"""Failure detection.

* Coordinator side: workers go HEALTHY -> SUSPECTED -> FAILED when heartbeats stop.
* Worker side: every node independently watches the coordinator and starts an election when
  COORDINATOR_HEARTBEAT messages stop arriving.
"""
from __future__ import annotations

import asyncio
import time

from monitoring.heartbeat_receiver import log_transitions
from node.node_state import Role


class FailureDetector:
    def __init__(self, rt):
        self.rt = rt

    async def run(self) -> None:
        rt = self.rt
        while True:
            await asyncio.sleep(rt.timing.detector_tick)
            try:
                rt.faults.expire()
                if rt.state.role == Role.COORDINATOR and rt.membership.active:
                    log_transitions(rt, rt.membership.check_timeouts())
                else:
                    self._watch_coordinator()
            except asyncio.CancelledError:
                raise
            except Exception:
                rt.log.exception("failure detector tick failed")

    def _watch_coordinator(self) -> None:
        rt = self.rt
        if not rt.state.startup_done or rt.stopping or rt.election.in_progress:
            return
        if rt.coord.coordinator_id is not None:
            silence = rt.coord.silence()
            if silence > rt.timing.coordinator_timeout:
                lost = rt.coord.coordinator_id
                rt.events.node_event("COORDINATOR_TIMEOUT", lost, silent_for_s=round(silence, 3),
                                     timeout_s=rt.timing.coordinator_timeout, epoch_lost=rt.coord.epoch)
                rt.coord.clear()
                rt.spawn(self._staggered_election())
        elif time.monotonic() - rt.election.last_finished > max(rt.timing.election_timeout, 1.0):
            rt.election.start_election("no_coordinator")

    async def _staggered_election(self) -> None:
        """Higher-priority nodes elect first, so lower ones usually find an announcement waiting."""
        rt = self.rt
        stagger = rt.timing.election_stagger * max(0, rt.cfg.max_priority - rt.state.priority)
        if stagger > 0:
            await asyncio.sleep(stagger)
        if rt.coord.coordinator_id is None:
            rt.election.start_election("coordinator_timeout")
