"""Which node this node currently follows, and in which epoch."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class CoordinatorState:
    coordinator_id: Optional[int] = None
    coordinator_priority: Optional[int] = None
    epoch: int = 0                   # epoch of the coordinator currently accepted
    highest_epoch_seen: int = 0      # highest epoch observed in any message (or recovered from disk)
    since: Optional[float] = None
    registered: bool = False         # registered with the current coordinator?
    last_heartbeat_mono: float = 0.0
    last_heartbeat_wall: float = 0.0
    lease_expires_at: float = 0.0
    previous_coordinator_id: Optional[int] = None

    def observe_epoch(self, epoch: int) -> None:
        if epoch > self.highest_epoch_seen:
            self.highest_epoch_seen = epoch

    @property
    def has_coordinator(self) -> bool:
        return self.coordinator_id is not None

    def set(self, coordinator_id: int, priority: int, epoch: int) -> None:
        if self.coordinator_id not in (None, coordinator_id):
            self.previous_coordinator_id = self.coordinator_id
        self.coordinator_id, self.coordinator_priority, self.epoch = coordinator_id, priority, epoch
        self.observe_epoch(epoch)
        self.since = time.time()
        self.registered = False
        self.touch()

    def touch(self, lease_expires_at: float = 0.0) -> None:
        self.last_heartbeat_mono = time.monotonic()
        self.last_heartbeat_wall = time.time()
        if lease_expires_at:
            self.lease_expires_at = lease_expires_at

    def clear(self) -> None:
        """Forget the coordinator (e.g. after a timeout). The epoch is kept to reject stale messages."""
        if self.coordinator_id is not None:
            self.previous_coordinator_id = self.coordinator_id
        self.coordinator_id = None
        self.coordinator_priority = None
        self.registered = False

    def silence(self) -> float:
        """Seconds since the last coordinator heartbeat/announcement."""
        return time.monotonic() - self.last_heartbeat_mono
