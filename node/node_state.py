"""Identity, role and health of a single node process."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class Role(str, Enum):
    WORKER = "worker"
    CANDIDATE = "candidate"
    COORDINATOR = "coordinator"


class Health(str, Enum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    RECOVERING = "RECOVERING"


@dataclass
class NodeState:
    node_id: int
    priority: int
    host: str
    port: int
    # A new incarnation is assigned every time the process starts. Using the start time in
    # milliseconds keeps it monotonic across restarts, so stale messages can be rejected.
    incarnation: int = field(default_factory=lambda: int(time.time() * 1000))
    role: Role = Role.WORKER
    startup_done: bool = False

    @property
    def started_at(self) -> float:
        return self.incarnation / 1000.0

    @property
    def uptime(self) -> float:
        return max(0.0, time.time() - self.started_at)
