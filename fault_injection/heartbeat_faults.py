"""Per-node fault injection: delayed and lost outgoing messages.

Process crashes are NOT handled here. They come from the external process controller
(process_control.py) so a node can be killed even when it is the one under test.
"""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from communication.messages import ELECTION_TYPES, HEARTBEAT_TYPES

HEARTBEAT_DELAY = "heartbeat_delay"
MESSAGE_LOSS = "message_loss"
VALID_SCOPES = ("heartbeat", "election", "all")


def scope_matches(scope: str, msg_type: str) -> bool:
    if scope == "all":
        return True
    if scope == "heartbeat":
        return msg_type in HEARTBEAT_TYPES
    if scope == "election":
        return msg_type in ELECTION_TYPES
    return False


@dataclass
class ActiveFault:
    kind: str
    delay_ms: float = 0.0
    loss_pct: float = 0.0
    scope: str = "heartbeat"
    started_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    experiment_id: Optional[str] = None
    fault_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def to_dict(self) -> Dict[str, Any]:
        remaining = None if self.expires_at is None else max(0.0, self.expires_at - time.time())
        return {"fault_id": self.fault_id, "kind": self.kind, "delay_ms": self.delay_ms,
                "loss_pct": self.loss_pct, "scope": self.scope, "started_at": self.started_at,
                "expires_at": self.expires_at, "remaining_s": remaining, "experiment_id": self.experiment_id}


@dataclass(frozen=True)
class Decision:
    drop: bool = False
    delay_s: float = 0.0


class FaultInjectionManager:
    def __init__(self, enabled: bool = True, seed: Optional[int] = None,
                 on_start: Optional[Callable[[ActiveFault], None]] = None,
                 on_end: Optional[Callable[[ActiveFault, str], None]] = None):
        self.enabled = enabled
        self._rng = random.Random(seed)
        self._faults: Dict[str, ActiveFault] = {}
        self._on_start, self._on_end = on_start, on_end

    # ------------------------------------------------------------------ control
    def apply(self, kind: str, *, delay_ms: float = 0.0, loss_pct: float = 0.0, scope: str = "heartbeat",
              duration_s: Optional[float] = None, experiment_id: Optional[str] = None) -> ActiveFault:
        if not self.enabled:
            raise PermissionError("fault injection is disabled in the configuration")
        if kind not in (HEARTBEAT_DELAY, MESSAGE_LOSS):
            raise ValueError(f"unknown fault type '{kind}'")
        if scope not in VALID_SCOPES:
            raise ValueError(f"scope must be one of {VALID_SCOPES}")
        if kind == HEARTBEAT_DELAY and delay_ms <= 0:
            raise ValueError("delay_ms must be > 0")
        if kind == MESSAGE_LOSS and not (0 < loss_pct <= 100):
            raise ValueError("loss_pct must be in (0, 100]")
        self.clear(kind, reason="replaced")           # one fault per kind at a time
        fault = ActiveFault(kind=kind, delay_ms=float(delay_ms), loss_pct=float(loss_pct), scope=scope,
                            expires_at=(time.time() + duration_s) if duration_s else None,
                            experiment_id=experiment_id)
        self._faults[fault.fault_id] = fault
        if self._on_start:
            self._on_start(fault)
        return fault

    def clear(self, kind: Optional[str] = None, reason: str = "cleared") -> List[ActiveFault]:
        removed = [f for f in list(self._faults.values()) if kind in (None, f.kind)]
        for f in removed:
            self._faults.pop(f.fault_id, None)
            if self._on_end:
                self._on_end(f, reason)
        return removed

    def expire(self) -> List[ActiveFault]:
        now = time.time()
        expired = [f for f in list(self._faults.values()) if f.expires_at is not None and f.expires_at <= now]
        for f in expired:
            self._faults.pop(f.fault_id, None)
            if self._on_end:
                self._on_end(f, "expired")
        return expired

    # ------------------------------------------------------------------ hot path
    def decide(self, msg_type: str) -> Decision:
        """Called for every outgoing protocol message."""
        if not self._faults:
            return Decision()
        now = time.time()
        drop, delay = False, 0.0
        for f in list(self._faults.values()):
            if f.expires_at is not None and f.expires_at <= now:
                continue
            if not scope_matches(f.scope, msg_type):
                continue
            if f.kind == MESSAGE_LOSS and self._rng.random() * 100.0 < f.loss_pct:
                drop = True
            elif f.kind == HEARTBEAT_DELAY:
                delay = max(delay, f.delay_ms / 1000.0)
        return Decision(drop=drop, delay_s=0.0 if drop else delay)

    def active(self) -> List[Dict[str, Any]]:
        return [f.to_dict() for f in self._faults.values()]
