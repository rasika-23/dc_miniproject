"""Protocol message structures.

Every message carries the sender's node id and process incarnation. Messages that relate to
leadership also carry the coordinator epoch so receivers can reject stale traffic.
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    REGISTER = "REGISTER"
    HEARTBEAT = "HEARTBEAT"
    COORDINATOR_HEARTBEAT = "COORDINATOR_HEARTBEAT"
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"
    STATUS_SYNC = "STATUS_SYNC"
    FAULT_CONTROL = "FAULT_CONTROL"


HEARTBEAT_TYPES = {MessageType.HEARTBEAT.value, MessageType.COORDINATOR_HEARTBEAT.value}
ELECTION_TYPES = {MessageType.ELECTION.value, MessageType.OK.value, MessageType.COORDINATOR.value}


class Envelope(BaseModel):
    msg_type: str
    sender_id: int
    incarnation: int
    epoch: int = 0
    sent_at: float = Field(default_factory=time.time)


class RegisterMessage(Envelope):
    msg_type: str = MessageType.REGISTER.value
    priority: int
    host: str = "127.0.0.1"
    port: int = 0


class HeartbeatMessage(Envelope):
    msg_type: str = MessageType.HEARTBEAT.value
    seq: int = 0


class CoordinatorHeartbeatMessage(Envelope):
    msg_type: str = MessageType.COORDINATOR_HEARTBEAT.value
    coordinator_id: int
    lease_expires_at: float = 0.0
    seq: int = 0


class ElectionMessage(Envelope):
    msg_type: str = MessageType.ELECTION.value
    election_id: str
    candidate_priority: int


class OkMessage(Envelope):
    msg_type: str = MessageType.OK.value
    election_id: str


class CoordinatorMessage(Envelope):
    msg_type: str = MessageType.COORDINATOR.value
    coordinator_id: int
    coordinator_priority: int
    election_id: Optional[str] = None


class StatusSyncMessage(Envelope):
    msg_type: str = MessageType.STATUS_SYNC.value
    coordinator_id: int
    membership: List[Dict[str, Any]] = Field(default_factory=list)


class FaultControlMessage(Envelope):
    """Dashboard / test-controller request to a node's fault injection manager."""
    msg_type: str = MessageType.FAULT_CONTROL.value
    action: str = "apply"                 # apply | clear
    fault_type: Optional[str] = None      # heartbeat_delay | message_loss
    delay_ms: float = 0.0
    loss_pct: float = 0.0
    scope: str = "heartbeat"              # heartbeat | election | all
    duration_s: Optional[float] = None
    experiment_id: Optional[str] = None
