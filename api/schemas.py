"""Request/response schemas for the non-protocol endpoints."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    node_id: int
    priority: int
    role: str
    epoch: int
    coordinator_id: Optional[int] = None
    election_in_progress: bool = False


class FaultResponse(BaseModel):
    ok: bool
    action: str
    active_faults: List[Dict[str, Any]] = Field(default_factory=list)
    message: str = ""


class EventsResponse(BaseModel):
    node_id: int
    incarnation: int
    node_events: List[Dict[str, Any]]
    election_events: List[Dict[str, Any]]
