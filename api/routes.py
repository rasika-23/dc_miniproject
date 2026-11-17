"""Node HTTP endpoints. Each node exposes the applicable endpoints on its own port."""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_runtime, require_local
from api.schemas import EventsResponse, FaultResponse
from communication.messages import (CoordinatorHeartbeatMessage, CoordinatorMessage, ElectionMessage,
                                    FaultControlMessage, HeartbeatMessage, OkMessage, RegisterMessage,
                                    StatusSyncMessage)

router = APIRouter()


def _count(rt, msg_type: str) -> None:
    rt.counters.received[msg_type] += 1


# --------------------------------------------------------------------------- protocol
@router.post("/register")
async def register(msg: RegisterMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return rt.recovery.handle_register(msg)


@router.post("/heartbeat")
async def heartbeat(msg: HeartbeatMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return rt.receiver.handle_heartbeat(msg)


@router.post("/coordinator-heartbeat")
async def coordinator_heartbeat(msg: CoordinatorHeartbeatMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return await rt.receiver.handle_coordinator_heartbeat(msg)


@router.post("/election")
async def election(msg: ElectionMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return await rt.election.handle_election(msg)


@router.post("/election/ok")
async def election_ok(msg: OkMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return rt.election.handle_ok(msg)


@router.post("/coordinator")
async def coordinator(msg: CoordinatorMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return await rt.election.handle_coordinator_claim(msg.coordinator_id, msg.epoch, source="announcement",
                                                      election_id=msg.election_id)


@router.post("/status-sync")
async def status_sync(msg: StatusSyncMessage, rt=Depends(get_runtime)) -> Dict[str, Any]:
    _count(rt, msg.msg_type)
    return rt.receiver.handle_status_sync(msg)


# --------------------------------------------------------------------------- observation
@router.get("/health")
async def health(rt=Depends(get_runtime)) -> Dict[str, Any]:
    return rt.health_payload()


@router.get("/cluster/status")
async def cluster_status(rt=Depends(get_runtime)) -> Dict[str, Any]:
    return rt.status_payload()


@router.get("/cluster/events", response_model=EventsResponse)
async def cluster_events(since: Optional[float] = Query(None, description="unix timestamp"),
                         until: Optional[float] = None, limit: int = Query(1000, ge=1, le=20000),
                         rt=Depends(get_runtime)) -> EventsResponse:
    return EventsResponse(
        node_id=rt.state.node_id, incarnation=rt.state.incarnation,
        node_events=rt.repo.node_events(since=since, until=until, limit=limit),
        election_events=rt.repo.election_events(since=since, until=until, limit=limit))


@router.get("/cluster/metrics")
async def cluster_metrics(rt=Depends(get_runtime)) -> Dict[str, Any]:
    return rt.metrics_payload()


# --------------------------------------------------------------------------- fault injection
@router.post("/faults", response_model=FaultResponse, dependencies=[Depends(require_local)])
async def faults(msg: FaultControlMessage, rt=Depends(get_runtime)) -> FaultResponse:
    _count(rt, msg.msg_type)
    try:
        if msg.action == "apply":
            if not msg.fault_type:
                raise ValueError("fault_type is required")
            rt.faults.apply(msg.fault_type, delay_ms=msg.delay_ms, loss_pct=msg.loss_pct, scope=msg.scope,
                            duration_s=msg.duration_s, experiment_id=msg.experiment_id)
        elif msg.action == "clear":
            rt.faults.clear(msg.fault_type)
        else:
            raise ValueError("action must be 'apply' or 'clear'")
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return FaultResponse(ok=True, action=msg.action, active_faults=rt.faults.active())
