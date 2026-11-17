"""A tiny stand-in microservice with a chaos switch, so outages can be created on demand."""
from __future__ import annotations

import asyncio
import random
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

LOOPBACK = {"127.0.0.1", "::1", "localhost", "testclient"}
MODES = ("ok", "down", "slow", "flaky", "hang")


class ChaosRequest(BaseModel):
    mode: str = "ok"                     # ok | down (503) | slow | flaky (random 503) | hang (never answers in time)
    latency_ms: float = 0.0              # used by slow
    error_pct: float = 0.0               # used by flaky
    duration_s: Optional[float] = None   # auto-reset to ok after this many seconds


def make_app(service_id: str, name: str) -> FastAPI:
    app = FastAPI(title=f"demo service {service_id}", docs_url=None, redoc_url=None)
    state = {"mode": "ok", "latency_ms": 0.0, "error_pct": 0.0, "until": None, "since": time.time()}

    def current() -> str:
        if state["until"] and time.time() >= state["until"]:
            state.update(mode="ok", latency_ms=0.0, error_pct=0.0, until=None)
        return state["mode"]

    @app.get("/health")
    async def health():
        mode = current()
        if mode == "down":
            return JSONResponse({"service": service_id, "status": "down"}, status_code=503)
        if mode == "hang":
            await asyncio.sleep(30)
        if mode == "slow":
            await asyncio.sleep(state["latency_ms"] / 1000.0)
        if mode == "flaky" and random.random() * 100 < state["error_pct"]:
            return JSONResponse({"service": service_id, "status": "error"}, status_code=503)
        return {"service": service_id, "name": name, "status": "ok"}

    @app.get("/chaos")
    async def get_chaos():
        return {"service": service_id, "mode": current(), **{k: state[k] for k in ("latency_ms", "error_pct", "until")}}

    @app.post("/chaos")
    async def set_chaos(req: ChaosRequest, request: Request):
        if (request.client.host if request.client else "") not in LOOPBACK:
            raise HTTPException(403, "chaos controls are loopback only")
        if req.mode not in MODES:
            raise HTTPException(422, f"mode must be one of {MODES}")
        state.update(mode=req.mode, latency_ms=req.latency_ms, error_pct=req.error_pct,
                     until=(time.time() + req.duration_s) if req.duration_s else None, since=time.time())
        return {"service": service_id, "mode": req.mode}

    return app
