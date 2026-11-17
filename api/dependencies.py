"""Shared FastAPI dependencies."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Request

LOOPBACK = {"127.0.0.1", "::1", "localhost", "testclient"}


def get_runtime(request: Request):
    return request.app.state.runtime


def require_local(request: Request) -> None:
    """Fault controls are for the local test environment only."""
    host = request.client.host if request.client else ""
    if host not in LOOPBACK:
        raise HTTPException(status_code=403, detail="fault injection is restricted to loopback clients")


RuntimeDep = Depends(get_runtime)
