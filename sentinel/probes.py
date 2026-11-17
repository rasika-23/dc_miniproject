"""Health checks against monitored services (HTTP status + latency, or TCP connect)."""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict
from urllib.parse import urlparse

import httpx

from node.config import ServiceSpec


def make_result(spec: ServiceSpec, node_id: int, ts: float, ok: bool, latency_ms: float,
                status_code: int | None = None, error: str | None = None) -> Dict[str, Any]:
    return {"service_id": spec.id, "node_id": node_id, "ts": ts, "ok": ok, "latency_ms": round(latency_ms, 2),
            "status_code": status_code, "error": error}


async def probe_http(client: httpx.AsyncClient, spec: ServiceSpec, node_id: int) -> Dict[str, Any]:
    ts, t0 = time.time(), time.perf_counter()
    try:
        resp = await client.get(spec.url, timeout=spec.timeout_s)
        latency = (time.perf_counter() - t0) * 1000
        if resp.status_code == spec.expected_status:
            return make_result(spec, node_id, ts, True, latency, resp.status_code)
        return make_result(spec, node_id, ts, False, latency, resp.status_code, f"HTTP {resp.status_code}")
    except httpx.TimeoutException:
        return make_result(spec, node_id, ts, False, spec.timeout_s * 1000, None, "timeout")
    except httpx.ConnectError:
        return make_result(spec, node_id, ts, False, (time.perf_counter() - t0) * 1000, None, "connection refused")
    except httpx.HTTPError as exc:
        return make_result(spec, node_id, ts, False, (time.perf_counter() - t0) * 1000, None, type(exc).__name__)


async def probe_tcp(spec: ServiceSpec, node_id: int) -> Dict[str, Any]:
    u = urlparse(spec.url)
    ts, t0 = time.time(), time.perf_counter()
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(u.hostname, u.port), timeout=spec.timeout_s)
        latency = (time.perf_counter() - t0) * 1000
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return make_result(spec, node_id, ts, True, latency)
    except asyncio.TimeoutError:
        return make_result(spec, node_id, ts, False, spec.timeout_s * 1000, None, "timeout")
    except OSError as exc:
        return make_result(spec, node_id, ts, False, (time.perf_counter() - t0) * 1000, None,
                           "connection refused" if isinstance(exc, ConnectionRefusedError) else type(exc).__name__)


async def run_probe(client: httpx.AsyncClient, spec: ServiceSpec, node_id: int) -> Dict[str, Any]:
    return await (probe_tcp(spec, node_id) if spec.kind == "tcp" else probe_http(client, spec, node_id))
