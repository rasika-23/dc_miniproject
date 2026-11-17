"""HTTP requests to peer nodes, with message counters and fault-injection hooks."""
from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any, Dict, Optional

import httpx
from pydantic import BaseModel

from communication.peer_registry import PeerRegistry


class PeerError(Exception):
    """Base class for failed peer communication."""


class PeerUnreachable(PeerError):
    """Connection refused, timed out, or otherwise no usable response."""


class MessageDropped(PeerError):
    """The message was discarded by the fault injection manager (simulated loss)."""


class MessageCounters:
    """Protocol message counts for one process incarnation (used for message-overhead metrics)."""

    def __init__(self) -> None:
        self.sent: Counter = Counter()
        self.received: Counter = Counter()
        self.dropped = 0
        self.delayed = 0
        self.failed = 0

    def snapshot(self) -> Dict[str, Any]:
        return {"sent": dict(self.sent), "sent_total": sum(self.sent.values()),
                "received": dict(self.received), "received_total": sum(self.received.values()),
                "dropped": self.dropped, "delayed": self.delayed, "failed": self.failed}


class PeerClient:
    def __init__(self, registry: PeerRegistry, *, faults=None, timeout: float = 1.0,
                 counters: Optional[MessageCounters] = None):
        self.registry = registry
        self.faults = faults
        self.counters = counters or MessageCounters()
        self._http = httpx.AsyncClient(timeout=timeout)

    async def close(self) -> None:
        await self._http.aclose()

    async def send(self, peer_id: int, path: str, message: BaseModel) -> Dict[str, Any]:
        """POST a protocol message. Raises PeerUnreachable / MessageDropped on failure."""
        msg_type = getattr(message, "msg_type", "UNKNOWN")
        self.counters.sent[msg_type] += 1
        if self.faults is not None:
            decision = self.faults.decide(msg_type)
            if decision.drop:
                self.counters.dropped += 1
                raise MessageDropped(f"{msg_type} to node {peer_id} dropped by fault injection")
            if decision.delay_s > 0:
                self.counters.delayed += 1
                await asyncio.sleep(decision.delay_s)
        url = self.registry.url(peer_id, path)
        payload = message.model_dump(mode="json")
        for attempt in (1, 2):
            try:
                resp = await self._http.post(url, json=payload)
                break
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError) as exc:
                if attempt == 2:  # stale keep-alive connection to a restarted peer: retry once
                    self.counters.failed += 1
                    raise PeerUnreachable(str(exc)) from exc
            except httpx.HTTPError as exc:
                self.counters.failed += 1
                raise PeerUnreachable(f"{type(exc).__name__}: {exc}") from exc
        if resp.status_code != 200:
            self.counters.failed += 1
            raise PeerError(f"HTTP {resp.status_code} from node {peer_id}{path}")
        return resp.json()

    async def get(self, peer_id: int, path: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """GET helper for discovery/status calls (not counted as protocol messages)."""
        try:
            resp = await self._http.get(self.registry.url(peer_id, path), timeout=timeout or self._http.timeout)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as exc:
            raise PeerUnreachable(f"{type(exc).__name__}: {exc}") from exc
