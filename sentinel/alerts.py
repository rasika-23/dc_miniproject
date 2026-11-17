"""Alert delivery channels: node log (always) and an optional webhook."""
from __future__ import annotations

from typing import Any, Dict, List

import httpx


async def deliver(alert: Dict[str, Any], webhook_url: str | None) -> List[str]:
    """Send one alert. Returns the channels that accepted it. The alert carries an idempotency key."""
    channels = ["log"]
    if webhook_url:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.post(webhook_url, json=alert, headers={"Idempotency-Key": alert["dedupe_key"]})
                if r.status_code < 300:
                    channels.append("webhook")
        except httpx.HTTPError:
            pass
    return channels


def message_for(event: str, incident: Dict[str, Any], service_name: str) -> str:
    kind = incident.get("kind", "down")
    if event == "opened":
        what = "is DOWN" if kind == "down" else "is DEGRADED"
        return f"[{incident['severity'].upper()}] {service_name} {what}: {incident.get('reason', '')}"
    if event == "escalated":
        return f"[{incident['severity'].upper()}] {service_name} got worse and is now DOWN: {incident.get('reason', '')}"
    return f"[RESOLVED] {service_name} has recovered ({incident.get('incident_id')})"
