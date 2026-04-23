"""
Lightweight integration metrics hooks (extend with Prometheus/OpenTelemetry).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any


_lock = Lock()
_counters: dict[str, int] = {}


@dataclass
class IntegrationMetricEvent:
    integration_type: str
    company_id: str
    status: str
    http_status: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def record_integration_event(event: IntegrationMetricEvent) -> None:
    key = f"{event.integration_type}:{event.status}"
    with _lock:
        _counters[key] = _counters.get(key, 0) + 1


def snapshot_counters() -> dict[str, int]:
    with _lock:
        return dict(_counters)
