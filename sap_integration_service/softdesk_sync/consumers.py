"""
Consumers for internal Softdesk events.

Keep handlers fast: enqueue domain work (Helpdesk tickets, assets, AI) via Celery.
"""
from __future__ import annotations

import logging

from integration_bus.signals import integration_event

logger = logging.getLogger(__name__)


def _on_integration_event(
    sender,
    *,
    integration: str,
    event_type: str,
    external_id: str,
    payload: dict,
    content_hash: str,
    dedup_key: str,
    **kwargs,
) -> None:
    if integration != "softdesk":
        return
    logger.info(
        "softdesk.consumer.received",
        extra={
            "event_type": event_type,
            "external_id": external_id,
            "content_hash": content_hash,
            "dedup_key": dedup_key,
        },
    )
    # Example extension: dispatch_helpdesk_sync.delay(event_type, external_id, payload)


integration_event.connect(_on_integration_event, dispatch_uid="softdesk_default_consumer")
