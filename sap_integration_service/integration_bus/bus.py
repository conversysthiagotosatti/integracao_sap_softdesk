"""
Publish integration events with DB-backed deduplication.

Flow: atomic insert receipt → on_commit fire in-process signal (consumers).
Heavy work should enqueue Celery from consumers, not from the poller thread.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from django.db import IntegrityError, transaction

from integration_bus.models import IntegrationEventReceipt
from integration_bus.signals import integration_event

logger = logging.getLogger(__name__)


def make_dedup_key(event_type: str, external_id: str, content_hash: str) -> str:
    raw = f"{event_type}|{external_id}|{content_hash}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _snapshot(payload: dict[str, Any], max_chars: int = 4000) -> dict[str, Any]:
    """Store a bounded copy for audit; full payload is still passed to consumers."""
    try:
        s = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except TypeError:
        s = str(payload)
    if len(s) > max_chars:
        return {"_truncated": True, "preview": s[:max_chars]}
    return json.loads(s) if isinstance(payload, dict) else {"_raw": s}


def publish(
    *,
    integration: str,
    event_type: str,
    external_id: str,
    payload: dict[str, Any],
    content_hash: str,
) -> bool:
    """
    Emit an internal integration event once per (event_type, external_id, content_hash).

    Returns True if this call published a new event; False if deduplicated.
    """
    dedup_key = make_dedup_key(event_type, external_id, content_hash)
    try:
        with transaction.atomic():
            IntegrationEventReceipt.objects.create(
                integration=integration,
                dedup_key=dedup_key,
                event_type=event_type,
                external_id=str(external_id),
                content_hash=content_hash,
                payload_snapshot=_snapshot(payload),
            )
    except IntegrityError:
        logger.info(
            "integration.event.deduped",
            extra={
                "integration": integration,
                "event_type": event_type,
                "external_id": external_id,
                "dedup_key": dedup_key,
            },
        )
        return False

    def _send() -> None:
        integration_event.send(
            integration_event,
            integration=integration,
            event_type=event_type,
            external_id=str(external_id),
            payload=payload,
            content_hash=content_hash,
            dedup_key=dedup_key,
        )

    transaction.on_commit(_send)
    logger.info(
        "integration.event.scheduled",
        extra={
            "integration": integration,
            "event_type": event_type,
            "external_id": external_id,
            "dedup_key": dedup_key,
        },
    )
    return True
