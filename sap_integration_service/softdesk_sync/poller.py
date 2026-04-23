"""
Polling orchestration: fetch pages, detect creates/updates, publish internal events.
"""
from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone as dj_tz

from integration_bus import bus as event_bus
from softdesk_sync.chamados_source import get_chamados_list_client
from softdesk_sync.client import SoftdeskAPIClient
from softdesk_sync.conversys_client import ConversysHelpdeskClient
from softdesk_sync.diff import payload_hash
from softdesk_sync.models import SoftdeskChamadoState, SoftdeskSyncState
from softdesk_sync.pagination import extract_items_and_next_page

logger = logging.getLogger(__name__)

INTEGRATION_NAME = "softdesk"
EVENT_CREATED = "softdesk.chamado.created"
EVENT_UPDATED = "softdesk.chamado.updated"


def _id_field() -> str:
    return getattr(settings, "SOFTDESK_CHAMADO_ID_FIELD", "id")


def _updated_field() -> str:
    return getattr(settings, "SOFTDESK_CHAMADO_UPDATED_AT_FIELD", "updated_at")


def _extract_id(item: dict[str, Any]) -> str:
    fid = _id_field()
    val = item.get(fid)
    if val is None:
        raise KeyError(f"Chamado payload missing id field {fid!r}")
    return str(val)


def _extract_updated(item: dict[str, Any]) -> str:
    field = _updated_field()
    val = item.get(field)
    return "" if val is None else str(val)


def _max_watermark(items: list[dict[str, Any]], current: str) -> str:
    best = current or ""
    for it in items:
        v = _extract_updated(it)
        if not v:
            continue
        if not best or v > best:
            best = v
    return best


def run_poll(
    *,
    scope: str = "chamados",
    client: SoftdeskAPIClient | ConversysHelpdeskClient | None = None,
) -> dict[str, int]:
    """
    Execute one polling cycle (synchronous; intended to be called from Celery).

    Returns counters for observability.
    """
    client = client or get_chamados_list_client()
    page_size = int(getattr(settings, "SOFTDESK_PAGE_SIZE", 50))
    max_pages = int(getattr(settings, "SOFTDESK_MAX_PAGES_PER_RUN", 50))

    created_events = 0
    updated_events = 0
    pages = 0

    with transaction.atomic():
        state, _ = SoftdeskSyncState.objects.select_for_update().get_or_create(
            scope=scope,
            defaults={"watermark": ""},
        )
        state.last_poll_started_at = dj_tz.now()
        state.save(update_fields=["last_poll_started_at"])

    watermark = state.watermark or None
    first_page = int(getattr(settings, "SOFTDESK_FIRST_PAGE", 1))
    next_page: int | None = first_page

    try:
        while next_page is not None and pages < max_pages:
            pages += 1
            raw = client.fetch_chamados_page(
                page=next_page,
                page_size=page_size,
                updated_since=watermark or None,
            )
            items, next_page_hint = extract_items_and_next_page(raw)
            for item in items:
                external_id = _extract_id(item)
                h = payload_hash(item)
                remote_updated = _extract_updated(item)

                with transaction.atomic():
                    row = SoftdeskChamadoState.objects.select_for_update().filter(external_id=external_id).first()
                    if row is None:
                        published = event_bus.publish(
                            integration=INTEGRATION_NAME,
                            event_type=EVENT_CREATED,
                            external_id=external_id,
                            payload=item,
                            content_hash=h,
                        )
                        if published:
                            created_events += 1
                        SoftdeskChamadoState.objects.update_or_create(
                            external_id=external_id,
                            defaults={
                                "content_hash": h,
                                "remote_updated_at": remote_updated,
                            },
                        )
                    elif row.content_hash != h:
                        published = event_bus.publish(
                            integration=INTEGRATION_NAME,
                            event_type=EVENT_UPDATED,
                            external_id=external_id,
                            payload=item,
                            content_hash=h,
                        )
                        if published:
                            updated_events += 1
                        row.content_hash = h
                        row.remote_updated_at = remote_updated
                        row.save(update_fields=["content_hash", "remote_updated_at", "updated_at"])

            if next_page_hint is not None:
                next_page = next_page_hint
            elif (
                bool(getattr(settings, "SOFTDESK_AUTO_INCREMENT_PAGE", True))
                and items
                and len(items) >= page_size
            ):
                next_page = int(next_page or first_page) + 1
            else:
                next_page = None

            new_wm = _max_watermark(items, state.watermark)
            if new_wm:
                state.watermark = new_wm

        state.last_poll_finished_at = dj_tz.now()
        state.consecutive_failures = 0
        state.last_error_code = ""
        state.last_error_message = ""
        state.save(
            update_fields=[
                "watermark",
                "last_poll_finished_at",
                "consecutive_failures",
                "last_error_code",
                "last_error_message",
                "updated_at",
            ]
        )

        logger.info(
            "softdesk.poll.success",
            extra={
                "scope": scope,
                "pages": pages,
                "created_events": created_events,
                "updated_events": updated_events,
                "watermark": state.watermark,
            },
        )
        return {"pages": pages, "created_events": created_events, "updated_events": updated_events}

    except Exception as exc:
        logger.exception(
            "softdesk.poll.failed",
            extra={"scope": scope, "error": str(exc)},
        )
        with transaction.atomic():
            st = SoftdeskSyncState.objects.select_for_update().get(pk=state.pk)
            st.consecutive_failures = int(st.consecutive_failures or 0) + 1
            st.last_error_code = exc.__class__.__name__
            st.last_error_message = str(exc)[:4000]
            st.last_poll_finished_at = dj_tz.now()
            st.save(
                update_fields=[
                    "consecutive_failures",
                    "last_error_code",
                    "last_error_message",
                    "last_poll_finished_at",
                    "updated_at",
                ]
            )
        raise
