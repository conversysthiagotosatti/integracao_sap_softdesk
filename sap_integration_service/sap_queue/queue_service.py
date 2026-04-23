"""
Database-backed queue for asynchronous SAP integration (Celery-ready).

Package name is ``sap_queue`` (not ``queue``) to avoid shadowing Python's stdlib ``queue`` module.
"""
from __future__ import annotations

import logging

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from integrations import sap_service
from logs.models import SapIntegrationLog
from sap_queue.models import SapQueue
from services.retry_service import schedule_retry_or_fail

logger = logging.getLogger(__name__)


def enqueue(
    company_id: str,
    integration_type: str,
    payload: dict,
    *,
    user_id: str = "system",
    payload_version: str = "",
) -> SapQueue:
    return SapQueue.objects.create(
        company_id=str(company_id),
        user_id=str(user_id),
        integration_type=str(integration_type),
        payload=payload,
        status=SapQueue.Status.PENDING,
        payload_version=payload_version or "",
    )


def _dispatch_item(item: SapQueue) -> None:
    itype = item.integration_type
    if itype == sap_service.TYPE_PURCHASE_INVOICE:
        log = sap_service.send_purchase_invoice(
            item.company_id,
            item.user_id,
            item.payload,
            payload_version=item.payload_version or None,
        )
    elif itype == sap_service.TYPE_BUSINESS_PARTNER:
        log = sap_service.send_business_partner(
            item.company_id,
            item.user_id,
            item.payload,
            payload_version=item.payload_version or None,
        )
    else:
        raise ValueError(f"Unsupported integration_type in queue: {itype}")

    if log.status == SapIntegrationLog.Status.ERROR:
        raise RuntimeError(log.error_message or "SAP integration returned error status.")


@transaction.atomic
def process_queue(*, batch_size: int = 10, company_id: str | None = None) -> int:
    """
    Process pending queue rows (synchronous worker entrypoint; Celery can wrap this).
    Returns number of items attempted.

    If ``company_id`` is set, only rows for that tenant are considered.
    """
    now = timezone.now()
    qs_base = SapQueue.objects.select_for_update(skip_locked=True).filter(
        Q(status=SapQueue.Status.PENDING)
        | Q(
            status=SapQueue.Status.ERROR,
            next_retry_at__isnull=False,
            next_retry_at__lte=now,
        ),
    )
    if company_id:
        qs_base = qs_base.filter(company_id=str(company_id))
    qs = qs_base.order_by("created_at")[:batch_size]

    processed = 0
    for item in qs:
        item.status = SapQueue.Status.PROCESSING
        item.save(update_fields=["status"])
        processed += 1

        try:
            _dispatch_item(item)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Queue item %s failed", item.id)
            schedule_retry_or_fail(item, str(exc))
        else:
            item.status = SapQueue.Status.DONE
            item.next_retry_at = None
            item.save(update_fields=["status", "next_retry_at"])

    return processed


@transaction.atomic
def process_queue_item(*, item_id: int, company_id: str | None = None) -> dict:
    """
    Processa exatamente um registro da fila (por PK).

    Reenvio manual pelo painel: ``pending`` ou qualquer ``error`` (inclui
    falha definitiva sem ``next_retry_at``). Reinicia retentativas ao sair de ``error``.

    Retorna ``{"ok": bool, "message": str}``.
    """
    try:
        qs = SapQueue.objects.select_for_update().filter(pk=item_id)
        if company_id:
            qs = qs.filter(company_id=str(company_id))
        item = qs.get()
    except SapQueue.DoesNotExist:
        return {"ok": False, "message": f"Item #{item_id} não encontrado."}

    if item.status == SapQueue.Status.DONE:
        return {
            "ok": False,
            "message": f"Item #{item_id} já foi concluído (status=done).",
        }
    if item.status == SapQueue.Status.PROCESSING:
        return {
            "ok": False,
            "message": f"Item #{item_id} ainda em processamento; aguarde ou atualize a página.",
        }
    if item.status not in (SapQueue.Status.PENDING, SapQueue.Status.ERROR):
        return {
            "ok": False,
            "message": f"Item #{item_id} não pode ser enviado (status={item.status}).",
        }

    if item.status == SapQueue.Status.ERROR:
        item.retry_count = 0
        item.next_retry_at = None

    item.status = SapQueue.Status.PROCESSING
    item.save(update_fields=["status", "retry_count", "next_retry_at"])

    try:
        _dispatch_item(item)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Queue item %s failed", item.id)
        schedule_retry_or_fail(item, str(exc))
        return {"ok": False, "message": str(exc), "item_id": item_id}

    item.status = SapQueue.Status.DONE
    item.next_retry_at = None
    item.save(update_fields=["status", "next_retry_at"])
    return {
        "ok": True,
        "message": f"Item #{item_id} integrado ao SAP com sucesso.",
        "item_id": item_id,
    }
