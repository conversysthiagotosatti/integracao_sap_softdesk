"""
High-level SAP integration orchestration with full audit logging.
"""
from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.db import transaction

from integrations import sap_client
from integrations.metrics import IntegrationMetricEvent, record_integration_event
from logs.models import SapIntegrationLog

logger = logging.getLogger(__name__)

TYPE_PURCHASE_INVOICE = "purchase_invoice"
TYPE_BUSINESS_PARTNER = "business_partner"

ENDPOINTS = {
    TYPE_PURCHASE_INVOICE: ("POST", "PurchaseInvoices"),
    TYPE_BUSINESS_PARTNER: ("POST", "BusinessPartners"),
}

# Metadados do micro-sap / sistema principal — não enviar ao OData do SAP.
_PURCHASE_INVOICE_META_KEYS = frozenset({"company_id", "external_id"})


def purchase_invoice_body_for_service_layer(payload: dict) -> dict:
    """Remove chaves internas antes do POST ``PurchaseInvoices`` (v1/v2)."""
    return {k: v for k, v in payload.items() if k not in _PURCHASE_INVOICE_META_KEYS}


def _payload_version(explicit: str | None) -> str:
    if explicit:
        return str(explicit)
    return str(getattr(settings, "INTEGRATION_DEFAULT_PAYLOAD_VERSION", "1"))


def _find_idempotent_success(
    company_id: str,
    integration_type: str,
    external_id: str,
) -> SapIntegrationLog | None:
    if not external_id:
        return None
    return (
        SapIntegrationLog.objects.filter(
            company_id=str(company_id),
            integration_type=integration_type,
            external_id=external_id,
            status=SapIntegrationLog.Status.SUCCESS,
        )
        .order_by("-created_at")
        .first()
    )


def _create_pending_log(
    *,
    company_id: str,
    user_id: str,
    integration_type: str,
    method: str,
    endpoint: str,
    request_payload: dict,
    external_id: str,
    payload_version: str,
) -> SapIntegrationLog:
    return SapIntegrationLog.objects.create(
        company_id=str(company_id),
        user_id=str(user_id),
        integration_type=integration_type,
        method=method,
        endpoint=endpoint,
        request_payload=request_payload,
        status=SapIntegrationLog.Status.PENDING,
        external_id=external_id or "",
        payload_version=payload_version,
    )


def _finalize_log(
    log: SapIntegrationLog,
    *,
    status: str,
    http_status: int | None,
    response_payload: Any,
    error_message: str = "",
) -> SapIntegrationLog:
    log.status = status
    log.http_status = http_status
    if response_payload is None:
        log.response_payload = None
    elif isinstance(response_payload, (dict, list)):
        log.response_payload = response_payload
    else:
        log.response_payload = {"value": response_payload}
    log.error_message = error_message or ""
    log.save(
        update_fields=[
            "status",
            "http_status",
            "response_payload",
            "error_message",
            "updated_at",
        ]
    )
    record_integration_event(
        IntegrationMetricEvent(
            integration_type=log.integration_type,
            company_id=log.company_id,
            status=status,
            http_status=http_status,
            extra={"log_id": log.id},
        )
    )
    return log


@transaction.atomic
def send_purchase_invoice(
    company_id: str,
    user_id: str,
    payload: dict,
    *,
    payload_version: str | None = None,
) -> SapIntegrationLog:
    pv = _payload_version(payload_version)
    external_id = str(payload.get("external_id") or payload.get("Reference") or "")

    cached = _find_idempotent_success(company_id, TYPE_PURCHASE_INVOICE, external_id)
    if cached:
        logger.info("Idempotent hit for purchase_invoice external_id=%s", external_id)
        return cached

    method, endpoint = ENDPOINTS[TYPE_PURCHASE_INVOICE]
    log = _create_pending_log(
        company_id=company_id,
        user_id=user_id,
        integration_type=TYPE_PURCHASE_INVOICE,
        method=method,
        endpoint=endpoint,
        request_payload=payload,
        external_id=external_id,
        payload_version=pv,
    )

    sl_body = purchase_invoice_body_for_service_layer(payload)
    try:
        sap_resp = sap_client.request(company_id, method, endpoint, sl_body)
    except Exception as exc:  # noqa: BLE001 — boundary: persist fault
        logger.exception("SAP purchase_invoice failed for company_id=%s", company_id)
        return _finalize_log(
            log,
            status=SapIntegrationLog.Status.ERROR,
            http_status=None,
            response_payload={},
            error_message=str(exc),
        )

    ok = 200 <= sap_resp.status_code < 300
    return _finalize_log(
        log,
        status=SapIntegrationLog.Status.SUCCESS if ok else SapIntegrationLog.Status.ERROR,
        http_status=sap_resp.status_code,
        response_payload=sap_resp.data,
        error_message="" if ok else str(sap_resp.data),
    )


@transaction.atomic
def send_business_partner(
    company_id: str,
    user_id: str,
    payload: dict,
    *,
    payload_version: str | None = None,
) -> SapIntegrationLog:
    pv = _payload_version(payload_version)
    external_id = str(payload.get("external_id") or payload.get("CardCode") or "")

    cached = _find_idempotent_success(company_id, TYPE_BUSINESS_PARTNER, external_id)
    if cached:
        logger.info("Idempotent hit for business_partner external_id=%s", external_id)
        return cached

    method, endpoint = ENDPOINTS[TYPE_BUSINESS_PARTNER]
    log = _create_pending_log(
        company_id=company_id,
        user_id=user_id,
        integration_type=TYPE_BUSINESS_PARTNER,
        method=method,
        endpoint=endpoint,
        request_payload=payload,
        external_id=external_id,
        payload_version=pv,
    )

    try:
        sap_resp = sap_client.request(company_id, method, endpoint, payload)
    except Exception as exc:  # noqa: BLE001
        logger.exception("SAP business_partner failed for company_id=%s", company_id)
        return _finalize_log(
            log,
            status=SapIntegrationLog.Status.ERROR,
            http_status=None,
            response_payload={},
            error_message=str(exc),
        )

    ok = 200 <= sap_resp.status_code < 300
    return _finalize_log(
        log,
        status=SapIntegrationLog.Status.SUCCESS if ok else SapIntegrationLog.Status.ERROR,
        http_status=sap_resp.status_code,
        response_payload=sap_resp.data,
        error_message="" if ok else str(sap_resp.data),
    )
