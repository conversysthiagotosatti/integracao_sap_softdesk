"""
Sincroniza departamentos do SAP Business One Service Layer (GET ``Departments``) para o banco local.
"""
from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from historico_sap.models import DepartamentoSap
from integrations import sap_client

logger = logging.getLogger(__name__)


def _department_code(item: dict[str, Any]) -> int | None:
    for k in ("Code", "DepartmentID", "code", "departmentId"):
        if k not in item or item[k] is None:
            continue
        try:
            return int(item[k])
        except (TypeError, ValueError):
            continue
    return None


def _department_name(item: dict[str, Any]) -> str:
    for k in ("Name", "DepartmentName", "name"):
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()[:512]
    return ""


def _department_description(item: dict[str, Any]) -> str:
    for k in ("Description", "description", "Remarks"):
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()[:1024]
    return ""


def fetch_all_departments_pages(company_id: str, *, page_size: int = 200) -> list[dict[str, Any]]:
    """GET ``Departments`` com paginação OData ``$top`` / ``$skip``."""
    out: list[dict[str, Any]] = []
    skip = 0
    while True:
        endpoint = f"Departments?$top={page_size}&$skip={skip}"
        resp = sap_client.request(company_id, "GET", endpoint)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"SAP Departments HTTP {resp.status_code}: {repr(resp.data)}"[:2000]
            )
        data = resp.data if isinstance(resp.data, dict) else {}
        batch = data.get("value")
        if not isinstance(batch, list):
            if skip == 0:
                raise RuntimeError("Resposta SAP sem lista 'value' em Departments.")
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return out


@transaction.atomic(using="conversys")
def sync_departments(company_id: str) -> dict[str, Any]:
    """
    Busca ``/Departments`` no Service Layer e grava/atualiza na tabela ``departamentos_sap`` (BD ``historico_clientes`` / ``conversys``).

    Retorna contadores e erros parciais (linhas ignoradas).
    """
    cid = (company_id or "").strip()
    if not cid:
        raise ValueError("company_id é obrigatório.")
    if "conversys" not in settings.DATABASES:
        raise RuntimeError(
            "Configure CONVERSYS_POSTGRES_DB (banco historico_clientes) para gravar departamentos."
        )

    rows = fetch_all_departments_pages(cid)
    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    for raw in rows:
        if not isinstance(raw, dict):
            skipped += 1
            continue
        code = _department_code(raw)
        if code is None:
            skipped += 1
            errors.append(f"registro sem Code/DepartmentID: {raw!r}"[:300])
            continue
        name = _department_name(raw)
        desc = _department_description(raw)
        obj, created = DepartamentoSap.objects.update_or_create(
            company_id=cid,
            code=code,
            defaults={
                "name": name,
                "description": desc,
                "payload": raw,
                "synced_at": timezone.now(),
            },
        )
        if created:
            inserted += 1
        else:
            updated += 1

    logger.info(
        "departments_sync.done",
        extra={
            "company_id": cid,
            "inserted": inserted,
            "updated": updated,
            "skipped": skipped,
        },
    )
    return {
        "ok": True,
        "company_id": cid,
        "total_api": len(rows),
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "errors_sample": errors[:10],
    }
