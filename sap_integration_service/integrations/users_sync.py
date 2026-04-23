"""
Sincroniza usuários do SAP Business One Service Layer (GET ``Users``) para o banco local.
"""
from __future__ import annotations

import logging
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from historico_sap.models import UsuarioSap
from integrations import sap_client

logger = logging.getLogger(__name__)


def _unwrap_user_record(item: dict[str, Any]) -> dict[str, Any]:
    """
    Retorna só o objeto do usuário: desembrulha ``User`` / ``user`` e remove metadados OData
    (chaves ``@...``, ``odata.*``) do mesmo nível.
    """
    for nk in ("User", "user"):
        inner = item.get(nk)
        if isinstance(inner, dict) and (
            inner.get("InternalKey") is not None
            or inner.get("internalKey") is not None
            or inner.get("UserCode")
            or inner.get("userCode")
        ):
            return dict(inner)
    return {
        k: v
        for k, v in item.items()
        if isinstance(k, str)
        and not k.startswith("@")
        and not k.lower().startswith("odata.")
    }


# Chaves canônicas gravadas em ``raw_json`` (ordem estável).
_USER_JSON_FIELD_SPECS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("InternalKey", ("InternalKey", "internalKey")),
    ("UserCode", ("UserCode", "userCode")),
    ("UserName", ("UserName", "userName")),
    ("eMail", ("eMail", "Email", "email", "E_Mail")),
    (
        "MobilePhoneNumber",
        ("MobilePhoneNumber", "mobilePhoneNumber", "MobilePhone", "mobilePhone"),
    ),
    ("Department", ("Department", "department", "DepartmentCode", "departmentCode")),
)


def _first_present(record: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    for key in candidates:
        if key not in record:
            continue
        val = record[key]
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        return val
    return None


def _subset_user_json(record: dict[str, Any]) -> dict[str, Any]:
    """Monta o JSON persistido: só os campos solicitados, chaves fixas, valores nulos se ausentes."""
    out: dict[str, Any] = {}
    for canonical, candidates in _USER_JSON_FIELD_SPECS:
        val = _first_present(record, candidates)
        if canonical == "InternalKey" and val is not None:
            try:
                val = int(val)
            except (TypeError, ValueError):
                pass
        out[canonical] = val
    return out


def _user_dedupe_key(item: dict[str, Any]) -> str | None:
    """Chave estável para unicidade por empresa (InternalKey ou UserCode)."""
    for k in ("InternalKey", "internalKey"):
        if k not in item or item[k] is None:
            continue
        try:
            return f"i:{int(item[k])}"
        except (TypeError, ValueError):
            continue
    for k in ("UserCode", "userCode"):
        v = item.get(k)
        if v is not None and str(v).strip():
            return f"u:{str(v).strip()[:250]}"
    return None


def fetch_all_users_pages(company_id: str, *, page_size: int = 200) -> list[dict[str, Any]]:
    """GET ``Users`` com paginação OData ``$top`` / ``$skip``."""
    out: list[dict[str, Any]] = []
    skip = 0
    while True:
        endpoint = f"Users?$top={page_size}&$skip={skip}"
        resp = sap_client.request(company_id, "GET", endpoint)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"SAP Users HTTP {resp.status_code}: {repr(resp.data)}"[:2000]
            )
        data = resp.data if isinstance(resp.data, dict) else {}
        batch = data.get("value")
        if not isinstance(batch, list):
            if skip == 0:
                raise RuntimeError("Resposta SAP sem lista 'value' em Users.")
            break
        out.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    return out


@transaction.atomic(using="conversys")
def sync_users(company_id: str) -> dict[str, Any]:
    """
    Busca ``/Users`` no Service Layer e grava em ``usuarios_sap`` o JSON reduzido a:
    InternalKey, UserCode, UserName, eMail, MobilePhoneNumber, Department.
    Duplicidade: ``(company_id, dedupe_key)``.
    """
    cid = (company_id or "").strip()
    if not cid:
        raise ValueError("company_id é obrigatório.")
    if "conversys" not in settings.DATABASES:
        raise RuntimeError(
            "Configure CONVERSYS_POSTGRES_DB (banco historico_clientes) para gravar usuários SAP."
        )

    rows = fetch_all_users_pages(cid)
    inserted = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    for raw in rows:
        if not isinstance(raw, dict):
            skipped += 1
            continue
        record = _unwrap_user_record(raw)
        dk = _user_dedupe_key(record)
        if not dk:
            skipped += 1
            errors.append(f"registro sem InternalKey/UserCode: {raw!r}"[:300])
            continue
        internal_key = None
        if dk.startswith("i:"):
            try:
                internal_key = int(dk[2:])
            except ValueError:
                internal_key = None
        user_code = str(record.get("UserCode") or record.get("userCode") or "").strip()[:254]
        if not user_code and dk.startswith("u:"):
            user_code = dk[2:]

        _obj, created = UsuarioSap.objects.update_or_create(
            company_id=cid,
            dedupe_key=dk,
            defaults={
                "internal_key": internal_key,
                "user_code": user_code,
                "raw_json": _subset_user_json(record),
                "synced_at": timezone.now(),
            },
        )
        if created:
            inserted += 1
        else:
            updated += 1

    logger.info(
        "users_sync.done",
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
