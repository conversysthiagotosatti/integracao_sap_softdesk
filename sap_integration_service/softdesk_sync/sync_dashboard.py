"""
Load chamados from Softdesk list API for the sync UI (filter by helpdesk code + open status).
"""
from __future__ import annotations

from typing import Any

from django.conf import settings

from softdesk_sync.conversys_chamados_db import fetch_chamados_from_conversys_db


def _closed_status_values() -> set[str]:
    raw = getattr(settings, "SOFTDESK_CHAMADO_FECHADO_VALUES", "fechado") or "fechado"
    return {x.strip().lower() for x in str(raw).split(",") if x.strip()}


def _closed_status_ids() -> set[str]:
    raw = getattr(settings, "SOFTDESK_CHAMADO_FECHADO_STATUS_IDS", "") or ""
    return {x.strip() for x in str(raw).split(",") if x.strip()}


def _helpdesk_code_field() -> str:
    return getattr(settings, "SOFTDESK_CHAMADO_CODIGO_HELPDESK_FIELD", "codigo_helpdesk_api")


def _status_field() -> str:
    return getattr(settings, "SOFTDESK_CHAMADO_STATUS_FIELD", "status")


def _title_fields() -> tuple[str, ...]:
    raw = getattr(settings, "SOFTDESK_CHAMADO_TITLE_FIELDS", "titulo,assunto,descricao_resumida,subject")
    return tuple(x.strip() for x in str(raw).split(",") if x.strip())


def _pick_title(item: dict[str, Any]) -> str:
    for k in _title_fields():
        v = item.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()[:200]
    return "—"


def _include_item(item: dict[str, Any]) -> bool:
    code_f = _helpdesk_code_field()
    raw_code = item.get(code_f)
    if raw_code is None or str(raw_code).strip() == "":
        return False

    status_f = _status_field()
    status_val = item.get(status_f)
    if status_val is None or str(status_val).strip() == "":
        return True

    closed_ids = _closed_status_ids()
    if closed_ids and str(status_val).strip() in closed_ids:
        return False

    return str(status_val).strip().lower() not in _closed_status_values()


def load_sync_table_chamados() -> tuple[list[dict[str, Any]], str | None]:
    """
    Monta as linhas do painel a partir do PostgreSQL do Conversys (tabela ``helpdesk_chamado``).

    Each row: { "item", "codigo", "titulo", "status_display", "id_display" } — só entra quem tem ``codigo_helpdesk_api``.
    """
    items, err = fetch_chamados_from_conversys_db()
    if err:
        return [], err

    code_f = _helpdesk_code_field()
    status_f = _status_field()
    id_f = getattr(settings, "SOFTDESK_CHAMADO_ID_FIELD", "id")

    rows: list[dict[str, Any]] = []
    for it in items:
        if not _include_item(it):
            continue
        cid = it.get(id_f)
        codigo = str(it.get(code_f) or "").strip()
        rows.append(
            {
                "item": it,
                "codigo": codigo,
                "titulo": _pick_title(it),
                "status_display": str(it.get(status_f) or "—"),
                "id_display": str(cid) if cid is not None else "—",
            }
        )

    return rows, None
