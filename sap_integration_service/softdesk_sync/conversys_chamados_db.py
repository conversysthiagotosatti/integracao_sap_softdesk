"""
Leitura direta da tabela de chamados do PostgreSQL do Conversys (sem HTTP).

Requer conexão Django ``conversys`` em ``settings.DATABASES`` (via CONVERSYS_POSTGRES_*).
Tabela padrão: ``helpdesk_chamado`` (model ``helpdesk.Chamado`` do historico_clientes_backend).
"""
from __future__ import annotations

import logging
import re
from typing import Any

from django.conf import settings
from django.db import connections

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def fetch_chamados_from_conversys_db() -> tuple[list[dict[str, Any]], str | None]:
    """
    Retorna (lista de dicts no formato esperado pelo painel, mensagem de erro ou None).
    """
    if "conversys" not in settings.DATABASES:
        return [], (
            "Configure a leitura do banco Conversys: defina CONVERSYS_POSTGRES_DB no .env "
            "(mesmo banco do ambiente-conversys, ex.: historico_clientes)."
        )

    table = (getattr(settings, "CONVERSYS_CHAMADOS_TABLE", "helpdesk_chamado") or "helpdesk_chamado").strip()
    if not _TABLE_RE.match(table):
        return [], "CONVERSYS_CHAMADOS_TABLE inválido (use apenas letras, números e _)."

    limit = max(1, min(int(getattr(settings, "CONVERSYS_CHAMADOS_QUERY_LIMIT", "2000")), 50_000))

    conn = connections["conversys"]
    qn = conn.ops.quote_name
    cols = ("id", "titulo", "status", "codigo_helpdesk_api")
    select_sql = ", ".join(qn(c) for c in cols)
    code_col = qn("codigo_helpdesk_api")
    # Painel: só chamados com código helpdesk preenchido (alinhado ao filtro em sync_dashboard).
    where_code = f"WHERE {code_col} IS NOT NULL AND BTRIM({code_col}) <> ''"
    sql = (
        f"SELECT {select_sql} FROM {qn(table)} {where_code} "
        f"ORDER BY {qn('atualizado_em')} DESC NULLS LAST, {qn('id')} DESC "
        f"LIMIT %s"
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, [limit])
            names = [c[0] for c in cursor.description]
            out: list[dict[str, Any]] = []
            for row in cursor.fetchall():
                item = dict(zip(names, row))
                out.append(item)
    except Exception as exc:  # noqa: BLE001
        logger.exception("conversys_chamados_db.query_failed")
        return [], f"Erro ao consultar o banco Conversys: {exc}"

    return out, None
