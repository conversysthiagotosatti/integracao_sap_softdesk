"""
Cliente HTTP para o **poller** Celery: API Conversys (JWT) ou Softdesk/Soft4.

O painel web de sincronização **não** usa isto — lê o PostgreSQL (``conversys_chamados_db``).
"""
from __future__ import annotations

from django.conf import settings


def conversys_list_configured() -> bool:
    base = (getattr(settings, "CONVERSYS_API_BASE_URL", "") or "").strip()
    token = (getattr(settings, "CONVERSYS_API_JWT", "") or "").strip()
    return bool(base and token)


def conversys_config_mismatch_message() -> str | None:
    """Se só metade das variáveis Conversys estiver definida, explica o erro."""
    base = (getattr(settings, "CONVERSYS_API_BASE_URL", "") or "").strip()
    token = (getattr(settings, "CONVERSYS_API_JWT", "") or "").strip()
    if base and not token:
        return (
            "CONVERSYS_API_BASE_URL está definido, mas falta CONVERSYS_API_JWT "
            "(token Bearer do login no backend Conversys)."
        )
    if token and not base:
        return "CONVERSYS_API_JWT está definido, mas falta CONVERSYS_API_BASE_URL (ex.: http://127.0.0.1:8000)."
    return None


def get_chamados_list_client():
    """
    Retorna cliente com ``fetch_chamados_page`` compatível com o poller e o painel Softdesk.
    """
    if conversys_list_configured():
        from softdesk_sync.conversys_client import ConversysHelpdeskClient

        return ConversysHelpdeskClient()
    from softdesk_sync.client import SoftdeskAPIClient

    return SoftdeskAPIClient()
