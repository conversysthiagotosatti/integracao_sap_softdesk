"""
Cliente da API de chamados do backend Conversys (ambiente-conversys / historico_clientes_backend).

GET {CONVERSYS_API_BASE_URL}/api/helpdesk/chamados/ — autenticação JWT Bearer (SimpleJWT).
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urljoin

import requests
from django.conf import settings

from softdesk_sync.exceptions import SoftdeskAPIError, SoftdeskRateLimitError
from softdesk_sync.rate_limit import MinuteWindowRateLimiter

logger = logging.getLogger(__name__)


class ConversysHelpdeskClient:
    """Mesma ideia de ``SoftdeskAPIClient.fetch_chamados_page`` para respostas DRF."""

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        rate_limiter: MinuteWindowRateLimiter | None = None,
    ) -> None:
        self._session = session or requests.Session()
        self._rate_limiter = rate_limiter or MinuteWindowRateLimiter(
            key_prefix=getattr(settings, "SOFTDESK_RL_CACHE_PREFIX", "softdesk:rl"),
            max_requests=int(getattr(settings, "SOFTDESK_MAX_REQUESTS_PER_MINUTE", 60)),
        )

    def _timeout(self) -> float:
        return float(getattr(settings, "CONVERSYS_API_TIMEOUT_SECONDS", "30"))

    def _base_url(self) -> str:
        base = (getattr(settings, "CONVERSYS_API_BASE_URL", "") or "").strip().rstrip("/")
        if not base:
            raise SoftdeskAPIError("CONVERSYS_API_BASE_URL is not configured.")
        return base + "/"

    def _headers(self) -> dict[str, str]:
        token = (getattr(settings, "CONVERSYS_API_JWT", "") or "").strip()
        if not token:
            raise SoftdeskAPIError("CONVERSYS_API_JWT is not configured.")
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

    def _chamados_path(self) -> str:
        return getattr(settings, "CONVERSYS_HELPDESK_CHAMADOS_PATH", "api/helpdesk/chamados").strip().lstrip("/")

    def request_json(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> Any:
        self._rate_limiter.acquire()
        url = urljoin(self._base_url(), path.lstrip("/"))
        try:
            resp = self._session.request(
                method.upper(),
                url,
                headers=self._headers(),
                params=params or {},
                timeout=self._timeout(),
            )
        except requests.RequestException as exc:
            logger.exception("conversys.helpdesk.http.error", extra={"path": path, "error": str(exc)})
            raise SoftdeskAPIError(str(exc)) from exc

        if resp.status_code == 429:
            raise SoftdeskRateLimitError("HTTP 429 from Conversys API")
        if resp.status_code >= 400:
            body = (resp.text or "")[:2000]
            logger.warning(
                "conversys.helpdesk.http.status",
                extra={"path": path, "status_code": resp.status_code, "body_preview": body},
            )
            raise SoftdeskAPIError(f"HTTP {resp.status_code}: {body}")

        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as exc:
            raise SoftdeskAPIError("Invalid JSON from Conversys API") from exc

    def fetch_chamados_page(
        self,
        *,
        page: int | None = None,
        page_size: int | None = None,
        updated_since: str | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> Any:
        rel = self._chamados_path()
        params: dict[str, Any] = dict(extra_params or {})
        page_param = getattr(settings, "SOFTDESK_PAGE_QUERY_PARAM", "page")
        size_param = getattr(settings, "SOFTDESK_PAGE_SIZE_QUERY_PARAM", "page_size")
        since_param = getattr(settings, "SOFTDESK_UPDATED_SINCE_QUERY_PARAM", "updated_since")
        if page is not None:
            params[page_param] = page
        if page_size is not None:
            params[size_param] = page_size
        # O ViewSet do Conversys não usa o mesmo filtro incremental do Soft4.
        if updated_since and getattr(settings, "CONVERSYS_LIST_SEND_UPDATED_SINCE", False):
            params[since_param] = updated_since
        return self.request_json("GET", rel, params=params)
