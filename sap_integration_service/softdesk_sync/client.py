"""
HTTP client for Softdesk REST API (no webhooks — polling only).

Contract is intentionally defensive: response shapes vary by tenant/version.
Configure mapping via Django settings (see ``core.settings`` SOFTDESK_*).
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


class SoftdeskAPIClient:
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
        return float(getattr(settings, "SOFTDESK_HTTP_TIMEOUT_SECONDS", 5))

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        token = getattr(settings, "SOFTDESK_API_TOKEN", "") or ""
        if token:
            headers["Authorization"] = f"Bearer {token}"
        api_key = getattr(settings, "SOFTDESK_API_KEY", "") or ""
        if api_key and "Authorization" not in headers:
            headers["X-API-Key"] = api_key
        hash_api = (getattr(settings, "SOFTDESK_LIST_HASH_API", "") or "").strip()
        if not hash_api:
            hash_api = (getattr(settings, "SOFTDESK_DOSSIE_HASH_API", "") or "").strip()
        if hash_api:
            headers["hash-api"] = hash_api
        return headers

    def _base_url(self) -> str:
        base = (getattr(settings, "SOFTDESK_BASE_URL", "") or "").strip().rstrip("/")
        if not base:
            raise SoftdeskAPIError("SOFTDESK_BASE_URL is not configured.")
        return base + "/"

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
            logger.exception(
                "softdesk.http.error",
                extra={"path": path, "method": method, "error": str(exc)},
            )
            raise SoftdeskAPIError(str(exc)) from exc

        if resp.status_code == 429:
            raise SoftdeskRateLimitError("HTTP 429 from Softdesk")
        if resp.status_code >= 400:
            body = (resp.text or "")[:2000]
            logger.warning(
                "softdesk.http.status",
                extra={"path": path, "status_code": resp.status_code, "body_preview": body},
            )
            raise SoftdeskAPIError(f"HTTP {resp.status_code}: {body}")

        if not resp.content:
            return None
        try:
            return resp.json()
        except ValueError as exc:
            raise SoftdeskAPIError("Invalid JSON from Softdesk") from exc

    def fetch_chamados_page(
        self,
        *,
        page: int | None = None,
        page_size: int | None = None,
        updated_since: str | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> Any:
        rel = getattr(settings, "SOFTDESK_CHAMADOS_PATH", "api/api.php/chamados").strip().lstrip("/")
        params: dict[str, Any] = dict(extra_params or {})
        page_param = getattr(settings, "SOFTDESK_PAGE_QUERY_PARAM", "page")
        size_param = getattr(settings, "SOFTDESK_PAGE_SIZE_QUERY_PARAM", "page_size")
        since_param = getattr(settings, "SOFTDESK_UPDATED_SINCE_QUERY_PARAM", "updated_since")
        if page is not None:
            params[page_param] = page
        if page_size is not None:
            params[size_param] = page_size
        if updated_since:
            params[since_param] = updated_since
        return self.request_json("GET", rel, params=params)
