"""
Fetch chamado dossie from Soft4 / Softdesk HTTP endpoint (RetornaDossie).
"""
from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


def build_dossie_url(chamado: str) -> str:
    base = (getattr(settings, "SOFTDESK_DOSSIE_BASE_URL", "") or "").strip().rstrip("/")
    if not base:
        base = "https://conversys.soft4.com.br"
    rel = getattr(settings, "SOFTDESK_DOSSIE_PATH", "api/api.php/chamado").strip().lstrip("/")
    code = quote(str(chamado).strip(), safe="")
    flag = getattr(settings, "SOFTDESK_DOSSIE_FLAG_PARAM", "RetornaDossie")
    chamado_param = getattr(settings, "SOFTDESK_DOSSIE_CHAMADO_PARAM", "chamado")
    return f"{base}/{rel}?{flag}&{chamado_param}={code}"


def fetch_dossie(chamado: str) -> dict[str, Any]:
    """
    GET dossie JSON (or wrap non-JSON body). Raises on transport errors.
    """
    url = build_dossie_url(chamado)
    timeout = float(getattr(settings, "SOFTDESK_DOSSIE_TIMEOUT_SECONDS", "30"))
    headers: dict[str, str] = {}
    hash_api = (getattr(settings, "SOFTDESK_DOSSIE_HASH_API", "") or "").strip()
    if hash_api:
        headers["hash-api"] = hash_api
    token = (getattr(settings, "SOFTDESK_DOSSIE_BEARER_TOKEN", "") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        logger.exception("softdesk.dossie.request_failed", extra={"url": url})
        raise RuntimeError(f"Falha na requisição: {exc}") from exc

    text = (resp.text or "").strip()
    out: dict[str, Any] = {
        "_http_status": resp.status_code,
        "_request_url": url,
    }
    if resp.status_code >= 400:
        out["_error"] = text[:8000] if text else f"HTTP {resp.status_code}"
        return out

    if not text:
        out["_empty_body"] = True
        return out

    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "json" in ctype or text.startswith("{") or text.startswith("["):
        try:
            parsed: Any = resp.json()
            if isinstance(parsed, dict):
                return parsed
            return {"_data": parsed}
        except ValueError:
            pass

    out["_raw_body"] = text[:50000]
    return out
