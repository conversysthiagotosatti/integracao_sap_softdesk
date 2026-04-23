"""
SAP Business One Service Layer HTTP client (session + requests).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests
from django.conf import settings

from integrations.crypto_helpers import decrypt_secret
from integrations.models import SapClientCredential

logger = logging.getLogger(__name__)

_sessions: dict[str, requests.Session] = {}
_ssl_insecure_logged = False


def _requests_verify() -> bool | str:
    """Caminho PEM da CA corporativa, ou True/False conforme settings."""
    ca = getattr(settings, "SAP_SSL_CA_BUNDLE", None)
    if ca:
        return ca
    if not getattr(settings, "SAP_SSL_VERIFY", True):
        return False
    return True


def _apply_ssl_to_session(session: requests.Session) -> None:
    global _ssl_insecure_logged
    verify = _requests_verify()
    session.verify = verify
    if verify is False:
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        if not _ssl_insecure_logged:
            _ssl_insecure_logged = True
            logger.warning(
                "SAP_SSL_VERIFY está desativado: conexões ao Service Layer não validam certificado TLS."
            )


@dataclass
class SapResponse:
    status_code: int
    data: Any
    raw_text: str


def _base_url(config: SapClientCredential) -> str:
    """
    Normaliza a URL base do Service Layer (v1 ou v2).

    Ex.: ``https://localhost:50000/b1s/v2`` ou só ``https://localhost:50000``
    (usa ``SAP_DEFAULT_B1S_PATH`` do settings, padrão ``b1s/v2``).
    """
    raw = (config.base_url or "").strip().rstrip("/")
    if not raw:
        raise ValueError("SapClientCredential.base_url is empty.")
    lower = raw.lower()
    if "b1s/v2" in lower or lower.endswith("/v2") and "/b1s" in lower:
        return raw + ("/" if not raw.endswith("/") else "")
    if lower.endswith("/v1") or "b1s/v1" in lower:
        return raw + ("/" if not raw.endswith("/") else "")
    default_path = getattr(settings, "SAP_DEFAULT_B1S_PATH", "b1s/v2") or "b1s/v2"
    return urljoin(raw + "/", default_path.strip("/") + "/")


def login(company_id: str) -> requests.Session:
    """
    Load tenant SAP config, authenticate against Service Layer, return session with cookies.
    """
    config = (
        SapClientCredential.objects.filter(company_id=str(company_id), active=True)
        .order_by("-updated_at")
        .first()
    )
    if not config:
        raise ValueError(f"No active SAP credentials for company_id={company_id}.")

    password = decrypt_secret(config.password_encrypted)
    base = _base_url(config).rstrip("/")
    login_url = f"{base}/Login"

    session = requests.Session()
    _apply_ssl_to_session(session)
    session.headers.update(
        {
            "Content-Type": "application/json",
            "B1S-ReplaceCollectionsOnPatch": "true",
        }
    )

    body = {
        "CompanyDB": config.company_db,
        "UserName": config.username,
        "Password": password,
    }

    verify = _requests_verify()
    resp = session.post(login_url, json=body, timeout=60, verify=verify)
    if resp.status_code >= 400:
        logger.warning("SAP login failed for company_id=%s status=%s", company_id, resp.status_code)
        raise RuntimeError(f"SAP login failed: HTTP {resp.status_code}")

    _sessions[str(company_id)] = session
    return session


def _session_for(company_id: str) -> requests.Session:
    sid = str(company_id)
    if sid not in _sessions:
        return login(company_id)
    return _sessions[sid]


def request(
    company_id: str,
    method: str,
    endpoint: str,
    payload: dict | list | None = None,
) -> SapResponse:
    """
    Ensure SL session and perform HTTP call. Endpoint is relativo à base (ex.: ``.../b1s/v2/``).
    """
    config = (
        SapClientCredential.objects.filter(company_id=str(company_id), active=True)
        .order_by("-updated_at")
        .first()
    )
    if not config:
        raise ValueError(f"No active SAP credentials for company_id={company_id}.")

    base = _base_url(config).rstrip("/")
    url = urljoin(base + "/", endpoint.lstrip("/"))

    session = _session_for(company_id)
    method_u = method.upper()

    verify = _requests_verify()

    def _do() -> requests.Response:
        if method_u in ("GET", "DELETE"):
            return session.request(method_u, url, timeout=120, verify=verify)
        return session.request(
            method_u,
            url,
            json=payload if payload is not None else {},
            timeout=120,
            verify=verify,
        )

    resp = _do()
    if resp.status_code == 401:
        login(company_id)
        session = _session_for(company_id)
        resp = _do()

    text = resp.text or ""
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": text}

    return SapResponse(status_code=resp.status_code, data=data, raw_text=text)


def clear_session(company_id: str) -> None:
    _sessions.pop(str(company_id), None)
