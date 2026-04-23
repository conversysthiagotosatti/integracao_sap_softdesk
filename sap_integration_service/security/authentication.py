"""
JWT validation for tokens issued by the main system.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import jwt
from django.conf import settings
from rest_framework import authentication, exceptions


@dataclass(frozen=True)
class ServicePrincipal:
    """Represents the caller authenticated via main-system JWT."""

    user_id: str
    company_id: str
    claims: dict[str, Any]

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def pk(self) -> str:
        return self.user_id


def validate_token(token: str) -> dict[str, Any]:
    """
    Validate JWT signature, expiration, issuer; extract user_id and company_id.

    Expected claims (customizable via JWT claim names):
      - sub or user_id
      - company_id
      - iss (must match settings.JWT_ISSUER)
      - aud (optional, must match settings.JWT_AUDIENCE if present)
    """
    if not token or not isinstance(token, str):
        raise exceptions.AuthenticationFailed("Missing token.")

    algorithm = settings.JWT_ALGORITHM
    secret = settings.JWT_SECRET_KEY
    issuer = settings.JWT_ISSUER
    audience = getattr(settings, "JWT_AUDIENCE", None)

    decode_kwargs: dict[str, Any] = {
        "algorithms": [algorithm],
        "issuer": issuer,
        "options": {"require": ["exp", "iss"]},
    }
    if audience:
        decode_kwargs["audience"] = audience

    try:
        payload = jwt.decode(token, secret, **decode_kwargs)
    except jwt.ExpiredSignatureError as exc:
        raise exceptions.AuthenticationFailed("Token expirado.") from exc
    except jwt.InvalidSignatureError as exc:
        raise exceptions.AuthenticationFailed(
            "Assinatura do token inválida. Confira JWT_SECRET_KEY no .env e se o token foi emitido com o mesmo segredo."
        ) from exc
    except jwt.InvalidIssuerError as exc:
        raise exceptions.AuthenticationFailed(
            f"Emissor (iss) do token inválido ou ausente. O token deve conter iss=\"{issuer}\"."
        ) from exc
    except jwt.MissingRequiredClaimError as exc:
        raise exceptions.AuthenticationFailed(
            "Token sem claim obrigatória (ex.: iss ou exp). Gere o JWT com iss e exp."
        ) from exc
    except jwt.InvalidTokenError as exc:
        raise exceptions.AuthenticationFailed(
            "Token inválido (formato, algoritmo ou audiência). Verifique JWT_ALGORITHM e JWT_AUDIENCE no .env."
        ) from exc

    user_id = str(payload.get("user_id") or payload.get("sub") or "")
    company_id = str(payload.get("company_id") or "")

    if not user_id or not company_id:
        raise exceptions.AuthenticationFailed("Token missing user_id or company_id.")

    return {
        "user_id": user_id,
        "company_id": company_id,
        "claims": payload,
    }


def principal_from_claims(data: dict[str, Any]) -> ServicePrincipal:
    return ServicePrincipal(
        user_id=data["user_id"],
        company_id=data["company_id"],
        claims=data.get("claims") or {},
    )


class BearerJWTAuthentication(authentication.BaseAuthentication):
    """DRF authentication using Authorization: Bearer <jwt>."""

    keyword = "Bearer"

    def authenticate(self, request):
        django_request = getattr(request, "_request", request)
        existing = getattr(django_request, "user", None)
        if isinstance(existing, ServicePrincipal):
            token = getattr(django_request, "auth", None)
            return existing, token

        auth_header = authentication.get_authorization_header(request)
        if not auth_header:
            return None

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].decode().lower() != self.keyword.lower():
            return None

        token = parts[1].decode()
        data = validate_token(token)
        principal = principal_from_claims(data)
        return principal, token

    def authenticate_header(self, request):
        return self.keyword
