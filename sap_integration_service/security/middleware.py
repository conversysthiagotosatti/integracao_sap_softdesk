"""
Enforce JWT on every HTTP route except explicit health checks.
"""
from __future__ import annotations

import json

from django.http import JsonResponse
from rest_framework.exceptions import AuthenticationFailed

from .authentication import principal_from_claims, validate_token


class JWTAuthenticationMiddleware:
    """
    Exige ``Authorization: Bearer <JWT>`` em todas as rotas,
    exceto health público, admin Django e arquivos estáticos.
    Todas as rotas sob ``/api/`` exigem token (inclui ``/api/health/``).
    """

    EXACT_PATHS = frozenset({"/health", "/health/", "/admin"})
    PREFIX_EXEMPT = ("/admin/", "/static/", "/integrations/")

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        path = request.path or ""

        if path in self.EXACT_PATHS or any(path.startswith(p) for p in self.PREFIX_EXEMPT):
            return self.get_response(request)

        auth_header = request.META.get("HTTP_AUTHORIZATION", "")
        if not auth_header.startswith("Bearer "):
            return _unauthorized("Authorization header must be: Bearer <token>")

        token = auth_header.split(" ", 1)[1].strip()
        try:
            data = validate_token(token)
        except AuthenticationFailed as exc:
            detail = getattr(exc, "detail", str(exc))
            if isinstance(detail, (list, dict)):
                message = json.dumps(detail)
            else:
                message = str(detail)
            return _unauthorized(message)

        request.user = principal_from_claims(data)
        request.auth = token
        request.jwt_claims = data["claims"]
        return self.get_response(request)


def _unauthorized(message: str):
    body = {"detail": message, "code": "authentication_failed"}
    return JsonResponse(body, status=401)
