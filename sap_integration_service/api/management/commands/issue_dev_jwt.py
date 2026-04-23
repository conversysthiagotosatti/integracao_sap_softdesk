"""
JWT de desenvolvimento alinhado ao .env (POSTMAN / testes locais).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Gera um JWT válido para este ambiente (JWT_SECRET_KEY, JWT_ISSUER, JWT_AUDIENCE). "
        "Cole o resultado na variável `token` do Postman (coleção SAP Integration Service)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--user-id", default="dev-user", help="Claim user_id / sub")
        parser.add_argument("--company-id", default="EMPRESA-01", help="Claim company_id (SapClientCredential)")

    def handle(self, *args, **options):
        if not settings.DEBUG:
            self.stderr.write(
                self.style.WARNING(
                    "DJANGO_DEBUG não está ativo. Este comando é pensado para desenvolvimento."
                )
            )

        secret = settings.JWT_SECRET_KEY
        issuer = settings.JWT_ISSUER
        audience = getattr(settings, "JWT_AUDIENCE", None)
        algorithm = settings.JWT_ALGORITHM

        uid = options["user_id"]
        cid = options["company_id"]
        now = datetime.now(timezone.utc)

        payload: dict = {
            "iss": issuer,
            "exp": now + timedelta(hours=24),
            "iat": now,
            "sub": uid,
            "user_id": uid,
            "company_id": cid,
        }
        if audience:
            payload["aud"] = audience

        token = jwt.encode(payload, secret, algorithm=algorithm)
        if isinstance(token, bytes):
            token = token.decode("utf-8")

        self.stdout.write(token)
        self.stdout.write(
            self.style.SUCCESS(
                "Postman: set collection variable `token` to the line above (Bearer {{token}})."
            )
        )
