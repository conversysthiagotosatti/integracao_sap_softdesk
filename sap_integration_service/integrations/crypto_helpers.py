from __future__ import annotations

from django.conf import settings
from cryptography.fernet import Fernet, InvalidToken


def _fernet() -> Fernet:
    key = (settings.SAP_ENCRYPTION_KEY or "").strip()
    if not key:
        raise RuntimeError(
            "Defina SAP_ENCRYPTION_KEY no .env (chave Fernet em base64) e reinicie o Django. "
            "Gere uma chave com: python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        )
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt_secret(plain: str) -> str:
    return _fernet().encrypt(plain.encode("utf-8")).decode("utf-8")


def decrypt_secret(token: str) -> str:
    try:
        return _fernet().decrypt(token.encode("utf-8")).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError("Could not decrypt SAP secret.") from exc
