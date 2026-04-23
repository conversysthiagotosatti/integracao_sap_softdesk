from django.db import models


class SapClientCredential(models.Model):
    """
    Credenciais do SAP Service Layer por cliente (tenant).
    - client_code: identificador do cliente no seu cadastro (único).
    - company_id: deve coincidir com o claim company_id do JWT (único).
    """

    client_code = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
        help_text="Código interno do cliente (ex.: código no ERP principal).",
    )
    client_name = models.CharField(
        max_length=256,
        blank=True,
        help_text="Nome legível do cliente (opcional).",
    )
    company_id = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
        help_text="Identificador enviado no JWT (claim company_id) para localizar estas credenciais.",
    )
    base_url = models.URLField(max_length=512)
    company_db = models.CharField(max_length=128)
    username = models.CharField(max_length=128)
    password_encrypted = models.TextField(help_text="Senha do Service Layer criptografada (Fernet).")
    active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sap_client_credentials"
        verbose_name = "Credencial SAP por cliente"
        verbose_name_plural = "Credenciais SAP por cliente"
        ordering = ("client_code",)

    def __str__(self) -> str:
        return f"{self.client_code} → SAP ({self.company_id})"
