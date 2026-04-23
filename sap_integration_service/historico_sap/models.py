from django.db import models


class DepartamentoSap(models.Model):
    """
    Departamentos SAP (Service Layer ``Departments``).

    Persistido no PostgreSQL **historico_clientes** (alias Django ``conversys``,
    mesmas credenciais ``CONVERSYS_POSTGRES_*``).
    """

    company_id = models.CharField(max_length=64, db_index=True)
    code = models.IntegerField(help_text="Código do departamento no SAP (Code / DepartmentID).")
    name = models.CharField(max_length=512, blank=True)
    description = models.TextField(blank=True)
    payload = models.JSONField(default=dict, blank=True, help_text="Objeto bruto retornado pela API.")
    criado_em = models.DateTimeField(auto_now_add=True, db_column="criado_em")
    synced_at = models.DateTimeField(
        null=True,
        blank=True,
        db_column="atualizado_em",
        help_text="Última sincronização com o Service Layer (coluna legada ``atualizado_em``).",
    )

    class Meta:
        # Tabela no PostgreSQL ``historico_clientes`` (alias ``conversys``).
        db_table = "departamentos_sap"
        verbose_name = "Departamento SAP"
        verbose_name_plural = "Departamentos SAP"
        ordering = ("company_id", "code")
        constraints = [
            models.UniqueConstraint(
                fields=("company_id", "code"),
                name="uniq_departamentos_sap_company_code",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.company_id} / {self.code} — {self.name or '—'}"


class UsuarioSap(models.Model):
    """
    Usuários SAP (Service Layer ``Users``): JSON reduzido aos campos de negócio acordados.

    Persistido em **historico_clientes** (alias ``conversys``). Sem duplicar linhas por
    ``(company_id, dedupe_key)`` — ``dedupe_key`` deriva de ``InternalKey`` ou ``UserCode``.
    """

    company_id = models.CharField(max_length=64, db_index=True)
    dedupe_key = models.CharField(
        max_length=256,
        db_index=True,
        help_text="Chave derivada da API, ex.: i:1 ou u:MANAGER.",
    )
    internal_key = models.IntegerField(null=True, blank=True)
    user_code = models.CharField(max_length=254, blank=True)
    raw_json = models.JSONField(
        default=dict,
        help_text="JSON reduzido: InternalKey, UserCode, UserName, eMail, MobilePhoneNumber, Department.",
    )
    synced_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "usuarios_sap"
        verbose_name = "Usuário SAP (raw)"
        verbose_name_plural = "Usuários SAP (raw)"
        ordering = ("company_id", "dedupe_key")
        constraints = [
            models.UniqueConstraint(
                fields=("company_id", "dedupe_key"),
                name="uniq_usuarios_sap_company_dedupe",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.company_id} / {self.dedupe_key}"
