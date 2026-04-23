from django.contrib import admin

from integrations.forms import SapClientCredentialAdminForm
from integrations.models import SapClientCredential


@admin.register(SapClientCredential)
class SapClientCredentialAdmin(admin.ModelAdmin):
    form = SapClientCredentialAdminForm
    list_display = (
        "client_code",
        "client_name",
        "company_id",
        "base_url",
        "company_db",
        "username",
        "active",
        "updated_at",
    )
    list_filter = ("active",)
    search_fields = ("client_code", "client_name", "company_id", "company_db", "username")
    readonly_fields = ("created_at", "updated_at", "password_masked")
    fieldsets = (
        (
            "Cliente",
            {
                "fields": (
                    "client_code",
                    "client_name",
                    "company_id",
                )
            },
        ),
        (
            "SAP Service Layer",
            {
                "fields": (
                    "base_url",
                    "company_db",
                    "username",
                    "sap_password",
                    "password_masked",
                    "active",
                )
            },
        ),
        ("Auditoria", {"fields": ("created_at", "updated_at")}),
    )

    @admin.display(description="Senha armazenada")
    def password_masked(self, obj: SapClientCredential | None) -> str:
        if not obj or not obj.pk or not (obj.password_encrypted or "").strip():
            return "—"
        return "•••••••• (criptografada)"
