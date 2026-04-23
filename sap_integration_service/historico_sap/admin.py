from django.contrib import admin

from historico_sap.models import DepartamentoSap, UsuarioSap


@admin.register(DepartamentoSap)
class DepartamentoSapAdmin(admin.ModelAdmin):
    list_display = ("company_id", "code", "name", "criado_em", "synced_at")
    list_filter = ("company_id",)
    search_fields = ("company_id", "name", "description")
    readonly_fields = ("payload", "criado_em", "synced_at")


@admin.register(UsuarioSap)
class UsuarioSapAdmin(admin.ModelAdmin):
    list_display = ("company_id", "dedupe_key", "internal_key", "user_code", "synced_at")
    list_filter = ("company_id",)
    search_fields = ("company_id", "dedupe_key", "user_code")
    readonly_fields = ("raw_json", "synced_at")
