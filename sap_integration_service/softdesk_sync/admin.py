from django.contrib import admin

from softdesk_sync.models import SoftdeskChamadoState, SoftdeskSyncState


@admin.register(SoftdeskSyncState)
class SoftdeskSyncStateAdmin(admin.ModelAdmin):
    list_display = ("scope", "watermark", "consecutive_failures", "last_poll_finished_at")
    readonly_fields = ("updated_at",)


@admin.register(SoftdeskChamadoState)
class SoftdeskChamadoStateAdmin(admin.ModelAdmin):
    list_display = ("external_id", "content_hash", "remote_updated_at", "updated_at")
    search_fields = ("external_id",)
