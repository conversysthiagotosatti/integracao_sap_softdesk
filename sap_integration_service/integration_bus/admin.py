from django.contrib import admin

from integration_bus.models import IntegrationEventReceipt


@admin.register(IntegrationEventReceipt)
class IntegrationEventReceiptAdmin(admin.ModelAdmin):
    list_display = ("created_at", "integration", "event_type", "external_id", "content_hash")
    list_filter = ("integration", "event_type")
    search_fields = ("external_id", "dedup_key")
    readonly_fields = ("dedup_key", "created_at")
