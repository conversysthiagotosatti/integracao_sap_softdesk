from django.contrib import admin

from sap_queue.models import SapQueue


@admin.register(SapQueue)
class SapQueueAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "company_id",
        "user_id",
        "integration_type",
        "status",
        "retry_count",
        "next_retry_at",
        "created_at",
    )
    list_filter = ("status", "integration_type", "company_id")
    search_fields = ("company_id", "user_id", "integration_type")
    readonly_fields = (
        "company_id",
        "user_id",
        "integration_type",
        "payload",
        "status",
        "retry_count",
        "next_retry_at",
        "payload_version",
        "created_at",
    )
    ordering = ("-created_at",)

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser
