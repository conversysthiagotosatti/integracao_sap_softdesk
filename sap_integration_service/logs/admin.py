from django.contrib import admin

from logs.models import SapIntegrationLog


@admin.register(SapIntegrationLog)
class SapIntegrationLogAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "company_id",
        "user_id",
        "integration_type",
        "status",
        "http_status",
        "retry_count",
        "external_id",
        "created_at",
    )
    list_filter = ("status", "integration_type", "company_id")
    search_fields = ("company_id", "user_id", "external_id", "endpoint")
    readonly_fields = (
        "company_id",
        "user_id",
        "integration_type",
        "method",
        "endpoint",
        "request_payload",
        "response_payload",
        "status",
        "http_status",
        "error_message",
        "retry_count",
        "external_id",
        "payload_version",
        "created_at",
        "updated_at",
    )
    ordering = ("-created_at",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return True

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser
