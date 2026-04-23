from rest_framework import serializers

from integrations import sap_service
from logs.models import SapIntegrationLog
from sap_queue.models import SapQueue


class SapSendSerializer(serializers.Serializer):
    type = serializers.ChoiceField(
        choices=[
            sap_service.TYPE_PURCHASE_INVOICE,
            sap_service.TYPE_BUSINESS_PARTNER,
        ]
    )
    payload = serializers.DictField()
    mode = serializers.ChoiceField(choices=["sync", "async"], default="sync")
    payload_version = serializers.CharField(required=False, allow_blank=True, max_length=16)


class SapIntegrationLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = SapIntegrationLog
        fields = (
            "id",
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


class SapQueueSerializer(serializers.ModelSerializer):
    class Meta:
        model = SapQueue
        fields = (
            "id",
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
