from __future__ import annotations

import logging

from django.http import JsonResponse
from django.views.decorators.http import require_GET
from rest_framework import status
from rest_framework.response import Response

from integrations import sap_service
from integrations.metrics import snapshot_counters
from logs.models import SapIntegrationLog
from sap_queue.models import SapQueue
from sap_queue.queue_service import enqueue
from security.authentication import ServicePrincipal

from .base import JWTAPIView
from .serializers import SapIntegrationLogSerializer, SapQueueSerializer, SapSendSerializer

logger = logging.getLogger(__name__)


def _principal(request) -> ServicePrincipal:
    user = request.user
    if not isinstance(user, ServicePrincipal):
        raise AssertionError("Expected authenticated ServicePrincipal.")
    return user


@require_GET
def health(request):
    """Somente monitoramento (load balancer); não exige JWT — use /api/health/ na integração."""
    return JsonResponse(
        {
            "status": "ok",
            "service": "sap-integration",
            "metrics": snapshot_counters(),
        }
    )


class SapHealthAPIView(JWTAPIView):
    """Health check autenticado (mesmo contrato de token das demais APIs)."""

    def get(self, request, *args, **kwargs):
        return Response(
            {
                "status": "ok",
                "service": "sap-integration",
                "metrics": snapshot_counters(),
            }
        )


class SapSendView(JWTAPIView):
    def post(self, request, *args, **kwargs):
        ser = SapSendSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data
        principal = _principal(request)

        if str(data["payload"].get("company_id") or "") not in ("", str(principal.company_id)):
            return Response(
                {"detail": "payload.company_id must match token company_id.", "code": "company_mismatch"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        itype = data["type"]
        payload = data["payload"]
        mode = data["mode"]
        pv = data.get("payload_version") or None

        if mode == "async":
            item = enqueue(
                principal.company_id,
                itype,
                payload,
                user_id=principal.user_id,
                payload_version=pv or "",
            )
            # Auditoria imediata: fila não passa por send_* aqui; o registro fica em SapQueue.
            # SapIntegrationLog documenta o aceite (202) até o worker processar a fila.
            audit = SapIntegrationLog.objects.create(
                company_id=principal.company_id,
                user_id=principal.user_id,
                integration_type=itype,
                method="POST",
                endpoint="AsyncQueue",
                request_payload=payload,
                response_payload={"queued_id": item.id, "status": item.status},
                status=SapIntegrationLog.Status.PENDING,
                http_status=status.HTTP_202_ACCEPTED,
                external_id=f"async-queue-{item.id}",
                payload_version=pv or "",
            )
            return Response(
                {
                    "queued_id": item.id,
                    "status": item.status,
                    "integration_log_id": audit.id,
                },
                status=status.HTTP_202_ACCEPTED,
            )

        if itype == sap_service.TYPE_PURCHASE_INVOICE:
            log = sap_service.send_purchase_invoice(
                principal.company_id,
                principal.user_id,
                payload,
                payload_version=pv,
            )
        else:
            log = sap_service.send_business_partner(
                principal.company_id,
                principal.user_id,
                payload,
                payload_version=pv,
            )

        http_status = status.HTTP_200_OK if log.status == SapIntegrationLog.Status.SUCCESS else status.HTTP_502_BAD_GATEWAY
        body = SapIntegrationLogSerializer(instance=log).data
        return Response(body, status=http_status)


class SapLogListView(JWTAPIView):
    def get(self, request, *args, **kwargs):
        principal = _principal(request)
        qs = SapIntegrationLog.objects.filter(company_id=principal.company_id).order_by("-created_at")[:200]
        data = [SapIntegrationLogSerializer(instance=o).data for o in qs]
        return Response(data)


class SapLogDetailView(JWTAPIView):
    def get(self, request, log_id: int, *args, **kwargs):
        principal = _principal(request)
        try:
            obj = SapIntegrationLog.objects.get(id=log_id, company_id=principal.company_id)
        except SapIntegrationLog.DoesNotExist:
            return Response({"detail": "Not found.", "code": "not_found"}, status=status.HTTP_404_NOT_FOUND)
        return Response(SapIntegrationLogSerializer(instance=obj).data)


class SapQueueListView(JWTAPIView):
    def get(self, request, *args, **kwargs):
        principal = _principal(request)
        qs = SapQueue.objects.filter(company_id=principal.company_id).order_by("-created_at")[:200]
        data = [SapQueueSerializer(instance=o).data for o in qs]
        return Response(data)
