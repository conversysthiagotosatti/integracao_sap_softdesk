from __future__ import annotations

import logging

from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_protect

from sap_queue.queue_service import process_queue_item

logger = logging.getLogger(__name__)


@method_decorator(csrf_protect, name="dispatch")
class ProcessQueueItemView(View):
    """POST: integra ao SAP apenas o item da fila indicado na URL."""

    http_method_names = ["post"]

    def post(self, request: HttpRequest, item_id: int) -> HttpResponse:
        company = (request.POST.get("company_id") or "").strip() or None

        try:
            result = process_queue_item(item_id=item_id, company_id=company)
        except Exception as exc:  # noqa: BLE001
            logger.exception("process_queue_item failed for id=%s", item_id)
            messages.error(request, f"Falha ao integrar item #{item_id}: {exc}")
        else:
            if result.get("ok"):
                messages.success(request, result["message"])
            else:
                messages.error(request, result.get("message", "Erro desconhecido."))

        url = reverse("integration-dashboard")
        if company:
            url = f"{url}?company_id={company}"
        return redirect(url)
