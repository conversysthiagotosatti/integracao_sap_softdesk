from __future__ import annotations

import logging
import time
from datetime import timedelta

from django.conf import settings
from django.contrib import messages
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import timezone
from django.utils.decorators import method_decorator
from django.views import View
from django.views.decorators.csrf import csrf_protect
from django.views.generic import TemplateView

from historico_sap.models import DepartamentoSap, UsuarioSap
from integrations.departments_sync import sync_departments
from integrations.users_conversys_sync import sync_usuario_sap_to_conversys_users
from integrations.users_sync import sync_users
from logs.models import SapIntegrationLog
from sap_queue.models import SapQueue
from softdesk_sync.dossie import fetch_dossie
from softdesk_sync.dossie_sync import sync_chamado_from_dossie
from softdesk_sync.sync_dashboard import load_sync_table_chamados

logger = logging.getLogger(__name__)


class IntegrationDashboardView(TemplateView):
    """Acompanhamento de logs e fila — acesso público (sem login)."""

    template_name = "monitoring/dashboard.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        company = (self.request.GET.get("company_id") or "").strip()

        logs_qs = SapIntegrationLog.objects.all().order_by("-created_at")
        queue_qs = SapQueue.objects.all().order_by("-created_at")
        if company:
            logs_qs = logs_qs.filter(company_id=company)
            queue_qs = queue_qs.filter(company_id=company)

        ctx["company_filter"] = company
        ctx["logs"] = list(logs_qs[:100])
        ctx["queue"] = list(queue_qs[:100])
        since = timezone.now() - timedelta(hours=24)
        ctx["stats"] = {
            "queue_pending": SapQueue.objects.filter(status=SapQueue.Status.PENDING).count(),
            "queue_error": SapQueue.objects.filter(status=SapQueue.Status.ERROR).count(),
            "logs_error_24h": SapIntegrationLog.objects.filter(
                status=SapIntegrationLog.Status.ERROR,
                created_at__gte=since,
            ).count(),
        }
        if company and "conversys" in settings.DATABASES:
            ctx["departments_count"] = DepartamentoSap.objects.filter(company_id=company).count()
            ctx["users_sap_count"] = UsuarioSap.objects.filter(company_id=company).count()
        else:
            ctx["departments_count"] = None
            ctx["users_sap_count"] = None
        return ctx


@method_decorator(csrf_protect, name="dispatch")
class IntegrationDepartmentsSyncView(View):
    """POST: GET ``Departments`` no Service Layer e persiste em ``departamentos_sap`` (historico_clientes)."""

    http_method_names = ["post"]

    def post(self, request: HttpRequest) -> HttpResponse:
        company = (request.POST.get("company_id") or "").strip()
        if not company:
            messages.error(request, "Informe o company_id (mesmo do JWT / credencial SAP).")
            return redirect(reverse("integration-dashboard"))

        try:
            result = sync_departments(company)
        except ValueError as exc:
            messages.error(request, str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("departments_sync failed company_id=%s", company)
            messages.error(request, f"Falha na sincronização de departamentos: {exc}")
        else:
            msg = (
                f"Departamentos sincronizados para «{result['company_id']}»: "
                f"{result['inserted']} novo(s), {result['updated']} atualizado(s), "
                f"{result['skipped']} ignorado(s); {result['total_api']} registro(s) na API."
            )
            if result.get("errors_sample"):
                msg += f" Avisos: {result['errors_sample'][:3]}"
            messages.success(request, msg)

        return redirect(f"{reverse('integration-dashboard')}?company_id={company}")


@method_decorator(csrf_protect, name="dispatch")
class IntegrationUsersSyncView(View):
    """POST: GET ``Users`` no Service Layer e persiste JSON bruto em ``usuarios_sap``."""

    http_method_names = ["post"]

    def post(self, request: HttpRequest) -> HttpResponse:
        company = (request.POST.get("company_id") or "").strip()
        if not company:
            messages.error(request, "Informe o company_id (mesmo do JWT / credencial SAP).")
            return redirect(reverse("integration-dashboard"))

        try:
            result = sync_users(company)
        except ValueError as exc:
            messages.error(request, str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("users_sync failed company_id=%s", company)
            messages.error(request, f"Falha na sincronização de usuários SAP: {exc}")
        else:
            msg = (
                f"Usuários SAP sincronizados para «{result['company_id']}»: "
                f"{result['inserted']} novo(s), {result['updated']} atualizado(s), "
                f"{result['skipped']} ignorado(s); {result['total_api']} registro(s) na API."
            )
            if result.get("errors_sample"):
                msg += f" Avisos: {result['errors_sample'][:3]}"
            messages.success(request, msg)

        return redirect(f"{reverse('integration-dashboard')}?company_id={company}")


@method_decorator(csrf_protect, name="dispatch")
class IntegrationUsersConversysLinkView(View):
    """POST: ``usuarios_sap`` → tabela de usuários do Conversys (inclusão/alteração)."""

    http_method_names = ["post"]

    def post(self, request: HttpRequest) -> HttpResponse:
        company = (request.POST.get("company_id") or "").strip()
        if not company:
            messages.error(request, "Informe o company_id.")
            return redirect(reverse("integration-dashboard"))

        try:
            result = sync_usuario_sap_to_conversys_users(company)
        except ValueError as exc:
            messages.error(request, str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.exception("users_conversys_sync failed company_id=%s", company)
            messages.error(request, f"Falha ao propagar usuários para o Conversys: {exc}")
        else:
            link = result.get("userprofile_link", "")
            link_part = f" [{link}]" if link else ""
            msg = (
                f"«{result['user_table']}» + «{result['userprofile_table']}»{link_part}: "
                f"{result['inserted']} inclusão(ões), {result['updated']} alteração(ões), "
                f"{result['skipped']} ignorado(s)."
            )
            if result.get("errors_sample"):
                msg += f" Detalhes: {result['errors_sample'][:2]}"
            messages.success(request, msg)

        return redirect(f"{reverse('integration-dashboard')}?company_id={company}")


class SoftdeskSyncDashboardView(TemplateView):
    """Chamados com código helpdesk + abertos; botão chama API RetornaDossie."""

    template_name = "monitoring/softdesk_sync.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        rows, err = load_sync_table_chamados()
        ctx["chamados"] = rows
        ctx["load_error"] = err
        ctx["softdesk_auto_interval_sec"] = max(
            15, int(getattr(settings, "SOFTDESK_SYNC_UI_AUTO_INTERVAL_SECONDS", "60"))
        )
        ctx["softdesk_auto_first_delay_sec"] = max(
            0, int(getattr(settings, "SOFTDESK_SYNC_UI_AUTO_FIRST_DELAY_SECONDS", "3"))
        )
        return ctx


def _chamados_json_payload() -> dict[str, object]:
    rows, err = load_sync_table_chamados()
    return {"ok": True, "chamados": rows, "load_error": err}


class SoftdeskChamadosJsonView(View):
    """GET — apenas recarrega a tabela (PostgreSQL), sem chamar o dossiê Soft4."""

    http_method_names = ["get"]

    def get(self, request: HttpRequest) -> HttpResponse:
        return JsonResponse(_chamados_json_payload(), json_dumps_params={"ensure_ascii": False})


class SoftdeskSyncCycleView(View):
    """
    POST — para cada linha da tabela: RetornaDossie + ``sync_chamado_from_dossie``;
    devolve a tabela atualizada e um resumo do ciclo (CSRF obrigatório).
    """

    http_method_names = ["post"]

    def post(self, request: HttpRequest) -> HttpResponse:
        batch_max = max(1, min(int(getattr(settings, "SOFTDESK_SYNC_UI_BATCH_MAX", "200")), 2000))
        delay_ms = max(0, int(getattr(settings, "SOFTDESK_SYNC_UI_INTER_DOSSIE_DELAY_MS", "0")))

        rows, load_err = load_sync_table_chamados()
        if load_err:
            return JsonResponse(
                {"ok": False, "chamados": [], "load_error": load_err, "sync_cycle": None},
                status=503,
                json_dumps_params={"ensure_ascii": False},
            )

        to_run = rows[:batch_max]
        results: list[dict[str, object]] = []
        ok_count = 0
        err_count = 0

        for row in to_run:
            codigo = str(row.get("codigo") or "").strip()
            if not codigo:
                continue
            try:
                data = fetch_dossie(codigo)
                sync_result = sync_chamado_from_dossie(codigo, data)
                ok = bool(sync_result.get("ok"))
                if ok:
                    ok_count += 1
                else:
                    err_count += 1
                results.append(
                    {
                        "codigo": codigo,
                        "ok": ok,
                        "detail": (sync_result.get("detail") or "")[:500],
                    }
                )
            except RuntimeError as exc:
                err_count += 1
                results.append({"codigo": codigo, "ok": False, "detail": str(exc)[:500]})
                logger.warning("softdesk.sync_cycle.dossie_failed", extra={"codigo": codigo, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001
                err_count += 1
                results.append({"codigo": codigo, "ok": False, "detail": str(exc)[:500]})
                logger.exception("softdesk.sync_cycle.row_failed", extra={"codigo": codigo})

            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

        rows_after, err_after = load_sync_table_chamados()
        return JsonResponse(
            {
                "ok": True,
                "chamados": rows_after,
                "load_error": err_after,
                "sync_cycle": {
                    "batch_max": batch_max,
                    "linhas_na_lista": len(rows),
                    "processadas": len(results),
                    "ok_count": ok_count,
                    "error_count": err_count,
                    "finished_at": timezone.now().isoformat(),
                    "results": results,
                },
            },
            json_dumps_params={"ensure_ascii": False},
        )


class SoftdeskDossieFetchView(View):
    """GET ?codigo= — proxy para exibir JSON do dossiê (evita CORS no browser)."""

    http_method_names = ["get"]

    def get(self, request: HttpRequest) -> HttpResponse:
        codigo = (request.GET.get("codigo") or "").strip()
        if not codigo:
            return JsonResponse({"detail": "Parâmetro codigo é obrigatório."}, status=400)
        try:
            data = fetch_dossie(codigo)
        except RuntimeError as exc:
            return JsonResponse({"detail": str(exc)}, status=502)
        if isinstance(data, dict):
            sync_result = sync_chamado_from_dossie(codigo, data)
            data = {**data, "_conversys_sync": sync_result}
        return JsonResponse(data, json_dumps_params={"ensure_ascii": False})
