from django.urls import path

from monitoring import process_views, views

urlpatterns = [
    path("", views.IntegrationDashboardView.as_view(), name="integration-dashboard"),
    path("softdesk/", views.SoftdeskSyncDashboardView.as_view(), name="softdesk-sync-dashboard"),
    path("softdesk/dossie/", views.SoftdeskDossieFetchView.as_view(), name="softdesk-dossie-fetch"),
    path("softdesk/api/chamados/", views.SoftdeskChamadosJsonView.as_view(), name="softdesk-api-chamados"),
    path("softdesk/api/sync-cycle/", views.SoftdeskSyncCycleView.as_view(), name="softdesk-api-sync-cycle"),
    path(
        "process-queue/<int:item_id>/",
        process_views.ProcessQueueItemView.as_view(),
        name="integration-process-queue-item",
    ),
]
