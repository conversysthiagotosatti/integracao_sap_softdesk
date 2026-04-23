from django.urls import path

from . import views

urlpatterns = [
    path("health/", views.health),
    path("health", views.health),
    path("api/health/", views.SapHealthAPIView.as_view()),
    path("api/health", views.SapHealthAPIView.as_view()),
    path("api/sap/send/", views.SapSendView.as_view()),
    path("api/sap/send", views.SapSendView.as_view()),
    path("api/sap/logs/", views.SapLogListView.as_view()),
    path("api/sap/logs", views.SapLogListView.as_view()),
    path("api/sap/logs/<int:log_id>/", views.SapLogDetailView.as_view()),
    path("api/sap/logs/<int:log_id>", views.SapLogDetailView.as_view()),
    path("api/sap/queue/", views.SapQueueListView.as_view()),
    path("api/sap/queue", views.SapQueueListView.as_view()),
]
