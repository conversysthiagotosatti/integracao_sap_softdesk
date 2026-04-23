from django.contrib import admin
from django.urls import include, path

admin.site.site_header = "Integração SAP"
admin.site.site_title = "Integração SAP"
admin.site.index_title = "Painel de administração"

urlpatterns = [
    path("admin/", admin.site.urls),
    path("integrations/", include("monitoring.urls")),
    path("", include("api.urls")),
]
