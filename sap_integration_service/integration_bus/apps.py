from django.apps import AppConfig


class IntegrationBusConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "integration_bus"
    verbose_name = "Integration event bus"
