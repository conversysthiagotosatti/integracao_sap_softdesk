from django.apps import AppConfig


class SapQueueConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "sap_queue"
    verbose_name = "SAP async queue"
