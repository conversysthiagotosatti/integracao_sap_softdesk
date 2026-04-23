from django.apps import AppConfig


class SoftdeskSyncConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "softdesk_sync"
    verbose_name = "Softdesk polling / fake webhook"

    def ready(self) -> None:
        # Register signal consumers (Helpdesk / Assets / AI hooks).
        from softdesk_sync import consumers  # noqa: F401
