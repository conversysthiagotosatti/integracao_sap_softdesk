from __future__ import annotations

from django.db import models


class SoftdeskSyncState(models.Model):
    """
    Global poller cursor + health for a Softdesk scope (e.g. chamados).

    Separate rows allow future scopes (assets, comments) without schema churn.
    """

    scope = models.CharField(max_length=64, unique=True, default="chamados")
    # Incremental sync: last remote "updated at" (ISO string from API) or opaque watermark
    watermark = models.CharField(max_length=128, blank=True, default="")
    # Optional opaque pagination cursor for next poll (API-specific)
    page_cursor = models.CharField(max_length=512, blank=True, default="")
    last_poll_started_at = models.DateTimeField(null=True, blank=True)
    last_poll_finished_at = models.DateTimeField(null=True, blank=True)
    last_error_code = models.CharField(max_length=64, blank=True, default="")
    last_error_message = models.TextField(blank=True, default="")
    consecutive_failures = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("scope",)

    def __str__(self) -> str:
        return f"SoftdeskSyncState({self.scope})"


class SoftdeskChamadoState(models.Model):
    """
    Per-ticket fingerprint for change detection (polling diff engine).

    Not a full mirror of Softdesk — only what is needed to detect creates/updates cheaply.
    """

    external_id = models.CharField(max_length=128, unique=True, db_index=True)
    content_hash = models.CharField(max_length=64)
    remote_updated_at = models.CharField(max_length=64, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Softdesk chamado fingerprint"
        verbose_name_plural = "Softdesk chamado fingerprints"

    def __str__(self) -> str:
        return f"Chamado {self.external_id}"
