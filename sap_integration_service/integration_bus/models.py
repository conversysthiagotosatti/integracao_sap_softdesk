from __future__ import annotations

from django.db import models


class IntegrationEventReceipt(models.Model):
    """
    Idempotency ledger: one row per successfully published internal event.

    Prevents duplicate downstream side-effects when polling overlaps or retries occur.
    """

    integration = models.CharField(max_length=64, db_index=True)
    dedup_key = models.CharField(max_length=64, unique=True)
    event_type = models.CharField(max_length=128, db_index=True)
    external_id = models.CharField(max_length=128, db_index=True)
    content_hash = models.CharField(max_length=64)
    payload_snapshot = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["integration", "event_type", "created_at"]),
        ]

    def __str__(self) -> str:
        return f"{self.event_type}:{self.external_id}"
