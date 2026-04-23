"""
Internal integration events (webhook simulation).

Handlers must be idempotent: the same logical event may be redelivered after crashes.
"""
from __future__ import annotations

from django.dispatch import Signal

# kwargs: integration, event_type, external_id, payload, content_hash, dedup_key
integration_event = Signal()
