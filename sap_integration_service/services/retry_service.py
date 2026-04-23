"""
Retry policy for async queue items (exponential backoff, capped attempts).
"""
from __future__ import annotations

from datetime import timedelta

from django.utils import timezone as dj_tz

from sap_queue.models import SapQueue

MAX_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 30


def backoff_seconds(retry_count: int) -> int:
    return int(BASE_BACKOFF_SECONDS * (2**retry_count))


def schedule_retry_or_fail(item: SapQueue, error_message: str) -> None:
    """
    Increment retry_count; if still under MAX_ATTEMPTS, schedule next_retry_at; else mark error.
    """
    item.retry_count = int(item.retry_count or 0) + 1
    if item.retry_count >= MAX_ATTEMPTS:
        item.status = SapQueue.Status.ERROR
        item.next_retry_at = None
        item.save(update_fields=["retry_count", "status", "next_retry_at"])
        return

    delta = timedelta(seconds=backoff_seconds(item.retry_count))
    item.status = SapQueue.Status.ERROR
    item.next_retry_at = dj_tz.now() + delta
    item.save(update_fields=["retry_count", "status", "next_retry_at"])
