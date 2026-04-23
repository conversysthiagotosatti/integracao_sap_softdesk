"""
Celery tasks: periodic Softdesk polling (fake webhook).

Retries use exponential backoff for transient HTTP failures and rate limits.
"""
from __future__ import annotations

import logging

from core.celery import app

from softdesk_sync.exceptions import SoftdeskAPIError, SoftdeskRateLimitError
from softdesk_sync.poller import run_poll

logger = logging.getLogger(__name__)


@app.task(
    bind=True,
    name="softdesk.poll_chamados",
    autoretry_for=(SoftdeskAPIError, SoftdeskRateLimitError, OSError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    acks_late=True,
    max_retries=12,
)
def softdesk_poll_chamados(self, scope: str = "chamados") -> dict[str, int]:
    try:
        return run_poll(scope=scope)
    except Exception:
        logger.exception(
            "softdesk.task.retrying",
            extra={"task_id": getattr(self.request, "id", None), "scope": scope},
        )
        raise
