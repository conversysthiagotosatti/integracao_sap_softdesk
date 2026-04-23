"""
Global rate limit for outbound Softdesk HTTP calls (default 60 req/min).

Uses Django cache for multi-process correctness when CACHE points to Redis.
"""
from __future__ import annotations

import time

from django.core.cache import cache


class MinuteWindowRateLimiter:
    """At most ``max_requests`` per rolling minute window (wall-clock minute buckets)."""

    def __init__(self, key_prefix: str, max_requests: int = 60) -> None:
        self.key_prefix = key_prefix
        self.max_requests = max_requests

    def _window_key(self) -> str:
        window = int(time.time()) // 60
        return f"{self.key_prefix}:{window}"

    def acquire(self) -> None:
        """
        Block (sleep) until a slot is available, then consume one token.

        Keeps polling tasks from hammering Softdesk when many workers run in parallel.
        """
        while True:
            key = self._window_key()
            try:
                n = cache.incr(key, delta=1)
            except ValueError:
                cache.add(key, 1, timeout=120)
                n = 1
            if n <= self.max_requests:
                return
            # Roll back this increment and wait for the next minute bucket.
            try:
                cache.decr(key, delta=1)
            except ValueError:
                pass
            sleep_s = max(0.05, 60.0 - (time.time() % 60.0) + 0.05)
            time.sleep(sleep_s)
