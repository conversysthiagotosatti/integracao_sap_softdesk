"""
Normalize Softdesk list endpoints into (items, next_page_hint).

``next_page_hint`` is either an int page number or None when exhausted.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

from django.conf import settings


def _get_path(d: dict[str, Any], dotted: str) -> Any:
    cur: Any = d
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def extract_items_and_next_page(data: Any) -> tuple[list[dict[str, Any]], int | None]:
    """
    Returns (items, next_page).

    Supports:
    - bare JSON list
    - envelope dict with configurable keys (defaults: results/items + next/meta)
    """
    items_key = getattr(settings, "SOFTDESK_LIST_ITEMS_KEY", "results")
    alt_items_key = getattr(settings, "SOFTDESK_LIST_ITEMS_ALT_KEY", "items")

    if isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
        return items, None

    if not isinstance(data, dict):
        return [], None

    raw_items = data.get(items_key)
    if raw_items is None:
        raw_items = data.get(alt_items_key)
    if raw_items is None:
        raw_items = data.get("data")
    if not isinstance(raw_items, list):
        return [], None

    items = [x for x in raw_items if isinstance(x, dict)]

    next_page: int | None = None
    next_url = data.get(getattr(settings, "SOFTDESK_NEXT_URL_KEY", "next"))
    if isinstance(next_url, str) and next_url.strip():
        parsed = urlparse(next_url)
        page_param = getattr(settings, "SOFTDESK_PAGE_QUERY_PARAM", "page")
        qs = parse_qs(parsed.query)
        if page_param in qs and qs[page_param]:
            try:
                next_page = int(qs[page_param][0])
            except ValueError:
                next_page = None

    if next_page is None:
        meta = data.get(getattr(settings, "SOFTDESK_META_KEY", "meta"))
        if isinstance(meta, dict):
            np = meta.get("next_page") or meta.get("nextPage")
            if isinstance(np, int):
                next_page = np

    if next_page is None:
        np = _get_path(data, getattr(settings, "SOFTDESK_NEXT_PAGE_JSON_PATH", ""))
        if isinstance(np, int):
            next_page = np

    return items, next_page
