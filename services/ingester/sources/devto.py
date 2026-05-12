"""
Dev.to source. Uses the public Forem search API — no auth required.
Best for programming / tech topics.

Search endpoint: https://dev.to/search/feed_content?class_name=Article&q={query}
"""

from __future__ import annotations

import logging
from datetime import timezone
from typing import Any

import httpx


logger = logging.getLogger("pulsestream.ingester.devto")

_DEVTO_SEARCH_URL = "https://dev.to/search/feed_content"


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Dev.to for articles matching the keywords.
    Returns normalized dicts ready to publish as MentionRawEvent.
    """
    if not keywords:
        return []

    params = {
        "class_name": "Article",
        "q": " ".join(keywords),
        "per_page": str(limit),
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(_DEVTO_SEARCH_URL, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Dev.to search failed: %s", exc)
            return []

    data = response.json()
    articles = data.get("result", [])
    if not isinstance(articles, list):
        return []

    return [n for article in articles if (n := _normalize(article)) is not None]


def _normalize(article: dict[str, Any]) -> dict[str, Any] | None:
    article_id = article.get("id")
    if not article_id:
        return None

    path = article.get("path") or ""
    url = f"https://dev.to{path}" if path.startswith("/") else None

    user = article.get("user") or {}
    author = user.get("name") or user.get("username")

    # published_at is an ISO string with timezone info
    published_iso: str | None = None
    published_at = article.get("published_at") or article.get("readable_publish_date")
    if published_at:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            published_iso = dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

    return {
        "external_id": str(article_id),
        "url": url,
        "author": author,
        "title": article.get("title"),
        "content": article.get("description"),
        "content_published_at": published_iso,
        "raw_payload": article,
    }
