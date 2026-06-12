"""
Dev.to source. Uses the official public Forem API — no auth required.

The internal /search/feed_content endpoint returns empty results from
hosted environments. The public /api/articles/search endpoint is stable
and works reliably without authentication.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx


logger = logging.getLogger("pulsestream.ingester.devto")

_DEVTO_API_URL = "https://dev.to/api/articles/search"


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Dev.to for articles matching the keywords.
    Returns normalized dicts ready to publish as MentionRawEvent.
    """
    if not keywords:
        return []

    params = {
        "q": " ".join(keywords),
        "per_page": str(min(limit, 30)),
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(_DEVTO_API_URL, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning("Dev.to API returned HTTP %s for query %r", e.response.status_code, params["q"])
            return []
        except httpx.HTTPError as exc:
            logger.warning("Dev.to request failed: %s", exc)
            return []

    try:
        articles = response.json()
    except Exception:
        logger.warning("Dev.to returned non-JSON (status=%d)", response.status_code)
        return []

    if not isinstance(articles, list):
        logger.warning("Dev.to unexpected response shape: %s", type(articles).__name__)
        return []

    results = [n for article in articles if (n := _normalize(article)) is not None]
    logger.info("Dev.to returned %d articles for query %r", len(results), params["q"])
    return results


def _normalize(article: dict[str, Any]) -> dict[str, Any] | None:
    article_id = article.get("id")
    if not article_id:
        return None

    url = article.get("url") or None
    if not url:
        path = article.get("path") or ""
        url = f"https://dev.to{path}" if path.startswith("/") else None

    user = article.get("user") or {}
    author = user.get("name") or user.get("username") or None

    published_iso: str | None = None
    published_at = article.get("published_at") or article.get("published_timestamp")
    if published_at:
        try:
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
