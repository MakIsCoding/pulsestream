"""
HackerNews source. Uses Algolia's HN search API (no auth, no rate limit
practically for our usage).

API docs: https://hn.algolia.com/api
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx


logger = logging.getLogger("pulsestream.ingester.hackernews")


HN_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
HN_ITEM_URL_TEMPLATE = "https://news.ycombinator.com/item?id={story_id}"


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search HackerNews for stories matching any of the given keywords.

    Returns a list of normalized dicts ready to publish as MentionRawEvent.
    Each dict has: external_id, url, author, title, content, content_published_at,
    raw_payload.
    """
    if not keywords:
        return []

    # Algolia search query: keywords joined by space = OR-ish full-text
    query = " ".join(keywords)
    params = {
        "query": query,
        "tags": "story",  # only stories, no comments/jobs
        "hitsPerPage": str(limit),
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.get(HN_SEARCH_URL, params=params)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("HN search failed: %s", e)
            return []

    data = response.json()
    hits = data.get("hits", [])
    return [_normalize(hit) for hit in hits if hit.get("objectID")]


def _normalize(hit: dict[str, Any]) -> dict[str, Any]:
    """Convert an Algolia hit into our standard mention shape."""
    story_id = hit.get("objectID")

    # Prefer the story's own URL; fall back to the HN comments page.
    url = hit.get("url") or HN_ITEM_URL_TEMPLATE.format(story_id=story_id)

    # Algolia returns 'created_at_i' as Unix epoch seconds.
    published_iso: str | None = None
    epoch = hit.get("created_at_i")
    if epoch is not None:
        published_iso = datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat()

    return {
        "external_id": str(story_id),
        "url": url,
        "author": hit.get("author"),
        "title": hit.get("title"),
        "content": hit.get("story_text") or None,
        "content_published_at": published_iso,
        "raw_payload": hit,
    }