"""
Reddit source. Uses the public JSON search endpoint — no auth required,
but Reddit demands a polite User-Agent header (ours is set in .env).

If we ever want higher rate limits, switch to an authenticated PRAW
client; the contract this module exposes won't change.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from shared.config import settings


logger = logging.getLogger("pulsestream.ingester.reddit")


REDDIT_SEARCH_URL = "https://www.reddit.com/search.json"


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Reddit for posts matching the keywords.
    Returns normalized dicts ready to publish as MentionRawEvent.
    """
    if not keywords:
        return []

    query = " ".join(keywords)
    params = {
        "q": query,
        "limit": str(limit),
        "sort": "new",
        "type": "link",
    }
    headers = {
        "User-Agent": settings.reddit_user_agent or "pulsestream/0.1",
    }

    async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
        try:
            response = await client.get(REDDIT_SEARCH_URL, params=params)
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("Reddit search failed: %s", e)
            return []

    payload = response.json()
    children = payload.get("data", {}).get("children", [])
    return [_normalize(child["data"]) for child in children if child.get("data")]


def _normalize(post: dict[str, Any]) -> dict[str, Any]:
    """Convert a Reddit post into our standard mention shape."""
    post_id = post.get("id")
    permalink = post.get("permalink", "")
    full_url = f"https://www.reddit.com{permalink}" if permalink else post.get("url")

    published_iso: str | None = None
    epoch = post.get("created_utc")
    if epoch is not None:
        published_iso = datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat()

    return {
        "external_id": str(post_id),
        "url": full_url,
        "author": post.get("author"),
        "title": post.get("title"),
        "content": post.get("selftext") or None,
        "content_published_at": published_iso,
        "raw_payload": post,
    }