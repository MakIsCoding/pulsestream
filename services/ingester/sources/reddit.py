"""
Reddit source. Uses Reddit's Atom RSS search feed instead of the JSON API.

The public JSON endpoint (reddit.com/search.json) returns 403 from cloud/server
IPs since Reddit's 2023 API policy change. The RSS feed is not subject to the
same IP-based blocking and works reliably from hosted environments.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import httpx

from shared.config import settings


logger = logging.getLogger("pulsestream.ingester.reddit")

REDDIT_RSS_URL = "https://www.reddit.com/search.rss"
ATOM_NS = "http://www.w3.org/2005/Atom"


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Reddit via the Atom RSS feed.
    Returns normalized dicts ready to publish as MentionRawEvent.
    """
    if not keywords:
        return []

    query = " ".join(keywords)
    params = {
        "q": query,
        "sort": "new",
        "t": "month",
        "limit": str(min(limit, 25)),  # RSS cap is 25
    }
    ua = settings.reddit_user_agent or "script:pulsestream:v1.0 (by /u/pulsestream_app)"
    headers = {
        "User-Agent": ua,
        "Accept": "application/rss+xml, application/xml, text/xml",
    }

    async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(REDDIT_RSS_URL, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning("Reddit RSS returned HTTP %s for query %r", e.response.status_code, query)
            return []
        except httpx.HTTPError as e:
            logger.warning("Reddit RSS request failed: %s", e)
            return []

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as e:
        logger.warning("Reddit RSS parse error: %s", e)
        return []

    entries = root.findall(f"{{{ATOM_NS}}}entry")
    logger.info("Reddit RSS for %r returned %d entries", query, len(entries))

    results = []
    for entry in entries:
        item = _normalize(entry)
        if item:
            results.append(item)
    return results


def _normalize(entry: ET.Element) -> dict[str, Any] | None:
    """Convert one Atom entry into our standard mention shape."""
    def tag(name: str) -> str:
        return f"{{{ATOM_NS}}}{name}"

    raw_id = (entry.findtext(tag("id")) or "").strip()
    # Reddit IDs: t3_xxx = link/self post, t5_xxx = subreddit — skip non-posts
    if not raw_id.startswith("t3_"):
        return None
    external_id = raw_id[3:]  # strip "t3_" prefix

    link_el = entry.find(tag("link"))
    url = link_el.get("href", "").strip() if link_el is not None else None

    title = (entry.findtext(tag("title")) or "").strip() or None

    # Prefer published over updated for post creation time
    published_raw = entry.findtext(tag("published")) or entry.findtext(tag("updated"))
    published_iso: str | None = None
    if published_raw:
        try:
            dt = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
            published_iso = dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass

    author_el = entry.find(tag("author"))
    author: str | None = None
    if author_el is not None:
        raw_author = (author_el.findtext(tag("name")) or "").strip()
        author = raw_author[3:] if raw_author.startswith("/u/") else (raw_author or None)

    return {
        "external_id": external_id,
        "url": url or f"https://www.reddit.com/comments/{external_id}/",
        "author": author,
        "title": title,
        "content": None,  # RSS doesn't include full selftext
        "content_published_at": published_iso,
        "raw_payload": {"id": external_id, "title": title, "url": url},
    }
