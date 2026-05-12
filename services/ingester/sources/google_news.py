"""
Google News RSS source. Covers any topic — tech, medical, education, etc.
No authentication required.

RSS endpoint: https://news.google.com/rss/search?q={query}
"""

from __future__ import annotations

import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx


logger = logging.getLogger("pulsestream.ingester.google_news")

_GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
_TAG_RE = re.compile(r"<[^>]+>")


async def fetch(keywords: list[str], limit: int = 20) -> list[dict[str, Any]]:
    """
    Search Google News RSS for articles matching the keywords.
    Returns normalized dicts ready to publish as MentionRawEvent.
    """
    if not keywords:
        return []

    params = {
        "q": " ".join(keywords),
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    async with httpx.AsyncClient(timeout=15.0, headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(_GOOGLE_NEWS_RSS, params=params)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Google News RSS fetch failed: %s", exc)
            return []

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError as exc:
        logger.warning("Google News RSS parse error: %s", exc)
        return []

    items = root.findall(".//item")[:limit]
    results = [n for item in items if (n := _normalize(item)) is not None]
    logger.info("Google News returned %d items for query %r", len(results), " ".join(keywords))
    return results


def _normalize(item: ET.Element) -> dict[str, Any] | None:
    title = (item.findtext("title") or "").strip()
    link = (item.findtext("link") or "").strip()
    guid = (item.findtext("guid") or link).strip()
    pub_date = (item.findtext("pubDate") or "").strip()
    description = (item.findtext("description") or "").strip()

    if not guid:
        return None

    # Guids can be long Google-internal URLs; hash them for a compact key.
    external_id = hashlib.md5(guid.encode()).hexdigest()

    published_iso: str | None = None
    if pub_date:
        try:
            dt = parsedate_to_datetime(pub_date)
            published_iso = dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass

    clean_description = _TAG_RE.sub("", description).strip() or None

    return {
        "external_id": external_id,
        "url": link or None,
        "author": None,
        "title": title or None,
        "content": clean_description,
        "content_published_at": published_iso,
        "raw_payload": {"guid": guid, "title": title, "link": link, "pub_date": pub_date},
    }
