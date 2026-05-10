"""
PulseStream Ingester.

Subscribes to `ingestion.jobs` events (published by the scheduler),
calls the appropriate source API (HackerNews, Reddit), and publishes
each result as a `mentions.raw` event for the analyzer to enrich.

Deduplication: every (source, external_id) pair is tracked in Redis
with a 7-day TTL. If we've seen an item before, we don't re-publish.
This prevents the analyzer from doing redundant LLM calls and keeps
free-tier API budgets in check.

Multiple ingester replicas can run safely — they share the Kafka
consumer group `ingester` so each job is processed exactly once.
"""

import asyncio
import logging
import signal
from typing import Any, Awaitable, Callable

from shared.config import settings
from shared.db import engine
from shared.kafka_client import (
    close_producer,
    deserialize_event,
    get_producer,
    make_consumer,
    publish_event,
)
from shared.redis_client import close_redis, get_redis

from services.ingester.sources import hackernews, reddit


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.ingester")


# Type for source modules' fetch function: keywords -> list of dicts
FetchFunc = Callable[[list[str], int], Awaitable[list[dict[str, Any]]]]

# Registry: source name → fetch coroutine
SOURCE_REGISTRY: dict[str, FetchFunc] = {
    "hackernews": hackernews.fetch,
    "reddit": reddit.fetch,
}

# Dedup TTL: 7 days. Long enough to cover repeated keyword hits;
# short enough that storage stays bounded.
DEDUP_TTL_SECONDS = 7 * 24 * 60 * 60

# Per-job result limit (each job pulls at most this many items).
PER_JOB_LIMIT = 20


_stop_event = asyncio.Event()


async def main() -> None:
    """Set up graceful shutdown, then run the consume loop."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _stop_event.set)

    # Warm up shared clients so the first job is fast.
    await get_producer()
    await get_redis()

    try:
        await _consume_jobs()
    finally:
        await close_producer()
        await close_redis()
        await engine.dispose()
        logger.info("Ingester shut down cleanly.")


async def _consume_jobs() -> None:
    """Subscribe to ingestion.jobs and dispatch each one to the right source."""
    topic = settings.kafka_topic_ingestion_jobs
    logger.info("Subscribing to Kafka topic %r as group 'ingester'", topic)

    async with make_consumer(topic, group_id="ingester") as consumer:
        async for msg in consumer:
            if _stop_event.is_set():
                break

            try:
                job = deserialize_event(msg.value)
                await _handle_job(job)
            except Exception:
                # One bad job must not kill the consumer.
                # In a future iteration we'd push the failed job to a DLQ.
                logger.exception("Failed to process ingestion job; moving on")


async def _handle_job(job: dict[str, Any]) -> None:
    """Process one ingestion.jobs event."""
    source = job.get("source")
    keywords = job.get("keywords") or []
    topic_id = job.get("topic_id")
    name = job.get("name")

    fetch_func = SOURCE_REGISTRY.get(source)
    if fetch_func is None:
        logger.warning("Unknown source %r; skipping job", source)
        return

    if not keywords:
        logger.info("Job for topic %s has no keywords; skipping", name)
        return

    logger.info(
        "Fetching from %s for topic=%r keywords=%s",
        source, name, keywords,
    )

    items = await fetch_func(keywords, PER_JOB_LIMIT)
    if not items:
        logger.info("  No results from %s for %r", source, name)
        return

    new_count = 0
    dup_count = 0
    for item in items:
        if await _is_duplicate(source, item["external_id"]):
            dup_count += 1
            continue

        mention_event = {
            "source": source,
            "external_id": item["external_id"],
            "topic_id": topic_id,
            "url": item.get("url"),
            "author": item.get("author"),
            "title": item.get("title"),
            "content": item.get("content"),
            "content_published_at": item.get("content_published_at"),
            "raw_payload": item.get("raw_payload"),
        }
        await publish_event(
            settings.kafka_topic_mentions_raw,
            mention_event,
            key=f"{source}:{item['external_id']}",
        )
        await _mark_seen(source, item["external_id"])
        new_count += 1

    logger.info(
        "  Published %d new mentions (%d duplicates skipped) from %s for %r",
        new_count, dup_count, source, name,
    )


async def _is_duplicate(source: str, external_id: str) -> bool:
    """Has this (source, external_id) been seen recently?"""
    redis = await get_redis()
    key = f"ingester:seen:{source}:{external_id}"
    return await redis.exists(key) == 1


async def _mark_seen(source: str, external_id: str) -> None:
    """Record this (source, external_id) so we skip it next time."""
    redis = await get_redis()
    key = f"ingester:seen:{source}:{external_id}"
    await redis.set(key, "1", ex=DEDUP_TTL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())