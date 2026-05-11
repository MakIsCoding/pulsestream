"""
PulseStream Scheduler.

Two responsibilities:

1. Subscribe to `topics.created` events from Kafka.
   When a user creates a new topic via the web API, this service is
   notified so it can immediately add the topic to its rotation.

2. Every N seconds, publish ingestion jobs to Kafka for each active
   topic. Each job tells an ingester worker to fetch new mentions
   from a specific source (HackerNews, Reddit) for that topic.

This service holds no HTTP endpoints. It runs as a long-lived process
with two concurrent loops:
- An async Kafka consumer for topic events
- An APScheduler job firing every INGESTION_INTERVAL_SECONDS
"""

import asyncio
import logging
import signal
from datetime import datetime, timedelta, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from groq import AsyncGroq, APIStatusError
from sqlalchemy import delete, select
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.config import settings
from shared.db import AsyncSessionLocal, engine
from shared.kafka_client import (
    close_producer,
    deserialize_event,
    get_producer,
    make_consumer,
    publish_event,
)
from shared.models import Mention, Topic, TopicDigest
from shared.schemas import IngestionJobEvent


_groq_client: AsyncGroq | None = None


def _get_groq_client() -> AsyncGroq:
    global _groq_client
    if _groq_client is None:
        if not settings.groq_api_key:
            raise RuntimeError("GROQ_API_KEY is not set")
        _groq_client = AsyncGroq(api_key=settings.groq_api_key)
    return _groq_client


@retry(
    retry=retry_if_not_exception_type(APIStatusError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
async def _call_groq_for_digest(topic_name: str, lines: list[str]) -> str:
    client = _get_groq_client()
    prompt = (
        f'Summarize the following recent discussions about "{topic_name}" in 2-3 sentences.\n'
        f"Focus on key themes, patterns, and notable viewpoints. Be concise and factual.\n\n"
        + "\n".join(lines)
    )
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=200,
        temperature=0.3,
    )
    return (response.choices[0].message.content or "").strip()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.scheduler")


# Sources we'll fan out ingestion jobs to. The ingester service
# subscribes to ingestion.jobs and routes by `source`.
INGESTION_SOURCES = ["hackernews", "reddit"]


class Scheduler:
    """Holds shared state and coordinates the consumer + periodic loop."""

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._aps = AsyncIOScheduler()

    async def run(self) -> None:
        """Main entrypoint. Sets up signal handling, runs everything until stop."""
        # Install graceful shutdown handlers so docker stop / Ctrl+C
        # actually stop us cleanly instead of getting force-killed.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stop_event.set)

        # Warm up the Kafka producer so the first periodic tick is fast.
        await get_producer()

        # Periodic job: fan out ingestion requests every N seconds.
        self._aps.add_job(
            self._fan_out_ingestion_jobs,
            trigger="interval",
            seconds=settings.ingestion_interval_seconds,
            id="fan_out_ingestion",
            max_instances=1,
            coalesce=True,
        )
        # Retention cleanup: delete mentions older than mention_retention_days.
        self._aps.add_job(
            self._cleanup_old_mentions,
            trigger="interval",
            hours=24,
            id="retention_cleanup",
            max_instances=1,
            coalesce=True,
        )
        # Digest generation: summarise recent mentions every 6 hours.
        self._aps.add_job(
            self._generate_digests,
            trigger="interval",
            hours=6,
            id="generate_digests",
            max_instances=1,
            coalesce=True,
        )
        self._aps.start()
        logger.info(
            "Started APScheduler. Fanning out ingestion jobs every %ds.",
            settings.ingestion_interval_seconds,
        )

        # Run an initial fan-out immediately, so we don't wait the full
        # interval after startup before doing anything.
        await self._fan_out_ingestion_jobs()

        # Run the topic-events consumer concurrently with the periodic job.
        consumer_task = asyncio.create_task(self._consume_topic_events())

        # Wait for shutdown signal, then clean up.
        await self._stop_event.wait()
        logger.info("Shutdown signal received. Stopping...")

        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        self._aps.shutdown(wait=False)
        await close_producer()
        await engine.dispose()
        logger.info("Scheduler stopped cleanly.")

    async def _consume_topic_events(self) -> None:
        """
        Listens for `topics.created` events. Right now we just log them —
        the periodic loop reads the DB directly, so we don't need to
        maintain in-memory state. Logging is useful for debugging though,
        and gives us a place to add reactive logic later (e.g., trigger
        immediate ingestion when a topic is created).
        """
        topic_name = settings.kafka_topic_topics_created
        logger.info("Subscribing to Kafka topic '%s'...", topic_name)

        async with make_consumer(topic_name, group_id="scheduler") as consumer:
            async for msg in consumer:
                if self._stop_event.is_set():
                    break
                event = deserialize_event(msg.value)
                logger.info(
                    "topic.created received: topic_id=%s user_id=%s name=%r",
                    event.get("topic_id"),
                    event.get("user_id"),
                    event.get("name"),
                )
                # Trigger an immediate ingestion for this topic so the user
                # doesn't wait the full interval to see their first results.
                await self._enqueue_for_topic(event)

    async def _enqueue_for_topic(self, topic_event: dict[str, Any]) -> None:
        """Fan out ingestion jobs for a single topic across all sources."""
        for source in INGESTION_SOURCES:
            job = IngestionJobEvent(
                topic_id=topic_event["topic_id"],
                user_id=topic_event["user_id"],
                name=topic_event["name"],
                keywords=topic_event["keywords"],
                source=source,
            )
            await publish_event(
                settings.kafka_topic_ingestion_jobs,
                job.model_dump(mode="json"),
                key=str(job.topic_id),
            )

    async def _cleanup_old_mentions(self) -> None:
        """Delete mentions older than `mention_retention_days` to cap storage."""
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=settings.mention_retention_days)
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    delete(Mention).where(Mention.ingested_at < cutoff)
                )
                await session.commit()
            deleted = result.rowcount
            if deleted:
                logger.info(
                    "Retention cleanup: removed %d mentions older than %d days",
                    deleted,
                    settings.mention_retention_days,
                )
        except Exception:
            logger.exception("Error during retention cleanup")

    async def _generate_digests(self) -> None:
        """Generate or refresh digests for all active topics that have enough data."""
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Topic).where(Topic.is_active.is_(True))
                )
                topics = list(result.scalars().all())

            for topic in topics:
                try:
                    await self._generate_digest_for_topic(topic)
                except Exception:
                    logger.exception("Digest failed for topic %s", topic.id)
        except Exception:
            logger.exception("Error during digest generation sweep")

    async def _generate_digest_for_topic(self, topic: Topic) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Mention)
                .where(
                    Mention.topic_id == topic.id,
                    Mention.analyzed_at.is_not(None),
                    Mention.ingested_at >= cutoff,
                )
                .order_by(Mention.ingested_at.desc())
                .limit(50)
            )
            mentions = list(result.scalars().all())

        if len(mentions) < 3:
            return  # not enough data to summarise

        # Build sentiment distribution and top entities directly from the data.
        dist: dict[str, int] = {"positive": 0, "negative": 0, "neutral": 0}
        entity_freq: dict[str, int] = {}
        for m in mentions:
            if m.sentiment_label in dist:
                dist[m.sentiment_label] += 1
            for e in m.entities or []:
                entity_freq[e] = entity_freq.get(e, 0) + 1

        top_entities = sorted(entity_freq, key=entity_freq.__getitem__, reverse=True)[:10]

        # Generate textual summary via Groq (best-effort — skip if no key).
        summary: str | None = None
        if settings.groq_api_key:
            lines = [
                f"- {(m.summary or m.title or '').strip()[:200]}"
                for m in mentions[:20]
                if (m.summary or m.title or "").strip()
            ]
            if lines:
                try:
                    summary = await _call_groq_for_digest(topic.name, lines)
                except Exception:
                    logger.warning("Groq digest call failed for topic %s; skipping summary", topic.id)

        async with AsyncSessionLocal() as session:
            digest = TopicDigest(
                topic_id=topic.id,
                summary=summary,
                sentiment_distribution=dist,
                top_entities=top_entities,
                mention_count=len(mentions),
            )
            session.add(digest)
            await session.commit()

        logger.info("Digest generated for topic %s (%d mentions)", topic.id, len(mentions))

    async def _fan_out_ingestion_jobs(self) -> None:
        """
        Runs every INGESTION_INTERVAL_SECONDS. Reads all active topics
        from Postgres and publishes one ingestion job per (topic, source).
        """
        try:
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    select(Topic).where(Topic.is_active.is_(True))
                )
                topics = list(result.scalars().all())

            if not topics:
                logger.info("No active topics. Skipping fan-out.")
                return

            count = 0
            for topic in topics:
                event = IngestionJobEvent(
                    topic_id=topic.id,
                    user_id=topic.user_id,
                    name=topic.name,
                    keywords=topic.keywords,
                    source="",  # set per source below
                )
                for source in INGESTION_SOURCES:
                    event_dict = event.model_dump(mode="json")
                    event_dict["source"] = source
                    await publish_event(
                        settings.kafka_topic_ingestion_jobs,
                        event_dict,
                        key=str(topic.id),
                    )
                    count += 1

            logger.info(
                "Fan-out complete: %d ingestion jobs published for %d topic(s).",
                count,
                len(topics),
            )
        except Exception:
            # Don't let a single failed tick kill the scheduler.
            # APScheduler will call us again on the next interval.
            logger.exception("Error during fan-out tick")


async def main() -> None:
    scheduler = Scheduler()
    await scheduler.run()


if __name__ == "__main__":
    asyncio.run(main())