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
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from shared.config import settings
from shared.db import AsyncSessionLocal, engine
from shared.kafka_client import (
    close_producer,
    deserialize_event,
    get_producer,
    make_consumer,
    publish_event,
)
from shared.models import Topic
from shared.schemas import IngestionJobEvent


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
            max_instances=1,  # never overlap two ticks
            coalesce=True,    # if backed up, run once instead of catching up
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