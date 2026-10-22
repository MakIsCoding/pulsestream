"""
PulseStream Analyzer.

Consumes `mentions.raw` events. For each one:
1. Calls Gemini to extract sentiment, entities, and a brief summary.
2. Inserts a Mention row in Postgres with all enriched fields.
3. Publishes a `mentions.analyzed` event so the WebSocket can push live
   updates to dashboards.

Cost discipline:
- Mentions are analyzed in BATCHES of N to amortize per-call overhead
  and stay well under Gemini's free-tier 1000 requests/day cap.
- Each Gemini call is retried with exponential backoff (tenacity).
- If the LLM returns malformed JSON, we still persist the mention with
  null sentiment/entities so we don't lose the raw data.

Idempotency:
- Postgres has a unique constraint on (source, external_id). If the
  analyzer is replayed (Kafka offset reset), we ON CONFLICT DO NOTHING
  so duplicates fail silently rather than crashing.

Delivery (at-least-once):
- Auto-commit is OFF. Offsets are committed only after every mention in a
  batch has been written to Postgres. If the process crashes, is redeployed,
  or Postgres is briefly down mid-batch, nothing is committed and the batch
  is re-delivered; the ON CONFLICT insert makes the repeat harmless.
- Messages that can never be stored (malformed JSON, missing fields, or rows
  the database rejects outright) go to the dead-letter topic instead of
  crashing the loop or being retried forever.
- Transient failures (database or Kafka briefly unreachable) are retried in
  place first, so a short blip doesn't cost another LLM call.
- LLM output is validated and coerced before it's stored (`_clean_analysis`),
  so a malformed reply is saved as "not analysed" rather than failing.
"""

import asyncio
import json
import logging
import signal
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from groq import AsyncGroq, APIStatusError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select
from aiokafka.errors import KafkaError
from sqlalchemy.exc import (
    DBAPIError,
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    TimeoutError as PoolTimeoutError,
)
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from shared.config import settings
from shared.db import AsyncSessionLocal, Base, engine
from shared.kafka_client import (
    close_producer,
    commit_offsets,
    deserialize_event,
    get_producer,
    make_consumer,
    publish_event,
    send_to_dead_letter,
)
from shared.models import Mention, Topic


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.analyzer")


# Batch a few mentions together per Gemini call to amortize overhead.
BATCH_SIZE = 5
BATCH_FLUSH_SECONDS = 5.0


_stop_event = asyncio.Event()
_groq_client: AsyncGroq | None = None


def _get_groq_client() -> AsyncGroq:
    """Lazy-initialize the Groq client."""
    global _groq_client
    if _groq_client is None:
        if not settings.groq_api_key:
            raise RuntimeError("GROQ_API_KEY is not set in env")
        _groq_client = AsyncGroq(api_key=settings.groq_api_key)
    return _groq_client


# ----------------------------------------------------------------------
# Gemini call
# ----------------------------------------------------------------------

ANALYSIS_PROMPT = """\
You will be given a list of social/news posts. For EACH post, produce a JSON
analysis with these fields:

- sentiment_score: a float in [-1.0, 1.0] where -1 is very negative, 0
  is neutral, +1 is very positive.
- sentiment_label: one of "positive", "negative", "neutral".
- entities: a list of named entities mentioned (people, products, companies,
  technologies). Up to 10 entries, no duplicates. Lowercase.
- summary: a single sentence (<=200 chars) capturing what the post is about.

Respond with a single JSON object of the form:
{"results": [<analysis for post 1>, <analysis for post 2>, ...]}

Keep the order matching the input. If you cannot analyze a post, use null
for that slot. No prose, no markdown fences — just the JSON object.

Posts to analyze:
"""


@retry(
    # Retry on transient errors only (network, timeouts).
    # Never retry on Groq HTTP errors (4xx rate limit, auth, bad request;
    # 5xx will also fail fast — better to drop the batch than burn quota).
    retry=retry_if_not_exception_type(APIStatusError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
async def _analyze_batch(
    posts: list[dict[str, Any]],
) -> list[dict[str, Any] | None]:
    """
    Send a batch to Groq, return one analysis dict per input post
    (or None for posts we couldn't analyze).
    """
    client = _get_groq_client()

    # Build a compact representation of each post for the prompt.
    formatted = []
    for i, post in enumerate(posts):
        title = (post.get("title") or "").strip()
        content = (post.get("content") or "").strip()
        body = title
        if content:
            body += f"\n{content[:500]}"
        formatted.append(f"--- Post {i + 1} ---\n{body}")

    prompt = ANALYSIS_PROMPT + "\n\n".join(formatted)

    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0.2,
    )

    raw = response.choices[0].message.content or "{}"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Groq returned non-JSON; raw=%r", raw[:200])
        return [None] * len(posts)

    if not isinstance(parsed, dict):
        logger.warning("Groq did not return a JSON object; got %s", type(parsed).__name__)
        return [None] * len(posts)

    results = parsed.get("results")
    if not isinstance(results, list):
        logger.warning("Groq response missing 'results' list; got %r", parsed)
        return [None] * len(posts)

    # Pad/trim to match input length.
    if len(results) < len(posts):
        results += [None] * (len(posts) - len(results))
    return results[: len(posts)]


# ----------------------------------------------------------------------
# Persistence
# ----------------------------------------------------------------------

SENTIMENT_LABELS = {"positive", "negative", "neutral"}
MAX_ENTITIES = 10
MAX_ENTITY_CHARS = 100
MAX_SUMMARY_CHARS = 1000


def _clean_analysis(analysis: Any) -> dict[str, Any] | None:
    """
    Never trust LLM output: keep only well-formed fields, coerce types, and
    drop anything else, so a malformed reply can't make the database insert
    fail. Returns None when nothing usable is left (stored as "not analysed").
    """
    if not isinstance(analysis, dict):
        return None

    score = analysis.get("sentiment_score")
    try:
        score = float(score)
        score = None if score != score else max(-1.0, min(1.0, score))  # NaN check, clamp
    except (TypeError, ValueError):
        score = None

    label = analysis.get("sentiment_label")
    label = label.strip().lower() if isinstance(label, str) else None
    if label not in SENTIMENT_LABELS:
        label = None

    raw_entities = analysis.get("entities")
    entities: list[str] | None = None
    if isinstance(raw_entities, list):
        seen: set[str] = set()
        entities = []
        for item in raw_entities:
            if isinstance(item, str):
                name = item.strip()[:MAX_ENTITY_CHARS]
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    entities.append(name)
            if len(entities) >= MAX_ENTITIES:
                break

    summary = analysis.get("summary")
    if isinstance(summary, str):
        summary = summary.strip()[:MAX_SUMMARY_CHARS] or None
    else:
        summary = None

    cleaned = {
        "sentiment_score": score,
        "sentiment_label": label,
        "entities": entities,
        "summary": summary,
    }
    if all(value is None for value in cleaned.values()):
        return None
    return cleaned


async def _persist_and_emit(
    raw_event: dict[str, Any],
    analysis: dict[str, Any] | None,
) -> None:
    """Insert/upsert the mention into Postgres and publish mentions.analyzed."""
    analysis = _clean_analysis(analysis)
    sentiment_score = None
    sentiment_label = None
    entities = None
    summary = None
    if analysis:
        sentiment_score = analysis.get("sentiment_score")
        sentiment_label = analysis.get("sentiment_label")
        entities = analysis.get("entities")
        summary = analysis.get("summary")

    topic_id = UUID(raw_event["topic_id"])
    source = raw_event["source"]
    external_id = raw_event["external_id"]

    published_at = None
    raw_dt = raw_event.get("content_published_at")
    if raw_dt:
        try:
            published_at = datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
        except ValueError:
            published_at = None

    async with AsyncSessionLocal() as session:
        # ON CONFLICT DO NOTHING handles the case where the same
        # (source, external_id) appears twice — duplicate events
        # don't crash us, they just no-op.
        stmt = pg_insert(Mention).values(
            topic_id=topic_id,
            source=source,
            external_id=external_id,
            url=raw_event.get("url"),
            author=raw_event.get("author"),
            title=raw_event.get("title"),
            content=raw_event.get("content"),
            content_published_at=published_at,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            entities=entities,
            summary=summary,
            raw_payload=raw_event.get("raw_payload"),
            analyzed_at=datetime.now(timezone.utc) if analysis else None,
        ).on_conflict_do_nothing(index_elements=["source", "external_id"])

        result = await session.execute(stmt)
        await session.commit()

        # If the row was inserted, fetch it to get its id and user_id.
        # If not (conflict), look up the existing row.
        loaded = await session.execute(
            select(Mention).where(
                Mention.source == source,
                Mention.external_id == external_id,
            )
        )
        mention = loaded.scalar_one_or_none()
        if mention is None:
            logger.warning("Could not load mention after upsert: %s/%s", source, external_id)
            return

        # Need user_id for the websocket event — fetch it via topic.
        topic_row = await session.get(Topic, topic_id)
        if topic_row is None:
            logger.warning("Topic %s vanished; skipping analyzed event", topic_id)
            return
        user_id = topic_row.user_id

    # Publish to mentions.analyzed for the websocket service.
    analyzed_event = {
        "mention_id": str(mention.id),
        "topic_id": str(topic_id),
        "user_id": str(user_id),
        "source": source,
        "title": raw_event.get("title"),
        "url": raw_event.get("url"),
        "sentiment_score": sentiment_score,
        "sentiment_label": sentiment_label,
        "entities": entities,
        "summary": summary,
    }
    await publish_event(
        settings.kafka_topic_mentions_analyzed,
        analyzed_event,
        key=str(topic_id),
    )


# ----------------------------------------------------------------------
# Consume loop
# ----------------------------------------------------------------------

REQUIRED_FIELDS = ("source", "external_id", "topic_id")


def _validate_event(event: Any) -> str | None:
    """
    Return why a mentions.raw event can never be processed, or None if it's
    usable. These are permanent problems: retrying won't fix them, so the
    message goes to the dead-letter topic.
    """
    if not isinstance(event, dict):
        return "event is not a JSON object"
    for field in REQUIRED_FIELDS:
        if not event.get(field):
            return f"missing field {field!r}"
    try:
        UUID(str(event["topic_id"]))
    except ValueError:
        return "topic_id is not a valid UUID"
    return None


# Errors that mean "a dependency is unreachable right now", so the same work
# will succeed later. Everything else is treated as permanent for that mention.
TRANSIENT_ERRORS = (OSError, OperationalError, InterfaceError, PoolTimeoutError, KafkaError)

# Before giving up on a batch, retry a transient failure in place, so a short
# database blip doesn't cost another Groq call (1 + 2 + 4 + 8 = 15 s of waiting).
PERSIST_ATTEMPTS = 5
PERSIST_BACKOFF_SECONDS = 1.0


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, TRANSIENT_ERRORS):
        return True
    return isinstance(exc, DBAPIError) and bool(exc.connection_invalidated)


async def _persist_with_retry(raw_event: dict[str, Any], analysis: Any) -> None:
    """Store one mention, retrying transient failures with exponential backoff."""
    delay = PERSIST_BACKOFF_SECONDS
    for attempt in range(1, PERSIST_ATTEMPTS + 1):
        try:
            await _persist_and_emit(raw_event, analysis)
            return
        except Exception as exc:
            if not _is_transient(exc) or attempt == PERSIST_ATTEMPTS:
                raise
            logger.warning(
                "Transient error storing %s/%s (attempt %d/%d): %s; retrying in %.0fs",
                raw_event.get("source"), raw_event.get("external_id"),
                attempt, PERSIST_ATTEMPTS, exc.__class__.__name__, delay,
            )
            await asyncio.sleep(delay)
            delay *= 2


async def _process_batch(batch: list[tuple[Any, dict[str, Any]]]) -> None:
    """
    Analyze a batch in one Groq call, then persist and emit each result.

    `batch` holds (Kafka message, decoded event) pairs; the message is kept so
    a mention that can't be stored can be sent to the dead-letter topic.

    Per mention:
    - Transient failure (database or Kafka unreachable): retried in place;
      if it still fails, it's raised on purpose, so the caller does NOT
      commit and the whole batch is re-delivered after the worker restarts.
    - Any other failure (the database rejects the row, unexpected data, or a
      bug): the mention is dead-lettered and the batch carries on, so one
      bad row can never stall the pipeline.
    """
    if not batch:
        return

    events = [event for _, event in batch]
    logger.info("Analyzing batch of %d mention(s) via Groq (%s)...", len(events), settings.groq_model)
    try:
        analyses = await _analyze_batch(events)
    except Exception:
        logger.exception("Groq analysis failed; persisting raw mentions only")
        analyses = [None] * len(events)

    stored = 0
    for (msg, raw_event), analysis in zip(batch, analyses):
        try:
            await _persist_with_retry(raw_event, analysis)
            stored += 1
        except Exception as exc:
            if _is_transient(exc):
                raise
            if isinstance(exc, (IntegrityError, DataError)):
                logger.warning(
                    "Database rejected mention %s/%s: %s",
                    raw_event.get("source"), raw_event.get("external_id"), exc.__class__.__name__,
                )
            else:
                logger.exception(
                    "Unexpected error storing mention %s/%s",
                    raw_event.get("source"), raw_event.get("external_id"),
                )
            await send_to_dead_letter(
                msg, f"could not store mention: {exc.__class__.__name__}: {str(exc)[:300]}"
            )

    analyzed = sum(1 for a in analyses if a is not None)
    logger.info("Batch done: %d/%d analyzed, %d stored", analyzed, len(events), stored)


async def _consume_mentions() -> None:
    """
    Consume mentions.raw, accumulate in batches, flush periodically.

    Offsets are committed only after a batch has been fully written to
    Postgres (at-least-once). Unprocessable messages go to the dead-letter
    topic instead of crashing the loop.
    """
    topic = settings.kafka_topic_mentions_raw
    logger.info("Subscribing to Kafka topic %r as group 'analyzer'", topic)

    batch: list[tuple[Any, dict[str, Any]]] = []
    last_flush = asyncio.get_running_loop().time()

    async with make_consumer(
        topic,
        group_id="analyzer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    ) as consumer:
        while not _stop_event.is_set():
            try:
                # Wait up to BATCH_FLUSH_SECONDS for the next message.
                msg = await asyncio.wait_for(
                    consumer.getone(), timeout=BATCH_FLUSH_SECONDS
                )
            except asyncio.TimeoutError:
                msg = None  # No new message — fall through and check if we should flush.

            if msg is not None:
                try:
                    event = deserialize_event(msg.value)
                    problem = _validate_event(event)
                except Exception as exc:  # undecodable bytes or invalid JSON
                    event, problem = None, f"undecodable message: {exc}"

                if problem:
                    await send_to_dead_letter(msg, problem)
                    if not batch:
                        # Nothing else is pending, so it's safe to move the
                        # committed offset past the bad message now.
                        await commit_offsets(consumer)
                else:
                    batch.append((msg, event))

            now = asyncio.get_running_loop().time()
            full = len(batch) >= BATCH_SIZE
            stale = batch and (now - last_flush) >= BATCH_FLUSH_SECONDS

            if full or stale:
                await _process_batch(batch)
                # Commit only now that every mention in the batch is stored.
                await commit_offsets(consumer)
                batch = []
                last_flush = now

        # Graceful stop (standalone service, SIGTERM): finish and commit.
        # If the task is cancelled instead, this is skipped and nothing is
        # committed, so the batch is simply re-delivered on the next start.
        if batch:
            logger.info("Draining final batch of %d on shutdown...", len(batch))
            await _process_batch(batch)
            await commit_offsets(consumer)


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _stop_event.set)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await get_producer()  # warm up

    try:
        await _consume_mentions()
    finally:
        await close_producer()
        await engine.dispose()
        logger.info("Analyzer shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())