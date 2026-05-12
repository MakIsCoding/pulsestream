"""
Topics routes — CRUD for the topics a user wants to track.

All endpoints require authentication. A user only sees and modifies
their own topics; all queries filter by current_user.id.

Endpoints:
- POST   /topics       — create a new topic
- GET    /topics       — list current user's topics
- GET    /topics/{id}  — get one topic by id
- PATCH  /topics/{id}  — update name/keywords/is_active
- DELETE /topics/{id}  — delete a topic (cascades to mentions)

Side effect: creating a topic publishes a `topics.created` event to
Kafka so the scheduler can start ingesting for it.
"""

import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

logger = logging.getLogger("pulsestream.web.topics")

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import settings
from shared.db import get_db
from shared.kafka_client import publish_event
from shared.models import Topic, TopicDigest, User
from shared.schemas import (
    TopicCreate,
    TopicCreatedEvent,
    TopicDigestRead,
    TopicRead,
    TrendPoint,
    TrendResponse,
    TopicUpdate,
)

from services.web.security.dependencies import get_current_user


router = APIRouter(prefix="/topics", tags=["topics"])


@router.post("", response_model=TopicRead, status_code=status.HTTP_201_CREATED)
async def create_topic(
    payload: TopicCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Topic:
    """
    Create a new topic for the current user.

    Publishes a `topics.created` event to Kafka so the scheduler
    can begin ingesting mentions for this topic.
    """
    topic = Topic(
        user_id=current_user.id,
        name=payload.name,
        keywords=payload.keywords,
    )
    db.add(topic)

    try:
        await db.commit()
    except IntegrityError:
        # Unique constraint on (user_id, name) caught a duplicate.
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"You already have a topic named '{payload.name}'",
        )

    await db.refresh(topic)

    # Notify downstream services. We use topic_id as the Kafka key so
    # all events for this topic are partition-ordered.
    event = TopicCreatedEvent(
        topic_id=topic.id,
        user_id=current_user.id,
        name=topic.name,
        keywords=topic.keywords,
    )
    await publish_event(
        settings.kafka_topic_topics_created,
        event.model_dump(mode="json"),
        key=str(topic.id),
    )

    return topic


@router.get("", response_model=list[TopicRead])
async def list_topics(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> list[Topic]:
    """List all topics belonging to the current user."""
    result = await db.execute(
        select(Topic)
        .where(Topic.user_id == current_user.id)
        .order_by(Topic.created_at.desc())
    )
    return list(result.scalars().all())


@router.get("/{topic_id}", response_model=TopicRead)
async def get_topic(
    topic_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Topic:
    """Get a single topic by id (must belong to the current user)."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)
    return topic


@router.patch("/{topic_id}", response_model=TopicRead)
async def update_topic(
    topic_id: UUID,
    payload: TopicUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Topic:
    """Update fields on an existing topic."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)

    # Apply only the fields that were actually sent in the request.
    # `exclude_unset=True` distinguishes "not sent" from "sent as null".
    updates = payload.model_dump(exclude_unset=True)
    for field, value in updates.items():
        setattr(topic, field, value)

    try:
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A topic with that name already exists",
        )

    await db.refresh(topic)
    return topic


@router.patch("/{topic_id}/pause", response_model=TopicRead)
async def pause_topic(
    topic_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Topic:
    """Pause ingestion for a topic. The scheduler skips inactive topics."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)
    topic.is_active = False
    await db.commit()
    await db.refresh(topic)
    return topic


@router.patch("/{topic_id}/resume", response_model=TopicRead)
async def resume_topic(
    topic_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Topic:
    """Resume ingestion for a paused topic."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)
    topic.is_active = True
    await db.commit()
    await db.refresh(topic)
    return topic


@router.delete("/{topic_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_topic(
    topic_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    """Delete a topic and all its associated mentions (cascade)."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)
    await db.delete(topic)
    await db.commit()


@router.get("/{topic_id}/digest/latest", response_model=TopicDigestRead | None)
async def get_latest_digest(
    topic_id: UUID,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> TopicDigest | None:
    """Return the most recent digest; regenerate in background if stale or missing."""
    topic = await _load_owned_topic(db, topic_id, current_user.id)
    result = await db.execute(
        select(TopicDigest)
        .where(TopicDigest.topic_id == topic_id)
        .order_by(TopicDigest.generated_at.desc())
        .limit(1)
    )
    digest = result.scalar_one_or_none()

    stale_cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)
    if digest is None or digest.generated_at < stale_cutoff:
        background_tasks.add_task(_regenerate_digest, topic)

    return digest


async def _regenerate_digest(topic: Topic) -> None:
    """Background task: generate a fresh digest for a topic if enough data exists."""
    try:
        from services.scheduler.main import Scheduler
        await Scheduler()._generate_digest_for_topic(topic)
    except Exception:
        pass


@router.get("/{topic_id}/trend", response_model=TrendResponse)
async def get_topic_trend(
    topic_id: UUID,
    bucket: str = Query("hour", pattern="^(hour|day)$"),
    window: str = Query("24h", pattern="^(24h|7d)$"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> TrendResponse:
    """Return avg sentiment bucketed by hour (default) or day over a rolling window."""
    await _load_owned_topic(db, topic_id, current_user.id)

    interval_map = {"24h": "24 hours", "7d": "7 days"}
    interval = interval_map[window]

    # date_trunc field arg must be a SQL literal, not a bind parameter.
    # bucket is validated by the pattern= constraint above.
    try:
        rows = (
            await db.execute(
                text(f"""
                    SELECT
                        date_trunc('{bucket}', ingested_at)  AS ts,
                        AVG(sentiment_score)                 AS avg_score,
                        CAST(COUNT(*) AS INTEGER)            AS cnt
                    FROM mentions
                    WHERE topic_id = CAST(:topic_id AS uuid)
                      AND ingested_at >= NOW() - CAST(:interval AS INTERVAL)
                      AND analyzed_at IS NOT NULL
                    GROUP BY 1
                    ORDER BY 1
                """),
                {"topic_id": str(topic_id), "interval": interval},
            )
        ).fetchall()

        # Source breakdown and overall stats for the same window.
        src_rows = (
            await db.execute(
                text("""
                    SELECT source,
                           CAST(COUNT(*) AS INTEGER)  AS cnt,
                           AVG(sentiment_score)       AS avg_score
                    FROM mentions
                    WHERE topic_id = CAST(:topic_id AS uuid)
                      AND ingested_at >= NOW() - CAST(:interval AS INTERVAL)
                      AND analyzed_at IS NOT NULL
                    GROUP BY source
                """),
                {"topic_id": str(topic_id), "interval": interval},
            )
        ).fetchall()
    except Exception:
        logger.exception("Trend query failed for topic %s", topic_id)
        raise

    sources = {r[0]: r[1] for r in src_rows}
    total = sum(r[1] for r in src_rows)
    # Weighted average sentiment — cast to float so Pydantic never sees Decimal.
    avg_sentiment: float | None = None
    if total:
        weighted = sum((float(r[2]) if r[2] is not None else 0.0) * r[1] for r in src_rows)
        avg_sentiment = weighted / total

    points = [
        TrendPoint(bucket=r[0], avg_score=float(r[1]) if r[1] is not None else None, count=r[2])
        for r in rows
    ]
    return TrendResponse(
        points=points, bucket=bucket, window=window,
        total=total, avg_sentiment=avg_sentiment, sources=sources,
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

async def _load_owned_topic(
    db: AsyncSession, topic_id: UUID, user_id: UUID
) -> Topic:
    """
    Loads a topic by id, ensuring it belongs to the given user.

    Returns the Topic or raises 404 if it doesn't exist OR is owned
    by a different user. (Same response in both cases — don't leak
    whether a topic_id exists belonging to someone else.)
    """
    result = await db.execute(
        select(Topic).where(Topic.id == topic_id, Topic.user_id == user_id)
    )
    topic = result.scalar_one_or_none()
    if topic is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found",
        )
    return topic