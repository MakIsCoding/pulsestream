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

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
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
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> TopicDigest | None:
    """Return the most recent digest for a topic, or null if none exist yet."""
    await _load_owned_topic(db, topic_id, current_user.id)
    result = await db.execute(
        select(TopicDigest)
        .where(TopicDigest.topic_id == topic_id)
        .order_by(TopicDigest.generated_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


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
    rows = (
        await db.execute(
            text(f"""
                SELECT
                    date_trunc('{bucket}', ingested_at) AS ts,
                    AVG(sentiment_score)               AS avg_score,
                    COUNT(*)::int                      AS cnt
                FROM mentions
                WHERE topic_id = :topic_id::uuid
                  AND ingested_at >= NOW() - CAST(:interval AS INTERVAL)
                  AND analyzed_at IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """),
            {"topic_id": str(topic_id), "interval": interval},
        )
    ).fetchall()

    points = [TrendPoint(bucket=r[0], avg_score=r[1], count=r[2]) for r in rows]
    return TrendResponse(points=points, bucket=bucket, window=window)


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