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

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import settings
from shared.db import get_db
from shared.kafka_client import publish_event
from shared.models import Topic, User
from shared.schemas import (
    TopicCreate,
    TopicCreatedEvent,
    TopicRead,
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