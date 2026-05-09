"""
Mentions read endpoint.

Mentions are produced by the ingester (raw events) and enriched by the
analyzer (sentiment, entities, summary). Both write to Postgres.

This service only reads them — paginated, filtered by topic, ordered
newest first. The user must own the topic.

Endpoints:
- GET /mentions  — paginated mentions for a topic
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_db
from shared.models import Mention, Topic, User
from shared.schemas import MentionsPage, MentionRead

from services.web.security.dependencies import get_current_user


router = APIRouter(prefix="/mentions", tags=["mentions"])


@router.get("", response_model=MentionsPage)
async def list_mentions(
    topic_id: UUID = Query(..., description="Topic to fetch mentions for"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    only_analyzed: bool = Query(
        True, description="If true, exclude mentions that haven't been analyzed yet"
    ),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> MentionsPage:
    """
    List mentions for one of the current user's topics.

    Paginated with limit + offset, ordered newest first by ingestion time.
    By default, only returns mentions that the analyzer has processed
    (have sentiment + entities filled in).
    """
    # Ownership check: the topic must belong to the current user.
    # We do this with a single query rather than a separate fetch
    # because it lets us fail fast without loading the topic row.
    owned = await db.execute(
        select(Topic.id).where(
            Topic.id == topic_id, Topic.user_id == current_user.id
        )
    )
    if owned.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Topic not found",
        )

    # Build the base filter — both the count and the page query use it.
    conditions = [Mention.topic_id == topic_id]
    if only_analyzed:
        conditions.append(Mention.analyzed_at.is_not(None))

    # Total count (for pagination metadata)
    total_result = await db.execute(
        select(func.count()).select_from(Mention).where(*conditions)
    )
    total = total_result.scalar_one()

    # Page of results
    page_result = await db.execute(
        select(Mention)
        .where(*conditions)
        .order_by(Mention.ingested_at.desc())
        .limit(limit)
        .offset(offset)
    )
    items = list(page_result.scalars().all())

    return MentionsPage(
        items=[MentionRead.model_validate(m) for m in items],
        total=total,
        limit=limit,
        offset=offset,
    )