"""
Authentication routes: register, login, and current user.

Endpoints:
- POST /auth/register — create a new user, return JWT
- POST /auth/login    — verify credentials, return JWT
- GET  /auth/me       — return the current user (requires auth)
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_db
from shared.models import Mention, Topic, User
from shared.redis_client import get_redis
from shared.schemas import TokenResponse, UserCreate, UserLogin, UserRead, UserStats

from services.web.security.dependencies import get_current_user
from shared.jwt import create_access_token
from services.web.security.passwords import hash_password, verify_password


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post(
    "/register",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register(
    payload: UserCreate,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """
    Register a new user. Returns a JWT immediately so the client
    doesn't have to log in again after registration.
    """
    # Cheap pre-check; the real guard is Postgres's unique constraint below.
    existing = await db.execute(select(User).where(User.email == payload.email))
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )

    user = User(
        email=payload.email,
        password_hash=hash_password(payload.password),
    )
    db.add(user)

    try:
        await db.commit()
    except IntegrityError:
        # Race condition: another request registered the same email
        # between our pre-check and the commit. Postgres caught it.
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )

    await db.refresh(user)

    token = create_access_token(user.id)
    return TokenResponse(access_token=token)


@router.post("/login", response_model=TokenResponse)
async def login(
    payload: UserLogin,
    db: AsyncSession = Depends(get_db),
) -> TokenResponse:
    """Verify credentials and issue a JWT."""
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()

    # Same generic message regardless of which check failed —
    # don't leak whether the email exists.
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled",
        )

    token = create_access_token(user.id)
    return TokenResponse(access_token=token)


@router.post("/heartbeat", status_code=status.HTTP_204_NO_CONTENT)
async def heartbeat(current_user: User = Depends(get_current_user)) -> None:
    """Marks the user as active. Scheduler only ingests for users active in the last 15 min."""
    redis = await get_redis()
    await redis.set(f"user:active:{current_user.id}", "1", ex=900)


@router.get("/me", response_model=UserRead)
async def me(current_user: User = Depends(get_current_user)) -> User:
    """Return the currently authenticated user."""
    return current_user


@router.get("/me/stats", response_model=UserStats)
async def me_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> UserStats:
    """Return aggregate topic and mention counts for the current user."""
    topic_count = (
        await db.execute(
            select(func.count()).select_from(Topic).where(Topic.user_id == current_user.id)
        )
    ).scalar_one()

    mention_count = (
        await db.execute(
            select(func.count())
            .select_from(Mention)
            .join(Topic, Mention.topic_id == Topic.id)
            .where(Topic.user_id == current_user.id)
        )
    ).scalar_one()

    return UserStats(topic_count=topic_count, mention_count=mention_count)