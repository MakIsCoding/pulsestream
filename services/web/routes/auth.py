"""
Authentication routes: register, login, and current user.

Endpoints:
- POST /auth/register — create a new user, return JWT
- POST /auth/login    — verify credentials, return JWT
- GET  /auth/me       — return the current user (requires auth)
"""

import logging
import secrets

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from shared.config import settings
from shared.db import get_db
from shared.models import Mention, Topic, User
from shared.redis_client import get_redis
from shared.schemas import (
    ForgotPasswordRequest,
    PasswordChange,
    ResetPasswordRequest,
    TokenResponse,
    UserCreate,
    UserLogin,
    UserRead,
    UserStats,
)

from services.web.security.dependencies import get_current_user
from shared.jwt import create_access_token
from services.web.security.passwords import hash_password, verify_password

logger = logging.getLogger("pulsestream.web.auth")

_RESET_TTL = 3600  # 1 hour


async def _send_reset_email(to_email: str, reset_url: str) -> None:
    if not settings.resend_api_key:
        logger.warning("RESEND_API_KEY not set — reset link: %s", reset_url)
        return
    html = f"""
    <div style="font-family:sans-serif;max-width:480px;margin:auto">
      <h2 style="color:#6366f1">PulseStream</h2>
      <p>Someone requested a password reset for your account.</p>
      <p style="margin:24px 0">
        <a href="{reset_url}"
           style="background:#6366f1;color:#fff;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:600">
          Reset password
        </a>
      </p>
      <p style="color:#64748b;font-size:13px">This link expires in 1 hour. If you didn't request this, ignore this email.</p>
    </div>
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {settings.resend_api_key}"},
                json={
                    "from": "PulseStream <onboarding@resend.dev>",
                    "to": [to_email],
                    "subject": "Reset your PulseStream password",
                    "html": html,
                },
            )
            if resp.status_code >= 400:
                logger.error("Resend API error %d: %s", resp.status_code, resp.text)
    except Exception:
        logger.exception("Failed to send reset email to %s", to_email)


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


@router.post("/forgot-password", status_code=status.HTTP_204_NO_CONTENT)
async def forgot_password(
    payload: ForgotPasswordRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> None:
    """Send a password-reset link. Always returns 204 — never reveals whether the email exists."""
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalar_one_or_none()
    if user is None:
        return  # silent — don't leak whether the email is registered

    token = secrets.token_urlsafe(32)
    redis = await get_redis()
    await redis.set(f"pw_reset:{token}", str(user.id), ex=_RESET_TTL)

    base = str(request.base_url).rstrip("/")
    reset_url = f"{base}/?reset_token={token}"
    await _send_reset_email(user.email, reset_url)


@router.post("/reset-password", status_code=status.HTTP_204_NO_CONTENT)
async def reset_password(
    payload: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db),
) -> None:
    """Validate the reset token and update the password."""
    redis = await get_redis()
    raw = await redis.get(f"pw_reset:{payload.token}")
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Reset link is invalid or has expired",
        )
    user_id = raw.decode() if isinstance(raw, bytes) else raw

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User not found")

    user.password_hash = hash_password(payload.new_password)
    await db.commit()
    await redis.delete(f"pw_reset:{payload.token}")


@router.patch("/password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    payload: PasswordChange,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> None:
    """Change the current user's password. Requires the existing password."""
    if not verify_password(payload.current_password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect",
        )
    current_user.password_hash = hash_password(payload.new_password)
    await db.commit()


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