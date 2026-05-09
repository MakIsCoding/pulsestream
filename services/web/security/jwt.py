"""
JWT issuance and verification.

Tokens encode the user_id (`sub`) and an expiry (`exp`). They're signed
with HMAC using JWT_SECRET from config. No DB lookup is needed to verify
a token — verification is a signature check.

Usage:
    token = create_access_token(user_id)
    user_id = decode_access_token(token)  # raises if invalid/expired
"""

from datetime import datetime, timedelta, timezone
from uuid import UUID

from jose import JWTError, jwt

from shared.config import settings


class InvalidTokenError(Exception):
    """Raised when a JWT is missing, malformed, or expired."""


def create_access_token(user_id: UUID) -> str:
    """Issues a signed JWT for the given user_id."""
    expire = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expiry_hours)
    payload = {
        "sub": str(user_id),
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> UUID:
    """
    Verifies the JWT signature and expiry. Returns the user_id.
    Raises InvalidTokenError on any problem.
    """
    try:
        payload = jwt.decode(
            token, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
        )
    except JWTError as e:
        raise InvalidTokenError(f"Invalid token: {e}") from e

    sub = payload.get("sub")
    if not sub:
        raise InvalidTokenError("Token missing 'sub' claim")

    try:
        return UUID(sub)
    except ValueError as e:
        raise InvalidTokenError("Token 'sub' is not a valid UUID") from e