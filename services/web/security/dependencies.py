"""
FastAPI dependencies for authentication.

The `get_current_user` dependency extracts the JWT from the Authorization
header, validates it, and loads the user from the database. Routes that
need authentication add `current_user: User = Depends(get_current_user)`
to their signature — that's it.

If the token is missing, invalid, or the user doesn't exist, FastAPI
automatically returns a 401 Unauthorized.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.db import get_db
from shared.models import User
from shared.jwt import decode_access_token, InvalidTokenError

# tokenUrl points to the login endpoint. FastAPI uses this to render the
# "Authorize" button correctly in the auto-generated /docs UI.
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """
    Resolves the current user from the JWT in the Authorization header.

    Raises 401 if:
    - No Authorization header
    - Invalid / expired / malformed JWT
    - Token's user_id doesn't match any user in the database
    - User exists but is_active=False
    """
    credentials_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        user_id = decode_access_token(token)
    except InvalidTokenError:
        raise credentials_error

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if user is None or not user.is_active:
        raise credentials_error

    return user