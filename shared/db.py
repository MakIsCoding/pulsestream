"""
Async Postgres connection setup.

Provides:
- `engine`: the async SQLAlchemy engine (connection pool)
- `AsyncSessionLocal`: factory for creating async DB sessions
- `get_db()`: FastAPI dependency that yields a session per request
- `Base`: declarative base class for ORM models (used in models.py)

Usage in FastAPI routes:
    from shared.db import get_db
    from sqlalchemy.ext.asyncio import AsyncSession
    from fastapi import Depends

    @app.get("/things")
    async def list_things(db: AsyncSession = Depends(get_db)):
        result = await db.execute(select(Thing))
        return result.scalars().all()

Usage in workers (no FastAPI):
    from shared.db import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        # do work
        await session.commit()
"""

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from shared.config import settings


class Base(DeclarativeBase):
    """Base class for all ORM models. Models inherit from this."""
    pass


# The engine is the connection pool. One per process.
# echo=False keeps SQL queries out of the logs in production;
# flip to True locally if you want to see every query.
engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,  # checks connection health before using it
    pool_size=10,
    max_overflow=20,
)

# Session factory. Each request/task gets its own session via this.
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,  # objects stay usable after commit
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency. Yields a database session and ensures it's closed.

    The async with block guarantees the session is closed even if the
    request handler raises. Sessions are NOT shared across requests.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        # Note: we don't commit here — request handlers commit explicitly.
        # This is intentional: implicit commits hide bugs.