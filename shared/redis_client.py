"""
Async Redis client wrapper.

Provides a single shared Redis connection per process. Used for:
- Caching (hot data, GET responses)
- Rate limiting (token bucket counters)
- Pub/sub (lightweight cross-service notifications, when Kafka is overkill)

Usage:
    from shared.redis_client import get_redis

    redis = await get_redis()
    await redis.set("key", "value", ex=60)
    value = await redis.get("key")

The connection is lazily created on first call and reused after that.
"""

from typing import Optional

import redis.asyncio as redis_lib
from redis.asyncio import Redis

from shared.config import settings


_redis: Optional[Redis] = None


async def get_redis() -> Redis:
    """
    Returns the shared async Redis client.

    Lazily initializes on first call. Subsequent calls return the same client.
    Connection pool is managed internally by redis-py.
    """
    global _redis
    if _redis is None:
        _redis = redis_lib.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,  # so .get() returns str instead of bytes
            max_connections=20,
            health_check_interval=30,
        )
    return _redis


async def close_redis() -> None:
    """
    Closes the shared Redis connection. Call this on graceful shutdown
    (FastAPI lifespan, worker exit handler).
    """
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None