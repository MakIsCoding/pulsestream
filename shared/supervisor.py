"""
Keep long-running worker loops alive.

Each background worker (ingester, analyzer, the scheduler's topic consumer)
is an endless consume loop. If one raises, for example because Kafka or
Postgres was briefly unreachable, its asyncio task would otherwise just end,
and that stage of the pipeline would stay stopped until the whole process
restarted, while /healthz still reported "ok".

`supervise()` logs the crash and starts the loop again after an exponential
backoff. Because pipeline consumers commit offsets only after finishing their
work, a restarted loop resumes from the last committed offset, so nothing is
skipped.
"""

import asyncio
import logging
import time
from typing import Awaitable, Callable

logger = logging.getLogger("pulsestream.supervisor")

INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
# A run that lasted at least this long counts as healthy, so the next crash
# starts again from the shortest backoff.
HEALTHY_RUN_SECONDS = 60.0

# How many times each worker has been restarted. Exposed on /healthz.
restart_counts: dict[str, int] = {}


async def supervise(name: str, run: Callable[[], Awaitable[None]]) -> None:
    """
    Run `run()` until it returns normally (a requested stop), restarting it
    with exponential backoff whenever it raises. Cancellation is passed
    through so the app can shut down.
    """
    restart_counts.setdefault(name, 0)
    backoff = INITIAL_BACKOFF_SECONDS
    while True:
        started = time.monotonic()
        try:
            await run()
            logger.info("Worker %s stopped normally", name)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            if time.monotonic() - started >= HEALTHY_RUN_SECONDS:
                backoff = INITIAL_BACKOFF_SECONDS
            restart_counts[name] += 1
            logger.exception(
                "Worker %s crashed (restart #%d); restarting in %.0fs",
                name, restart_counts[name], backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
