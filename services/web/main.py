"""
PulseStream — combined web service.

Runs ALL five microservices in one process so the whole stack fits on
Render's free tier (750 h/month = exactly 1 always-on service):

  • FastAPI HTTP API (auth, topics, mentions, config)
  • WebSocket endpoint (/ws) — live mention push
  • Scheduler background task — periodic ingestion fan-out + digests
  • Ingester background task — fetches HN/Reddit, publishes mentions.raw
  • Analyzer background task — enriches mentions.raw via Groq

All four workers run as asyncio tasks on the same event loop as uvicorn.
They are cancelled cleanly on shutdown.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from shared.config import settings
from shared.db import engine, Base
from shared.kafka_client import close_producer, get_producer
from shared.redis_client import close_redis, get_redis
import shared.models  # noqa: F401 — registers all ORM classes with Base

from services.web.routes import auth, config, mentions, topics
from services.web.ws_handler import consume_analyzed_mentions, router as ws_router


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.web")


# ──────────────────────────────────────────────────────────────────────
# Background worker wrappers
# Each wrapper runs the service's core loop and handles CancelledError
# cleanly so the lifespan shutdown is tidy.
# ──────────────────────────────────────────────────────────────────────

async def _run_scheduler() -> None:
    from services.scheduler.main import Scheduler
    sched = Scheduler()
    sched._aps.add_job(
        sched._fan_out_ingestion_jobs, trigger="interval",
        seconds=settings.ingestion_interval_seconds,
        id="fan_out_ingestion", max_instances=1, coalesce=True,
    )
    sched._aps.add_job(
        sched._cleanup_old_mentions, trigger="interval",
        hours=24, id="retention_cleanup", max_instances=1, coalesce=True,
    )
    sched._aps.add_job(
        sched._generate_digests, trigger="interval",
        hours=6, id="generate_digests", max_instances=1, coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    sched._aps.add_job(
        sched._keep_alive_ping, trigger="interval",
        hours=6, id="keep_alive", max_instances=1, coalesce=True,
    )
    sched._aps.start()
    logger.info("Scheduler started (interval=%ds)", settings.ingestion_interval_seconds)
    await sched._fan_out_ingestion_jobs()
    try:
        await sched._consume_topic_events()
    except asyncio.CancelledError:
        pass
    finally:
        sched._aps.shutdown(wait=False)
        logger.info("Scheduler stopped.")


async def _run_ingester() -> None:
    from services.ingester.main import _consume_jobs
    logger.info("Ingester starting...")
    try:
        await _consume_jobs()
    except asyncio.CancelledError:
        pass
    logger.info("Ingester stopped.")


async def _run_analyzer() -> None:
    from services.analyzer.main import _consume_mentions
    logger.info("Analyzer starting...")
    try:
        await _consume_mentions()
    except asyncio.CancelledError:
        pass
    logger.info("Analyzer stopped.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up PulseStream (combined service)...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await get_redis()
    await get_producer()

    # Launch all background workers as concurrent asyncio tasks.
    bg_tasks = [
        asyncio.create_task(_run_scheduler(),   name="scheduler"),
        asyncio.create_task(_run_ingester(),    name="ingester"),
        asyncio.create_task(_run_analyzer(),    name="analyzer"),
        asyncio.create_task(consume_analyzed_mentions(), name="ws-consumer"),
    ]

    logger.info("All background workers started. Web API is ready.")
    yield

    logger.info("Shutting down...")
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)

    await close_producer()
    await close_redis()
    await engine.dispose()
    logger.info("Shutdown complete.")


app = FastAPI(
    title="PulseStream Web API",
    description="Real-time topic intelligence platform — REST API",
    version="0.1.0",
    lifespan=lifespan,
)


# CORS — allow the local frontend (and any deployed frontend later) to
# call the API. In production we'll tighten this to specific origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten before prod deploy
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------
# Routers
# ----------------------------------------------------------------------

app.include_router(auth.router)
app.include_router(topics.router)
app.include_router(mentions.router)
app.include_router(config.router)
app.include_router(ws_router)


# ----------------------------------------------------------------------
# Liveness / readiness
# ----------------------------------------------------------------------

@app.get("/healthz", tags=["health"])
async def healthz() -> JSONResponse:
    """
    Liveness probe. Returns 200 if the process is up.

    Used by:
    - Docker HEALTHCHECK in the Dockerfile
    - Kubernetes liveness probes (later)
    - Load balancers
    """
    return JSONResponse({"status": "ok"})


# Serve the frontend SPA. Must be mounted LAST so API routes take precedence.
# html=True makes / resolve to index.html.
_FRONTEND_DIR = Path(__file__).resolve().parent.parent.parent / "frontend"
app.mount("/", StaticFiles(directory=str(_FRONTEND_DIR), html=True), name="frontend")