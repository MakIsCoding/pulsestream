"""
PulseStream Web API — FastAPI application entrypoint.

This is what uvicorn runs. It wires together:
- Auth, topics, and mentions routers
- Startup/shutdown lifecycle (open and close shared connections cleanly)
- CORS for the frontend
- A /healthz endpoint for Docker/Kubernetes healthchecks
- Auto-generated OpenAPI docs at /docs

Run with:
    uvicorn services.web.main:app --reload
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.db import engine
from shared.kafka_client import close_producer, get_producer
from shared.redis_client import close_redis, get_redis

from services.web.routes import auth, mentions, topics


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.web")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs on app startup and shutdown.

    Startup: warm up connections to Postgres, Redis, and Kafka so the
    first real request doesn't pay the connection-setup cost.

    Shutdown: cleanly close everything so we don't leak connections or
    leave Kafka producer batches unflushed.
    """
    logger.info("Starting up PulseStream Web API...")

    # Warm up shared clients (lazy-init happens on first call;
    # we call them here so connection errors fail loudly at startup
    # rather than mysteriously on the first user request).
    await get_redis()
    await get_producer()

    logger.info("Startup complete. Web API is ready.")
    yield  # ← app runs here

    logger.info("Shutting down PulseStream Web API...")
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


@app.get("/", tags=["health"])
async def root() -> JSONResponse:
    """Friendly root endpoint."""
    return JSONResponse(
        {
            "service": "pulsestream-web",
            "version": "0.1.0",
            "docs": "/docs",
        }
    )