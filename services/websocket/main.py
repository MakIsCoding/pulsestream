"""
PulseStream WebSocket service.

Browsers connect to ws://host:8001/ws?token=<JWT>. We:
1. Validate the JWT to identify the user.
2. Register the connection in an in-memory map: user_id -> set of websockets.
3. A background task consumes `mentions.analyzed` from Kafka and, for each
   event, pushes it to every websocket registered under that event's user_id.
4. On disconnect, we deregister and clean up.

Single-instance design. If we ever scale to multiple replicas, browsers
connected to replica A can't be reached by replica B's Kafka consumer.
The fix at that point is Redis pub/sub (replica-to-replica fanout) — we'll
add it when needed. For a portfolio app, single instance is fine.

Per-user routing matters: even though every analyzer event hits this
service, only the user who owns the topic should see their own mentions.
The `user_id` field in MentionAnalyzedEvent is the routing key.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse

from shared.config import settings
from shared.kafka_client import (
    close_producer,
    deserialize_event,
    make_consumer,
)

from shared.jwt import InvalidTokenError, decode_access_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsestream.websocket")


# ----------------------------------------------------------------------
# Connection registry
# ----------------------------------------------------------------------

class ConnectionManager:
    """
    Tracks live WebSocket connections, indexed by user_id.

    A single user can have multiple connections (multiple browser tabs,
    desktop + mobile, etc.) — that's why each user maps to a SET of sockets.
    """

    def __init__(self) -> None:
        self._connections: dict[UUID, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: UUID, ws: WebSocket) -> None:
        async with self._lock:
            self._connections.setdefault(user_id, set()).add(ws)
        logger.info(
            "Connected: user=%s (now %d connection(s) for this user, %d users total)",
            user_id, len(self._connections[user_id]), len(self._connections),
        )

    async def disconnect(self, user_id: UUID, ws: WebSocket) -> None:
        async with self._lock:
            sockets = self._connections.get(user_id)
            if sockets is None:
                return
            sockets.discard(ws)
            if not sockets:
                del self._connections[user_id]
        logger.info("Disconnected: user=%s", user_id)

    async def push_to_user(self, user_id: UUID, payload: dict) -> int:
        """
        Send a JSON payload to every connection belonging to user_id.

        Returns the number of successful sends. Drops dead sockets along
        the way (a browser tab closed without an explicit disconnect, etc.)
        """
        async with self._lock:
            sockets = list(self._connections.get(user_id, set()))

        if not sockets:
            return 0

        sent = 0
        dead: list[WebSocket] = []
        for ws in sockets:
            try:
                await ws.send_json(payload)
                sent += 1
            except Exception:
                # The browser likely went away. Mark for removal.
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections.get(user_id, set()).discard(ws)
                if user_id in self._connections and not self._connections[user_id]:
                    del self._connections[user_id]

        return sent


manager = ConnectionManager()


# ----------------------------------------------------------------------
# Background Kafka consumer
# ----------------------------------------------------------------------

_consumer_task: asyncio.Task | None = None


async def _consume_analyzed_mentions() -> None:
    """
    Long-lived task: consume `mentions.analyzed` from Kafka and fan out
    to whichever connected users care about each event.
    """
    topic = settings.kafka_topic_mentions_analyzed
    logger.info("Subscribing to Kafka topic %r as group 'websocket'", topic)

    # Use a unique group_id per replica if we ever add multiple replicas;
    # for now a single shared group is fine because we have one instance.
    while True:
        try:
            async with make_consumer(topic, group_id="websocket") as consumer:
                async for msg in consumer:
                    try:
                        event = deserialize_event(msg.value)
                    except Exception:
                        logger.exception("Bad event payload, skipping")
                        continue

                    user_id_str = event.get("user_id")
                    if not user_id_str:
                        continue
                    try:
                        user_id = UUID(user_id_str)
                    except ValueError:
                        continue

                    sent = await manager.push_to_user(user_id, {
                        "type": "mention.analyzed",
                        "data": event,
                    })
                    if sent:
                        logger.info(
                            "Pushed mention=%s to user=%s (%d socket(s))",
                            event.get("mention_id"), user_id, sent,
                        )
        except asyncio.CancelledError:
            logger.info("Consumer task cancelled. Exiting.")
            return
        except Exception:
            # Don't die forever if Kafka burps. Log and reconnect.
            logger.exception("Kafka consumer crashed; reconnecting in 5s")
            await asyncio.sleep(5)


# ----------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_task
    logger.info("Starting WebSocket service...")
    _consumer_task = asyncio.create_task(_consume_analyzed_mentions())
    yield
    logger.info("Shutting down WebSocket service...")
    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
    await close_producer()
    logger.info("WebSocket service shut down cleanly.")


app = FastAPI(
    title="PulseStream WebSocket",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/healthz", tags=["health"])
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "connected_users": len(manager._connections)})


@app.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    token: str | None = Query(default=None),
) -> None:
    """
    Browser connects with: ws://host:8001/ws?token=<JWT>

    On a successful handshake we register the user; from then on, every
    analyzed mention belonging to this user is pushed live.
    """
    if not token:
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    try:
        user_id = decode_access_token(token)
    except InvalidTokenError:
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await ws.accept()
    await manager.connect(user_id, ws)

    # Send a hello so the client knows the connection is live.
    await ws.send_json({"type": "hello", "user_id": str(user_id)})

    try:
        # We don't expect inbound messages right now (server -> client only),
        # but we have to await something or FastAPI considers the handler done
        # and closes the socket. Reading lets us also detect client disconnects.
        while True:
            await ws.receive_text()  # client may send pings/keepalives
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(user_id, ws)