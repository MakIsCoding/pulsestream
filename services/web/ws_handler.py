"""
WebSocket handler — runs inside the combined web service.

Manages live browser connections and fans out analyzed mentions to the
right user. Extracted from services/websocket/main.py so everything can
run in one process for Render's free tier.
"""

import asyncio
import logging
from uuid import UUID

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect, status

from shared.config import settings
from shared.jwt import InvalidTokenError, decode_access_token
from shared.kafka_client import deserialize_event, make_consumer

logger = logging.getLogger("pulsestream.websocket")


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[UUID, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, user_id: UUID, ws: WebSocket) -> None:
        async with self._lock:
            self._connections.setdefault(user_id, set()).add(ws)
        logger.info("WS connected: user=%s (%d total users)", user_id, len(self._connections))

    async def disconnect(self, user_id: UUID, ws: WebSocket) -> None:
        async with self._lock:
            sockets = self._connections.get(user_id)
            if sockets:
                sockets.discard(ws)
                if not sockets:
                    del self._connections[user_id]
        logger.info("WS disconnected: user=%s", user_id)

    async def push_to_user(self, user_id: UUID, payload: dict) -> int:
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
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections.get(user_id, set()).discard(ws)
                if user_id in self._connections and not self._connections[user_id]:
                    del self._connections[user_id]
        return sent


manager = ConnectionManager()
router = APIRouter(tags=["websocket"])


async def consume_analyzed_mentions() -> None:
    """Background task: fan out mentions.analyzed events to connected browsers."""
    topic = settings.kafka_topic_mentions_analyzed
    logger.info("WS consumer: subscribing to %r", topic)
    while True:
        try:
            async with make_consumer(topic, group_id="websocket") as consumer:
                async for msg in consumer:
                    try:
                        event = deserialize_event(msg.value)
                    except Exception:
                        logger.exception("Bad WS event payload, skipping")
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
                        logger.info("Pushed mention=%s to user=%s (%d socket(s))",
                                    event.get("mention_id"), user_id, sent)
        except asyncio.CancelledError:
            logger.info("WS consumer cancelled.")
            return
        except Exception:
            logger.exception("WS Kafka consumer crashed; reconnecting in 5s")
            await asyncio.sleep(5)


@router.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    token: str | None = Query(default=None),
) -> None:
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
    await ws.send_json({"type": "hello", "user_id": str(user_id)})

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await manager.disconnect(user_id, ws)
