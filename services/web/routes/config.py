from fastapi import APIRouter

from shared.config import settings

router = APIRouter(prefix="/api", tags=["config"])


@router.get("/config")
async def get_config():
    """Returns runtime config the frontend needs (e.g. WebSocket URL)."""
    return {"ws_url": settings.websocket_url}
