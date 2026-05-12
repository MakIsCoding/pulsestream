# WebSocket service — pushes mentions.analyzed events to connected browsers
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/websocket/requirements.txt /app/services/websocket/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/websocket/requirements.txt

COPY shared /app/shared
COPY services/websocket /app/services/websocket

ENV PYTHONPATH=/app

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8001}/healthz || exit 1

CMD uvicorn services.websocket.main:app --host 0.0.0.0 --port ${PORT:-8001}