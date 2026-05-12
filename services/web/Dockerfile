# Web API — FastAPI service
FROM python:3.12-slim

WORKDIR /app

# Install OS deps needed by asyncpg, bcrypt, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so Docker can cache the pip install layer.
COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/web/requirements.txt /app/services/web/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/web/requirements.txt

# Copy all service code — the combined process imports from scheduler,
# ingester, and analyzer modules at runtime.
COPY shared /app/shared
COPY services/web /app/services/web
COPY services/scheduler /app/services/scheduler
COPY services/ingester /app/services/ingester
COPY services/analyzer /app/services/analyzer
COPY frontend /app/frontend

# Make `from shared...` imports work
ENV PYTHONPATH=/app

# Health check — Docker uses this to know if the container is "ready"
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/healthz || exit 1

CMD uvicorn services.web.main:app --host 0.0.0.0 --port ${PORT:-8000}