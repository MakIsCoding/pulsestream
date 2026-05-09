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
# If only code changes, Docker reuses the cached pip install — much faster builds.
COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/web/requirements.txt /app/services/web/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/web/requirements.txt

# Now copy the actual code. Code changes invalidate this layer but not pip.
COPY shared /app/shared
COPY services/web /app/services/web
COPY frontend /app/frontend

# Make `from shared...` imports work
ENV PYTHONPATH=/app

# Health check — Docker uses this to know if the container is "ready"
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1

# --reload makes uvicorn auto-restart on code changes (dev only).
# When we deploy to production we'll remove --reload and add multiple workers.
CMD ["uvicorn", "services.web.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]