# Scheduler — periodic ingestion job dispatcher
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cached layer: install dependencies first
COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/scheduler/requirements.txt /app/services/scheduler/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/scheduler/requirements.txt

# Application code
COPY shared /app/shared
COPY services/scheduler /app/services/scheduler

ENV PYTHONPATH=/app

CMD ["python", "-u", "-m", "services.scheduler.main"]