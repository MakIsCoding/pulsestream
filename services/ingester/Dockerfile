# Ingester — fetches data from external sources, publishes mentions.raw to Kafka
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/ingester/requirements.txt /app/services/ingester/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/ingester/requirements.txt

COPY shared /app/shared
COPY services/ingester /app/services/ingester

ENV PYTHONPATH=/app

CMD ["python", "-u", "-m", "services.ingester.main"]