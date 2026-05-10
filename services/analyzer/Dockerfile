# Analyzer — consumes mentions.raw, enriches with Gemini, publishes mentions.analyzed
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY shared/requirements.txt /app/shared/requirements.txt
COPY services/analyzer/requirements.txt /app/services/analyzer/requirements.txt

RUN pip install --no-cache-dir -r /app/shared/requirements.txt
RUN pip install --no-cache-dir -r /app/services/analyzer/requirements.txt

COPY shared /app/shared
COPY services/analyzer /app/services/analyzer

ENV PYTHONPATH=/app

CMD ["python", "-u", "-m", "services.analyzer.main"]