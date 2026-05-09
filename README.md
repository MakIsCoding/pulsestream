# PulseStream

Real-time topic intelligence platform. Tracks mentions across HackerNews, Reddit, and RSS feeds. Runs AI sentiment + entity analysis. Live dashboard with WebSocket-driven updates.

## Architecture

A multi-service distributed system with:

- **Web API** — FastAPI, serves the dashboard and accepts user topics
- **WebSocket Server** — pushes live updates to the browser
- **Scheduler** — pulls new mentions from sources at intervals
- **Ingester** — fetches and normalizes data from each source
- **Analyzer** — runs AI sentiment and entity extraction via Gemini

Backed by Apache Kafka (event streaming), PostgreSQL (relational data), and Redis (caching).

## Stack

- **Backend**: Python, FastAPI, asyncio
- **Events**: Apache Kafka
- **Database**: PostgreSQL
- **Cache**: Redis
- **AI**: Google Gemini API
- **Containerization**: Docker, Docker Compose
- **Deployment**: Local dev → Neon + Upstash (managed) → Render/Fly.io

## Status

🚧 Under active development.
