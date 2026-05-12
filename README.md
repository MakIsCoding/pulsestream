# PulseStream

Real-time topic intelligence platform. Track any subject across HackerNews and Reddit, get AI-powered sentiment analysis and entity extraction on every mention, and watch your dashboard update live as new content arrives.

**Live demo:** https://pulsestream-nj48.onrender.com

---

## What it does

1. You create a **topic** with a name and keywords (e.g. "NEET" → keywords: `NEET 2026`, `NEET 2027`)
2. Every **2 minutes**, PulseStream fetches matching posts from HackerNews and Reddit
3. Each post is analyzed by **Groq AI** — sentiment score, named entities, one-line summary
4. Results appear in your dashboard **live** via WebSocket, no refresh needed
5. A **Trend** tab shows volume and sentiment over time; a **Summary** tab shows an AI-generated digest of the last 24 hours

---

## Architecture

```
Browser
  │  HTTP  (auth, topics, mentions REST API)
  │  WSS   (/ws — live mention push, no polling)
  ▼
┌──────────────────────────────────────────────────────────────┐
│                 Render Free Tier (single process)             │
│                                                               │
│  FastAPI ──on topic create──► Kafka: topics.created          │
│                                                               │
│  ┌─ Background asyncio tasks (same event loop) ────────────┐ │
│  │                                                          │ │
│  │  Scheduler  ──every 2 min──► Kafka: ingestion.jobs       │ │
│  │             ◄── Kafka: topics.created (immediate ingest) │ │
│  │             runs digest generation every 6 h             │ │
│  │                                                          │ │
│  │  Ingester   ◄── Kafka: ingestion.jobs                    │ │
│  │             ├── HackerNews Algolia API                   │ │
│  │             ├── Reddit JSON API                          │ │
│  │             └──► Kafka: mentions.raw                     │ │
│  │                  (deduped via Redis 7-day cache)         │ │
│  │                                                          │ │
│  │  Analyzer   ◄── Kafka: mentions.raw  (batches of 5)      │ │
│  │             ├── Groq API  (sentiment · entities · summary)│ │
│  │             ├── Neon PostgreSQL  (persist mention row)   │ │
│  │             └──► Kafka: mentions.analyzed                │ │
│  │                                                          │ │
│  │  WS Consumer◄── Kafka: mentions.analyzed                 │ │
│  │             └──► WebSocket push to matching user         │ │
│  └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
         │                    │                    │
    ┌────▼────┐          ┌────▼────┐         ┌────▼─────┐
    │  Neon   │          │Upstash  │         │  Aiven   │
    │Postgres │          │  Redis  │         │  Kafka   │
    └─────────┘          └─────────┘         └──────────┘
```

All five logical services (web API, scheduler, ingester, analyzer, WebSocket consumer) run as concurrent asyncio tasks inside a single Python process. This fits the entire stack on Render's free tier (750 h/month).

---

## Data flow

```
User creates topic
      │
      ▼
   topics.created (Kafka)
      │
      ├──► Scheduler: queues immediate ingestion
      │
      └──► Every 2 min:
              ingestion.jobs (Kafka, one per source per active topic)
                    │
                    ▼
              Ingester fetches HN / Reddit
                    │  dedup check (Redis)
                    ▼
              mentions.raw (Kafka)
                    │
                    ▼
              Analyzer: Groq batch analysis
                    │  writes to Postgres
                    ▼
              mentions.analyzed (Kafka)
                    │
                    ▼
              WebSocket consumer pushes to browser
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12, FastAPI, asyncio |
| Event streaming | Apache Kafka (Aiven free tier) |
| Database | PostgreSQL (Neon serverless) |
| Cache / dedup | Redis (Upstash free tier) |
| AI analysis | Groq API — Llama 3.1 8B Instant |
| Frontend | Vanilla JS SPA, Chart.js, Tailwind CSS |
| Deployment | Render free tier (Docker, single service) |
| Keep-alive | UptimeRobot — pings `/healthz` every 5 min |

---

## Features

- **Live feed** — mentions appear instantly via WebSocket as they are analyzed
- **Sentiment analysis** — every post scored −1.0 → +1.0 with a label
- **Entity extraction** — people, products, companies pulled from each post
- **Trend chart** — volume bars + sentiment line over 24 h or 7 days
- **AI digest** — Groq-generated 2–3 sentence summary refreshed every 6 hours
- **Pause / resume** — stop ingestion for a topic without deleting it
- **Inactive user throttle** — ingestion pauses 15 min after you leave the page, preventing runaway API usage
- **Deduplication** — 7-day Redis cache prevents re-analyzing the same post
- **Data retention** — mentions older than 30 days are pruned automatically

---

## Local development

### Prerequisites

- Docker + Docker Compose
- Accounts for: [Neon](https://neon.tech), [Upstash](https://upstash.com), [Aiven](https://console.aiven.io), [Groq](https://console.groq.com)

### Setup

```bash
git clone https://github.com/your-username/pulsestream
cd pulsestream

cp .env.cloud.example .env.cloud
# fill in credentials (see comments inside the file)

docker compose -f docker-compose.cloud.yml up --build -d
```

Open http://localhost:8000

### Services started by docker-compose

All five services run independently in docker-compose (useful for local debugging). On Render they are combined into one process.

| Container | Port | Role |
|---|---|---|
| `pulsestream-web` | 8000 | FastAPI HTTP + frontend |
| `pulsestream-websocket` | 8001 | WebSocket push |
| `pulsestream-scheduler` | — | Periodic ingestion jobs |
| `pulsestream-ingester` | — | HN + Reddit fetcher |
| `pulsestream-analyzer` | — | Groq enrichment |

---

## Cloud deployment (Render)

The repo includes a `render.yaml` that deploys everything as a single free web service.

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → **New → Web Service → connect repo**
3. Set **Runtime: Docker**, **Dockerfile: `services/web/Dockerfile`**, **Context: `.`**
4. Add environment variables from your `.env.cloud` file (see table below)
5. Deploy — your app is live at `https://your-app.onrender.com`

Set up **UptimeRobot** (free) to ping `https://your-app.onrender.com/healthz` every 5 minutes so Render never spins down the free instance.

---

## Environment variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | Neon connection string (`postgresql+asyncpg://...`) |
| `DATABASE_SSL` | `true` for Neon |
| `REDIS_URL` | Upstash Redis URL (`rediss://...`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Aiven host:port (SASL port, e.g. `...aivencloud.com:26885`) |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` |
| `KAFKA_SASL_USERNAME` | Aiven username (usually `avnadmin`) |
| `KAFKA_SASL_PASSWORD` | Aiven password |
| `KAFKA_SSL_CA_CERT` | Aiven CA cert, base64-encoded (`base64 -w 0 ca.pem`) |
| `JWT_SECRET` | Random secret (`python3 -c "import secrets; print(secrets.token_hex(32))"`) |
| `JWT_EXPIRY_HOURS` | `720` (30 days) |
| `GROQ_API_KEY` | Groq API key |
| `GROQ_MODEL` | `llama-3.1-8b-instant` |
| `INGESTION_INTERVAL_SECONDS` | `120` (2 minutes) |
| `MENTION_RETENTION_DAYS` | `30` |

---

## Project structure

```
pulsestream/
├── frontend/
│   ├── index.html          # SPA shell
│   └── app.js              # All UI logic (~1200 lines, no framework)
├── services/
│   ├── web/
│   │   ├── main.py         # FastAPI app + background worker launcher
│   │   ├── ws_handler.py   # WebSocket connection manager
│   │   ├── routes/
│   │   │   ├── auth.py     # register / login / heartbeat / me
│   │   │   ├── topics.py   # CRUD + trend + digest endpoints
│   │   │   └── mentions.py # paginated mentions list
│   │   └── Dockerfile
│   ├── scheduler/
│   │   └── main.py         # APScheduler + Kafka fan-out
│   ├── ingester/
│   │   ├── main.py         # Kafka consumer + dedup logic
│   │   └── sources/
│   │       ├── hackernews.py
│   │       └── reddit.py
│   └── analyzer/
│       └── main.py         # Groq batch analysis + Postgres write
├── shared/
│   ├── models.py           # SQLAlchemy ORM (User, Topic, Mention, TopicDigest)
│   ├── schemas.py          # Pydantic request/response models
│   ├── config.py           # Settings loaded from env
│   ├── db.py               # Async SQLAlchemy engine
│   ├── kafka_client.py     # aiokafka producer/consumer helpers
│   └── redis_client.py     # aioredis client
├── docker-compose.yml      # Local dev (includes Kafka + Postgres + Redis)
├── docker-compose.cloud.yml# Cloud dev (external managed services)
└── render.yaml             # Render deployment config
```
