"""
Re-analysis queries against a real Postgres (optional).

Skipped unless TEST_DATABASE_URL points at a throwaway database, e.g.
  TEST_DATABASE_URL=postgresql+asyncpg://postgres@127.0.0.1:5432/ps_test pytest -q
The tables are created and dropped by the test, so never point it at real data.
"""

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import services.analyzer.main as analyzer
from shared.db import Base
from shared.models import Mention, Topic, User


DB_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DB_URL, reason="TEST_DATABASE_URL not set")

GOOD = {"sentiment_score": -0.4, "sentiment_label": "negative", "entities": ["neet"], "summary": "Exam delayed."}


def test_load_and_store_reanalyses_against_postgres(monkeypatch):
    async def scenario():
        engine = create_async_engine(DB_URL)
        sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        monkeypatch.setattr(analyzer, "AsyncSessionLocal", sessions)
        analyzer._reanalyze_skipped.clear()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        try:
            now = datetime.now(timezone.utc)
            user = User(email="t@example.com", password_hash="x")
            topic = Topic(user=user, name="NEET", keywords=["neet"])
            fresh, older, done, stale, skipped = (uuid.uuid4() for _ in range(5))
            rows = [
                Mention(id=fresh, topic=topic, source="reddit", external_id="a", title="fresh",
                        ingested_at=now - timedelta(minutes=1)),
                Mention(id=older, topic=topic, source="reddit", external_id="b", title="older",
                        ingested_at=now - timedelta(days=2)),
                Mention(id=done, topic=topic, source="reddit", external_id="c", title="done",
                        sentiment_label="positive", sentiment_score=0.9, analyzed_at=now,
                        ingested_at=now),
                Mention(id=stale, topic=topic, source="reddit", external_id="d", title="stale",
                        ingested_at=now - timedelta(days=analyzer.REANALYZE_WINDOW_DAYS + 1)),
                Mention(id=skipped, topic=topic, source="reddit", external_id="e", title="skipped",
                        ingested_at=now),
            ]
            async with sessions() as session:
                session.add_all([user, topic, *rows])
                await session.commit()
            analyzer._reanalyze_skipped.add(skipped)

            # Newest first; already-analysed, too-old and skipped rows are left out.
            loaded = await analyzer._load_unanalyzed(5)
            assert [r["id"] for r in loaded] == [fresh, older]
            assert loaded[0]["user_id"] == user.id and loaded[0]["title"] == "fresh"

            # Only rows that still have no analysis are written.
            updated = await analyzer._store_reanalyses([(fresh, GOOD), (done, GOOD)])
            assert updated == {fresh}
            assert await analyzer._store_reanalyses([(fresh, GOOD)]) == set()  # repeat is a no-op

            async with sessions() as session:
                by_id = {m.id: m for m in (await session.execute(select(Mention))).scalars()}
            assert by_id[fresh].sentiment_label == "negative" and by_id[fresh].entities == ["neet"]
            assert by_id[fresh].analyzed_at is not None
            assert by_id[done].sentiment_label == "positive"  # existing analysis untouched
            assert [r["id"] for r in await analyzer._load_unanalyzed(5)] == [older]
        finally:
            analyzer._reanalyze_skipped.clear()
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
            await engine.dispose()

    asyncio.run(scenario())
