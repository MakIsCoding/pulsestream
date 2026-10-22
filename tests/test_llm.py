"""
Tests for the LLM settings, LLM health reporting and the re-analysis loop.

They check that:
- the model is one Groq still serves, with the reasoning settings a
  reasoning model needs, and that a retired model name can't break analysis;
- a failing LLM shows up on /healthz (without leaking account details);
- mentions stored without an analysis are analysed again, pushed live, and
  never overwrite an existing analysis; Groq outages make the loop back off
  instead of using up retries; a post that can never be analysed is skipped.

Nothing here calls Groq, Kafka or Postgres.

Run from the repo root:  pytest -q
"""

import asyncio
import json
import uuid
from types import SimpleNamespace

import groq
import httpx
import pytest

import services.analyzer.main as analyzer
import services.scheduler.main as scheduler_mod
import shared.llm as llm
from shared.config import Settings, settings


GOOD = {"sentiment_score": 0.6, "sentiment_label": "positive", "entities": ["groq"], "summary": "Good news."}


@pytest.fixture(autouse=True)
def fresh_state(monkeypatch):
    """Each test starts with clean LLM health and re-analysis bookkeeping."""
    llm.llm_health.__init__()
    analyzer._reanalyze_attempts.clear()
    analyzer._reanalyze_skipped.clear()
    analyzer._stop_event.clear()
    monkeypatch.setattr(settings, "groq_model", "openai/gpt-oss-20b")
    yield
    llm.llm_health.__init__()
    analyzer._reanalyze_attempts.clear()
    analyzer._reanalyze_skipped.clear()


def groq_error(cls, status: int, code: str, message: str = "details for org_secret123"):
    request = httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
    response = httpx.Response(status, request=request)
    return cls(message, response=response, body={"error": {"message": message, "code": code}})


class FakeGroq:
    """Records the arguments of each chat.completions.create call."""

    def __init__(self, content: str):
        self.calls: list[dict] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))
        self._content = content

    async def _create(self, **kwargs):
        self.calls.append(kwargs)
        message = SimpleNamespace(content=self._content)
        return SimpleNamespace(choices=[SimpleNamespace(message=message)])


# ── Model and reasoning settings ──────────────────────────────────────────

def test_default_model_is_one_groq_still_serves():
    assert Settings.model_fields["groq_model"].default == "openai/gpt-oss-20b"


def test_reasoning_model_gets_low_effort_and_room_to_answer():
    assert llm.completion_options(2048) == {
        "model": "openai/gpt-oss-20b",
        "max_completion_tokens": 2048,
        "reasoning_effort": "low",
        "include_reasoning": False,
    }


def test_non_reasoning_model_gets_no_reasoning_parameters(monkeypatch):
    monkeypatch.setattr(settings, "groq_model", "llama-3.3-70b-versatile")
    assert llm.completion_options(512) == {
        "model": "llama-3.3-70b-versatile",
        "max_completion_tokens": 512,
    }


def test_retired_model_in_env_is_replaced(monkeypatch):
    monkeypatch.setattr(settings, "groq_model", "llama-3.1-8b-instant")
    assert llm.groq_model() == "openai/gpt-oss-20b"
    snap = llm.llm_health.snapshot()
    assert snap["model"] == "openai/gpt-oss-20b"
    assert snap["configured_model"] == "llama-3.1-8b-instant"


def test_analyzer_call_uses_reasoning_settings_and_json_mode(monkeypatch):
    fake = FakeGroq(json.dumps({"results": [GOOD, None]}))
    monkeypatch.setattr(analyzer, "_get_groq_client", lambda: fake)

    results = asyncio.run(analyzer._analyze_batch([{"title": "a"}, {"title": "b"}]))

    assert results == [GOOD, None]
    call = fake.calls[0]
    assert call["model"] == "openai/gpt-oss-20b"
    assert call["reasoning_effort"] == "low" and call["include_reasoning"] is False
    assert call["max_completion_tokens"] == analyzer.ANALYSIS_MAX_OUTPUT_TOKENS
    assert call["response_format"] == {"type": "json_object"}
    assert "max_tokens" not in call


def test_digest_call_leaves_room_for_reasoning(monkeypatch):
    fake = FakeGroq("People are upbeat.")
    monkeypatch.setattr(scheduler_mod, "_get_groq_client", lambda: fake)

    summary = asyncio.run(scheduler_mod._call_groq_for_digest("AI", ["- one", "- two"]))

    assert summary == "People are upbeat."
    call = fake.calls[0]
    assert call["max_completion_tokens"] == scheduler_mod.DIGEST_MAX_OUTPUT_TOKENS >= 1024
    assert call["reasoning_effort"] == "low"
    assert "max_tokens" not in call


# ── LLM health ────────────────────────────────────────────────────────────

def test_failing_llm_shows_on_healthz_without_account_details(monkeypatch):
    from fastapi.testclient import TestClient
    import services.web.main as web_main

    async def rate_limited(posts):
        raise groq_error(groq.RateLimitError, 429, "rate_limit_exceeded")

    monkeypatch.setattr(analyzer, "_analyze_batch", rate_limited)
    analyses, reachable = asyncio.run(analyzer._analyze_posts([{"title": "a"}]))

    assert analyses == [None] and reachable is False
    body = TestClient(web_main.app).get("/healthz").json()  # no lifespan: no workers start
    assert body["status"] == "ok"
    assert body["llm"]["consecutive_failures"] == 1
    assert body["llm"]["last_error"] == "RateLimitError (HTTP 429, rate_limit_exceeded)"
    assert "org_secret123" not in json.dumps(body)


def test_success_resets_the_failure_count(monkeypatch):
    llm.llm_health.record_failure("earlier failure")

    async def ok(posts):
        return [GOOD]

    monkeypatch.setattr(analyzer, "_analyze_batch", ok)
    asyncio.run(analyzer._analyze_posts([{"title": "a"}]))
    assert llm.llm_health.consecutive_failures == 0
    assert llm.llm_health.last_success_at is not None


def test_request_rejection_and_groq_problems_are_told_apart():
    assert llm.is_request_rejected(groq_error(groq.BadRequestError, 400, "json_validate_failed"))
    assert not llm.is_request_rejected(groq_error(groq.BadRequestError, 400, "model_decommissioned"))
    assert not llm.is_request_rejected(groq_error(groq.RateLimitError, 429, "rate_limit_exceeded"))
    assert not llm.is_request_rejected(groq_error(groq.InternalServerError, 503, "service_unavailable"))


# ── Re-analysis ───────────────────────────────────────────────────────────

def row(i: int) -> dict:
    return {
        "id": uuid.UUID(int=i), "topic_id": uuid.UUID(int=100), "user_id": uuid.UUID(int=200),
        "source": "google_news", "title": f"post {i}", "content": None, "url": f"https://x/{i}",
    }


@pytest.fixture
def reanalysis(monkeypatch):
    """Fake the database, Groq and Kafka around `_reanalyze_once`; record what happens."""
    log = {"stored": [], "pushed": [], "rows": [row(1), row(2)], "analyses": [GOOD, None]}

    async def load(limit):
        return [r for r in log["rows"] if r["id"] not in analyzer._reanalyze_skipped][:limit]

    async def analyze(posts):
        result = log["analyses"]
        if isinstance(result, Exception):
            raise result
        return result[: len(posts)]

    async def store(results):
        log["stored"].append([mention_id for mention_id, _ in results])
        return {mention_id for mention_id, _ in results} - log.get("already_done", set())

    async def publish(topic, event, key=None):
        if log.get("kafka_down"):
            raise OSError("kafka unreachable")
        log["pushed"].append(event)

    monkeypatch.setattr(analyzer, "_load_unanalyzed", load)
    monkeypatch.setattr(analyzer, "_analyze_batch", analyze)
    monkeypatch.setattr(analyzer, "_store_reanalyses", store)
    monkeypatch.setattr(analyzer, "publish_event", publish)
    return log


def test_reanalysis_stores_usable_results_and_pushes_them_live(reanalysis):
    assert asyncio.run(analyzer._reanalyze_once()) is True

    assert reanalysis["stored"] == [[uuid.UUID(int=1)]]
    [event] = reanalysis["pushed"]
    assert event["mention_id"] == str(uuid.UUID(int=1)) and event["reanalyzed"] is True
    assert event["user_id"] == str(uuid.UUID(int=200)) and event["sentiment_label"] == "positive"
    assert analyzer._reanalyze_attempts == {uuid.UUID(int=2): 1}
    assert llm.llm_health.reanalyzed_mentions == 1


def test_post_that_never_gets_an_analysis_is_skipped_after_three_tries(reanalysis):
    reanalysis["rows"], reanalysis["analyses"] = [row(2)], [None]
    for _ in range(analyzer.REANALYZE_MAX_ATTEMPTS):
        asyncio.run(analyzer._reanalyze_once())

    assert uuid.UUID(int=2) in analyzer._reanalyze_skipped
    assert reanalysis["stored"] == []


def test_groq_outage_backs_off_and_counts_nothing_against_the_posts(reanalysis):
    reanalysis["analyses"] = groq.APIConnectionError(request=httpx.Request("POST", "https://api.groq.com"))

    assert asyncio.run(analyzer._reanalyze_once()) is False
    assert analyzer._reanalyze_attempts == {} and reanalysis["stored"] == []


def test_rejected_request_counts_against_the_posts_but_does_not_back_off(reanalysis):
    reanalysis["analyses"] = groq_error(groq.BadRequestError, 400, "json_validate_failed")

    assert asyncio.run(analyzer._reanalyze_once()) is True
    assert analyzer._reanalyze_attempts == {uuid.UUID(int=1): 1, uuid.UUID(int=2): 1}


def test_only_rows_actually_updated_are_pushed(reanalysis):
    reanalysis["analyses"] = [GOOD, GOOD]
    reanalysis["already_done"] = {uuid.UUID(int=2)}  # analysed elsewhere in the meantime

    asyncio.run(analyzer._reanalyze_once())
    assert [e["mention_id"] for e in reanalysis["pushed"]] == [str(uuid.UUID(int=1))]


def test_failed_live_push_does_not_undo_the_update(reanalysis):
    reanalysis["kafka_down"] = True

    assert asyncio.run(analyzer._reanalyze_once()) is True
    assert reanalysis["stored"] == [[uuid.UUID(int=1)]]
    assert llm.llm_health.reanalyzed_mentions == 1


def test_nothing_to_do_makes_no_llm_call(reanalysis):
    reanalysis["rows"] = []
    reanalysis["analyses"] = AssertionError("Groq should not be called")
    assert asyncio.run(analyzer._reanalyze_once()) is True


def test_reanalysis_loop_backs_off_while_groq_fails(monkeypatch):
    outcomes = iter([False, False, True, False])
    delays: list[float] = []

    async def once():
        return next(outcomes)

    async def fake_sleep(seconds):
        delays.append(seconds)
        if len(delays) > 4:
            raise asyncio.CancelledError

    monkeypatch.setattr(analyzer, "_reanalyze_once", once)
    monkeypatch.setattr(analyzer.asyncio, "sleep", fake_sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(analyzer._reanalyze_loop())

    start, interval = analyzer.REANALYZE_START_DELAY_SECONDS, analyzer.REANALYZE_INTERVAL_SECONDS
    assert delays == [start, interval * 2, interval * 4, interval, interval * 2]
