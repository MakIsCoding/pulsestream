"""
Delivery-guarantee tests for the Kafka consumers.

They check the behaviour the pipeline promises:
- offsets are committed only after the work is done (at-least-once);
- a failure part-way through a batch commits nothing, so it's re-delivered;
- messages that can never be processed go to the dead-letter topic instead
  of crashing the loop;
- crashed worker loops are restarted, and the scheduler's timer survives a
  crash of its topic consumer.

Run from the repo root:  pytest -q
"""

import asyncio
import uuid

import pytest
from sqlalchemy.exc import IntegrityError

import services.analyzer.main as analyzer
import services.ingester.main as ingester
import services.scheduler.main as scheduler_mod
import shared.supervisor as supervisor
from fakes import FakeConsumer, fake_make_consumer, make_msgs

TOPIC_ID = str(uuid.uuid4())
USER_ID = str(uuid.uuid4())


def mention(i: int) -> dict:
    return {"source": "reddit", "external_id": f"post{i}", "topic_id": TOPIC_ID, "title": f"t{i}"}


@pytest.fixture
def analyzer_env(monkeypatch):
    """Run the analyzer loop against a fake consumer; record persists, DLQ sends and commits."""
    log: list = []
    analyzer._stop_event.clear()
    monkeypatch.setattr(analyzer, "BATCH_FLUSH_SECONDS", 0.05)

    async def fake_analyze(events):
        return [None] * len(events)

    async def fake_persist(raw_event, analysis):
        log.append(("persist", raw_event["external_id"]))

    async def fake_dlq(msg, reason):
        log.append(("dlq", msg.offset))
        return True

    monkeypatch.setattr(analyzer, "_analyze_batch", fake_analyze)
    monkeypatch.setattr(analyzer, "_persist_and_emit", fake_persist)
    monkeypatch.setattr(analyzer, "send_to_dead_letter", fake_dlq)

    def run(values):
        consumer = FakeConsumer(make_msgs("mentions.raw", values), log, on_exhausted=analyzer._stop_event.set)
        monkeypatch.setattr(analyzer, "make_consumer", fake_make_consumer(consumer))
        asyncio.run(analyzer._consume_mentions())
        return consumer

    yield log, run, monkeypatch
    analyzer._stop_event.clear()


# ── Analyzer: at-least-once ───────────────────────────────────────────────

def test_analyzer_uses_manual_commit(analyzer_env):
    log, run, _ = analyzer_env
    consumer = run([mention(1)])
    assert consumer.kwargs["enable_auto_commit"] is False


def test_analyzer_commits_only_after_whole_batch_is_stored(analyzer_env):
    log, run, _ = analyzer_env
    run([mention(i) for i in range(5)])
    assert log == [("persist", f"post{i}") for i in range(5)] + [("commit", 5)]


def test_analyzer_commits_nothing_when_database_is_down(analyzer_env):
    log, run, monkeypatch = analyzer_env

    async def db_down(raw_event, analysis):
        raise ConnectionError("postgres unreachable")

    monkeypatch.setattr(analyzer, "_persist_and_emit", db_down)
    with pytest.raises(ConnectionError):
        run([mention(i) for i in range(3)])
    assert not [entry for entry in log if entry[0] == "commit"], "a failed batch must not be committed"


# ── Analyzer: dead-letter topic ───────────────────────────────────────────

def test_malformed_message_goes_to_dlq_and_loop_continues(analyzer_env):
    log, run, _ = analyzer_env
    run([b"{not json", mention(1)])
    # Bad message parked and committed past straight away (nothing was pending),
    # then the good one is stored and committed.
    assert log == [("dlq", 0), ("commit", 1), ("persist", "post1"), ("commit", 2)]


def test_message_missing_fields_goes_to_dlq(analyzer_env):
    log, run, _ = analyzer_env
    run([{"source": "reddit", "topic_id": TOPIC_ID}])  # no external_id
    assert log == [("dlq", 0), ("commit", 1)]


def test_row_rejected_by_database_goes_to_dlq_and_batch_still_commits(analyzer_env):
    log, run, monkeypatch = analyzer_env

    async def persist(raw_event, analysis):
        if raw_event["external_id"] == "post1":
            raise IntegrityError("INSERT", {}, Exception("foreign key violation"))
        log.append(("persist", raw_event["external_id"]))

    monkeypatch.setattr(analyzer, "_persist_and_emit", persist)
    run([mention(0), mention(1), mention(2)])
    assert log == [("persist", "post0"), ("dlq", 1), ("persist", "post2"), ("commit", 3)]


# ── Ingester ──────────────────────────────────────────────────────────────

def test_ingester_commits_each_job_and_dead_letters_bad_ones(monkeypatch):
    log: list = []
    ingester._stop_event.clear()
    handled: list = []

    async def handle(job):
        if job["topic_id"] == "boom":
            raise RuntimeError("source API down")
        handled.append(job["topic_id"])

    async def fake_dlq(msg, reason):
        log.append(("dlq", msg.offset))
        return True

    consumer = FakeConsumer(
        make_msgs("ingestion.jobs", [b"\xff\xfe", {"topic_id": "ok"}, {"topic_id": "boom"}]), log
    )
    monkeypatch.setattr(ingester, "make_consumer", fake_make_consumer(consumer))
    monkeypatch.setattr(ingester, "_handle_job", handle)
    monkeypatch.setattr(ingester, "send_to_dead_letter", fake_dlq)

    asyncio.run(ingester._consume_jobs())

    assert consumer.kwargs["enable_auto_commit"] is False
    assert handled == ["ok"]
    assert log == [("dlq", 0), ("commit", 1), ("commit", 2), ("commit", 3)]


# ── Scheduler's topic consumer ────────────────────────────────────────────

def test_scheduler_consumer_survives_bad_event_and_failed_publish(monkeypatch):
    log: list = []
    sched = scheduler_mod.Scheduler()
    event = {"topic_id": TOPIC_ID, "user_id": USER_ID, "name": "Rust", "keywords": ["cargo"]}

    async def failing_enqueue(evt):
        raise ConnectionError("kafka unreachable")

    async def fake_dlq(msg, reason):
        log.append(("dlq", msg.offset))
        return True

    consumer = FakeConsumer(make_msgs("topics.created", [{"name": "missing ids"}, event]), log)
    monkeypatch.setattr(scheduler_mod, "make_consumer", fake_make_consumer(consumer))
    monkeypatch.setattr(scheduler_mod, "send_to_dead_letter", fake_dlq)
    monkeypatch.setattr(sched, "_enqueue_for_topic", failing_enqueue)

    asyncio.run(sched._consume_topic_events())  # must not raise

    assert consumer.kwargs["enable_auto_commit"] is False
    assert log == [("dlq", 0), ("commit", 1), ("commit", 2)]


# ── Dead-letter helper ────────────────────────────────────────────────────

def test_dead_letter_record_keeps_origin_and_reason(monkeypatch):
    import shared.kafka_client as kc

    sent: list = []

    async def fake_publish(topic, event, key=None):
        sent.append((topic, event, key))

    monkeypatch.setattr(kc, "publish_event", fake_publish)
    msg = make_msgs("mentions.raw", [b"{not json"])[0]

    assert asyncio.run(kc.send_to_dead_letter(msg, "undecodable message")) is True
    topic, record, key = sent[0]
    assert topic == "pulsestream.dlq" and key == "mentions.raw"
    assert record["source_topic"] == "mentions.raw" and record["offset"] == 0
    assert record["value"] == "{not json" and record["error"] == "undecodable message"


def test_dead_letter_failure_is_reported_not_raised(monkeypatch):
    import shared.kafka_client as kc

    async def topic_missing(topic, event, key=None):
        raise RuntimeError("UnknownTopicOrPartition")

    monkeypatch.setattr(kc, "publish_event", topic_missing)
    msg = make_msgs("mentions.raw", [b"x"])[0]
    assert asyncio.run(kc.send_to_dead_letter(msg, "bad")) is False


def test_healthz_reports_worker_restarts():
    from fastapi.testclient import TestClient
    import services.web.main as web_main

    supervisor.restart_counts["analyzer"] = 3
    try:
        body = TestClient(web_main.app).get("/healthz").json()  # no lifespan: no workers start
    finally:
        supervisor.restart_counts.pop("analyzer", None)
    assert body["status"] == "ok" and body["worker_restarts"]["analyzer"] == 3


# ── Supervision ───────────────────────────────────────────────────────────

def test_supervise_restarts_a_crashed_worker(monkeypatch):
    monkeypatch.setattr(supervisor, "INITIAL_BACKOFF_SECONDS", 0.0)
    calls = {"n": 0}

    async def worker():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("kafka blip")
        # third run: a normal stop

    asyncio.run(supervisor.supervise("test-worker", worker))
    assert calls["n"] == 3
    assert supervisor.restart_counts["test-worker"] == 2


def test_supervise_passes_cancellation_through():
    async def main():
        async def forever():
            await asyncio.sleep(3600)

        task = asyncio.create_task(supervisor.supervise("test-cancel", forever))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())


def test_scheduler_timer_keeps_running_when_topic_consumer_crashes(monkeypatch):
    """Before this change, a crash here also shut down the 2-minute timer."""
    import services.web.main as web_main

    monkeypatch.setattr(supervisor, "INITIAL_BACKOFF_SECONDS", 0.0)
    created: list = []

    class TestScheduler(scheduler_mod.Scheduler):
        def __init__(self):
            super().__init__()
            self.consumer_runs = 0
            created.append(self)

        async def _fan_out_ingestion_jobs(self):
            pass

        async def _cleanup_old_mentions(self):
            pass

        async def _generate_digests(self):
            pass

        async def _keep_alive_ping(self):
            pass

        async def _consume_topic_events(self):
            self.consumer_runs += 1
            if self.consumer_runs == 1:
                raise ConnectionError("kafka unreachable")
            await asyncio.sleep(3600)

    monkeypatch.setattr(scheduler_mod, "Scheduler", TestScheduler)

    async def main():
        task = asyncio.create_task(web_main._run_scheduler())
        await asyncio.sleep(0.1)
        sched = created[0]
        assert sched.consumer_runs == 2, "consumer should have been restarted"
        assert sched._aps.running, "timer must survive the consumer crash"
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not sched._aps.running, "timer stops only when the app shuts down"

    asyncio.run(main())
