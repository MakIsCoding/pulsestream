"""
Async Kafka client wrappers — producer and consumer.

Kafka is the event backbone for PulseStream. Services publish events to
topics; other services subscribe and process them.

Topics we use:
- topics.created: web publishes when user creates a topic, scheduler consumes
- ingestion.jobs: scheduler publishes one job per (topic, source), ingester consumes
- mentions.raw: ingester publishes new items here, analyzer consumes
- mentions.analyzed: analyzer publishes results, websocket consumes
- pulsestream.dlq: dead-letter topic for messages that can never be processed

Delivery guarantees:
- The producer is idempotent (`enable_idempotence=True`, which requires
  acks="all"). The broker gives it a producer id and a sequence number per
  message, and drops a message it has already written, so a retry after a
  lost acknowledgement can't write a duplicate. It does NOT stop duplicates
  from a consumer re-processing a message and publishing it again (that's a
  new send); the consumers' idempotent writes handle those.
- The pipeline consumers (scheduler, ingester, analyzer) run with auto-commit
  OFF and call `commit_offsets()` only after a message's work is done. A crash
  or redeploy therefore re-delivers unfinished messages instead of skipping
  them (at-least-once). Every consumer is safe to repeat: the ingester dedups
  in Redis and the analyzer's insert is ON CONFLICT DO NOTHING.
- A message that can never be processed (malformed, or rejected by the
  database) is parked on the dead-letter topic with `send_to_dead_letter()`
  so it can't block or crash the consumer.

Usage (producer):
    from shared.kafka_client import get_producer
    producer = await get_producer()
    await producer.send_and_wait("mentions.raw", b'{"foo": "bar"}', key=b"hn-12345")

Usage (consumer):
    from shared.kafka_client import make_consumer

    async with make_consumer("mentions.raw", group_id="analyzer") as consumer:
        async for msg in consumer:
            print(msg.value)
"""

import asyncio
import base64
import json
import logging
import ssl
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

from shared.config import settings


logger = logging.getLogger("pulsestream.kafka")

# Longest a consumer waits to park a bad message before logging it and moving on.
DEAD_LETTER_TIMEOUT_SECONDS = 10.0

_producer: Optional[AIOKafkaProducer] = None


def _kafka_security_kwargs() -> dict:
    """Return SASL/SSL kwargs when cloud Kafka is configured.

    Local dev uses plain PLAINTEXT (no extra kwargs).
    Cloud (Aiven/Redpanda) uses SASL_SSL + SCRAM-SHA-256.
    Aiven requires its own CA cert loaded via KAFKA_SSL_CA_CERT (base64-encoded ca.pem).
    """
    proto = settings.kafka_security_protocol.upper()
    if proto == "PLAINTEXT":
        return {}
    ssl_ctx = ssl.create_default_context()
    # Aiven (and some other providers) use a custom CA not in the system trust store.
    # Set KAFKA_SSL_CA_CERT to the base64-encoded contents of the ca.pem file.
    if settings.kafka_ssl_ca_cert:
        ca_pem = base64.b64decode(settings.kafka_ssl_ca_cert).decode("utf-8")
        ssl_ctx.load_verify_locations(cadata=ca_pem)
    return {
        "security_protocol": proto,
        "sasl_mechanism": settings.kafka_sasl_mechanism,
        "sasl_plain_username": settings.kafka_sasl_username,
        "sasl_plain_password": settings.kafka_sasl_password,
        "ssl_context": ssl_ctx,
    }


# Idempotent producer: no duplicates from the producer's own retries.
# aiokafka requires acks="all" with it (acks=1 alongside it raises an error).
# acks="all" waits for every in-sync replica. On Aiven's free tier there is one
# broker and one replica, so that's the same single broker acks=1 waited for:
# it adds no durability there, only on a cluster with replicas.
PRODUCER_SETTINGS = {
    "enable_idempotence": True,
    "acks": "all",
    "linger_ms": 5,
    "compression_type": "gzip",
}


class PublishHealth:
    """Outcome of recent publishes, reported on /healthz."""

    def __init__(self) -> None:
        self.last_ok_at: datetime | None = None
        self.last_failure_at: datetime | None = None
        self.last_error: str | None = None
        self.consecutive_failures = 0

    def record_success(self) -> None:
        self.last_ok_at = datetime.now(timezone.utc)
        self.consecutive_failures = 0

    def record_failure(self, exc: BaseException) -> None:
        self.last_failure_at = datetime.now(timezone.utc)
        self.last_error = exc.__class__.__name__
        self.consecutive_failures += 1

    def snapshot(self) -> dict[str, Any]:
        def iso(value: datetime | None) -> str | None:
            return value.isoformat() if value else None

        return {
            "idempotent_producer": PRODUCER_SETTINGS["enable_idempotence"],
            "acks": PRODUCER_SETTINGS["acks"],
            "last_publish_ok_at": iso(self.last_ok_at),
            "last_publish_failure_at": iso(self.last_failure_at),
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
        }


publish_health = PublishHealth()


async def get_producer() -> AIOKafkaProducer:
    """
    Returns the shared async Kafka producer.

    Lazily started on first call. One producer per process is the standard
    pattern — producers are thread-safe and designed for sharing.
    """
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            **PRODUCER_SETTINGS,
            **_kafka_security_kwargs(),
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    """Stops the shared producer. Call on graceful shutdown."""
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


async def publish_event(topic: str, event: dict, key: Optional[str] = None) -> None:
    """
    Convenience helper to publish a JSON-serializable event.

    The key (if provided) ensures all events with the same key go to the
    same partition, which preserves ordering for that key. E.g., all events
    for the same `topic_id` should use the topic_id as the Kafka key so
    they're processed in order.
    """
    payload = json.dumps(event).encode("utf-8")
    key_bytes = key.encode("utf-8") if key else None
    try:
        producer = await get_producer()
        await producer.send_and_wait(topic, value=payload, key=key_bytes)
    except Exception as exc:
        publish_health.record_failure(exc)
        raise
    publish_health.record_success()


@asynccontextmanager
async def make_consumer(
    *topics: str,
    group_id: str,
    auto_offset_reset: str = "latest",
    enable_auto_commit: bool = True,
) -> AsyncIterator[AIOKafkaConsumer]:
    """
    Async context manager for a Kafka consumer.

    Each consumer belongs to a `group_id`. Members of the same group
    share the load (each partition goes to one consumer in the group).
    Different groups each get their own copy of every message.

    auto_offset_reset:
    - "latest" (default): start from new messages only on first run
    - "earliest": replay from the beginning (useful for analytics)

    enable_auto_commit:
    - True (default): aiokafka commits the consumed position on a 5 s timer,
      whether or not the work for those messages has finished. A crash can
      therefore skip messages. Only acceptable for best-effort consumers
      (the WebSocket push).
    - False: nothing is committed until the caller runs `commit_offsets()`
      after finishing the work. Used by every pipeline consumer.

    Usage:
        async with make_consumer("mentions.raw", group_id="analyzer",
                                 enable_auto_commit=False) as c:
            async for msg in c:
                event = json.loads(msg.value)
                ...  # do the work
                await commit_offsets(c)
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        auto_commit_interval_ms=5000,
        **_kafka_security_kwargs(),
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


async def commit_offsets(consumer: AIOKafkaConsumer) -> None:
    """
    Commit the consumer's current position. Call it only after the work for
    every message handed out so far is finished.

    A failed commit (broker unreachable, or the group rebalanced) is logged
    rather than raised: the uncommitted messages are simply delivered again,
    and every consumer in this project is safe to repeat.
    """
    try:
        await consumer.commit()
    except KafkaError:
        logger.warning(
            "Offset commit failed; messages since the last commit will be re-delivered",
            exc_info=True,
        )


async def send_to_dead_letter(msg: Any, reason: str) -> bool:
    """
    Park a message that can never be processed on the dead-letter topic,
    together with where it came from and why it failed, so the consumer can
    move on instead of crashing or retrying it forever.

    Returns False if even that fails (for example, the dead-letter topic
    hasn't been created). The caller still moves on; the full record is in
    the error log so nothing is lost silently.
    """
    raw_value = msg.value
    if isinstance(raw_value, (bytes, bytearray)):
        value_text = raw_value.decode("utf-8", errors="replace")
    else:
        value_text = repr(raw_value)
    raw_key = msg.key
    key_text = raw_key.decode("utf-8", errors="replace") if raw_key else None

    record = {
        "source_topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "key": key_text,
        "value": value_text,
        "error": reason,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        # Bounded wait: if the dead-letter topic is missing, the producer would
        # otherwise wait up to its 40 s request timeout and stall the consumer.
        await asyncio.wait_for(
            publish_event(settings.kafka_topic_dlq, record, key=msg.topic),
            timeout=DEAD_LETTER_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.exception(
            "Could not write to dead-letter topic %r; dropping message: %s",
            settings.kafka_topic_dlq,
            record,
        )
        return False

    logger.warning(
        "Sent %s[%s]@%s to dead-letter topic %r: %s",
        msg.topic, msg.partition, msg.offset, settings.kafka_topic_dlq, reason,
    )
    return True


def deserialize_event(raw: Any) -> dict:
    """
    Decode a Kafka message value into a Python dict.

    aiokafka gives raw bytes; we encoded as JSON in `publish_event`,
    so we just JSON-decode here.
    """
    return json.loads(raw.decode("utf-8"))