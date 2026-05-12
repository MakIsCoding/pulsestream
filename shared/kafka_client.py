"""
Async Kafka client wrappers — producer and consumer.

Kafka is the event backbone for PulseStream. Services publish events to
topics; other services subscribe and process them.

Topics we use:
- mentions.raw: ingester publishes new items here, analyzer consumes
- mentions.analyzed: analyzer publishes results, websocket consumes
- topics.created: web publishes when user creates a topic, scheduler consumes

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

import base64
import json
import ssl
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.config import settings


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
            acks=1,
            linger_ms=5,
            compression_type="gzip",
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
    producer = await get_producer()
    payload = json.dumps(event).encode("utf-8")
    key_bytes = key.encode("utf-8") if key else None
    await producer.send_and_wait(topic, value=payload, key=key_bytes)


@asynccontextmanager
async def make_consumer(
    *topics: str,
    group_id: str,
    auto_offset_reset: str = "latest",
) -> AsyncIterator[AIOKafkaConsumer]:
    """
    Async context manager for a Kafka consumer.

    Each consumer belongs to a `group_id`. Members of the same group
    share the load (each partition goes to one consumer in the group).
    Different groups each get their own copy of every message.

    auto_offset_reset:
    - "latest" (default): start from new messages only on first run
    - "earliest": replay from the beginning (useful for analytics)

    Usage:
        async with make_consumer("mentions.raw", group_id="analyzer") as c:
            async for msg in c:
                event = json.loads(msg.value)
                ...
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        **_kafka_security_kwargs(),
    )
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


def deserialize_event(raw: Any) -> dict:
    """
    Decode a Kafka message value into a Python dict.

    aiokafka gives raw bytes; we encoded as JSON in `publish_event`,
    so we just JSON-decode here.
    """
    return json.loads(raw.decode("utf-8"))