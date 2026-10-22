"""Small stand-ins for aiokafka objects, so consumer loops run without a broker."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class FakeMsg:
    """The fields of aiokafka's ConsumerRecord that the code reads."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Optional[bytes]


def make_msgs(topic: str, values: list[Any]) -> list[FakeMsg]:
    """Turn dicts into JSON messages; pass bytes through unchanged (for bad input)."""
    msgs = []
    for offset, value in enumerate(values):
        raw = value if isinstance(value, (bytes, type(None))) else json.dumps(value).encode()
        msgs.append(FakeMsg(topic, 0, offset, b"k", raw))
    return msgs


class FakeConsumer:
    """
    Hands out messages like aiokafka: the position moves forward as soon as a
    message is handed over. `commit()` records the position it would commit.
    """

    def __init__(self, msgs: list[FakeMsg], log: list, on_exhausted: Optional[Callable[[], None]] = None):
        self._msgs = list(msgs)
        self.log = log
        self.position = 0
        self._on_exhausted = on_exhausted
        self.kwargs: dict = {}

    def _next(self) -> FakeMsg:
        msg = self._msgs.pop(0)
        self.position = msg.offset + 1
        return msg

    async def getone(self) -> FakeMsg:
        if self._msgs:
            return self._next()
        if self._on_exhausted:
            self._on_exhausted()
        await asyncio.sleep(3600)  # nothing more; the caller's timeout fires
        raise AssertionError("unreachable")

    def __aiter__(self):
        return self

    async def __anext__(self) -> FakeMsg:
        if self._msgs:
            return self._next()
        raise StopAsyncIteration

    async def commit(self) -> None:
        self.log.append(("commit", self.position))


def fake_make_consumer(consumer: FakeConsumer):
    """Replacement for shared.kafka_client.make_consumer that yields `consumer`."""

    @asynccontextmanager
    async def _make(*topics, **kwargs):
        consumer.kwargs = kwargs
        yield consumer

    return _make
