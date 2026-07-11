"""SqsTransport rejects priority / min_priority instead of silently ignoring them.

SQS FIFO has no per-group priority and receive() can't filter by it, so honouring
`priority` / `min_priority` is impossible. Rather than a silent no-op (which makes a
priority-routed worker do nothing on SQS), publish/claim fail fast. The guards run
before any SQS call, so these tests need no AWS/LocalStack — just a stub client.
"""

import asyncio

import pytest

from harel.engine.aio_transport.sqs import AsyncSqsTransport
from harel.engine.transport.sqs import SqsTransport
from harel.spec.states import Event


class _FakeSqs:
    def __init__(self):
        self.sent = []

    def send_message(self, **kw):
        self.sent.append(kw)

    def receive_message(self, **kw):
        return {}


class _FakeAsyncSqs:
    async def send_message(self, **kw):
        pass

    async def receive_message(self, **kw):
        return {}


def _ev():
    return Event(kind="E")


def test_sqs_publish_rejects_priority():
    with pytest.raises(ValueError, match="message priority"):
        SqsTransport(_FakeSqs(), "q.fifo").publish("g", _ev(), priority=1)


def test_sqs_claim_rejects_min_priority():
    with pytest.raises(ValueError, match="min_priority"):
        SqsTransport(_FakeSqs(), "q.fifo").claim("w", visibility=30, min_priority=2)


def test_sqs_allows_the_defaults():
    fake = _FakeSqs()
    t = SqsTransport(fake, "q.fifo")
    t.publish("g", _ev())  # priority defaults to 0 -> no raise, message sent
    assert fake.sent[0]["MessageGroupId"] == "g"
    assert t.claim("w", visibility=30) is None  # min_priority defaults to 0 -> no raise


def test_async_sqs_rejects_priority_and_min_priority():
    async def go():
        with pytest.raises(ValueError, match="message priority"):
            await AsyncSqsTransport(_FakeAsyncSqs(), "q.fifo").publish("g", _ev(), priority=3)
        with pytest.raises(ValueError, match="min_priority"):
            await AsyncSqsTransport(_FakeAsyncSqs(), "q.fifo").claim("w", visibility=30, min_priority=1)

    asyncio.run(go())
