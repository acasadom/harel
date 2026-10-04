"""A publish racing the delete of a group found drained must not leave its message without a
group — unclaimable for ever. Mongo has no row lock: the transport re-checks after the delete,
and these tests put a publish exactly in that window (mongomock; the async transport shares the
code path). Postgres locks the group's row instead: test/integration/test_transport_race.py."""

import pytest

from harel import Event


class _PublishInTheWindow:
    """A messages collection whose `find_one` runs `publish` once, right after answering — so
    the publish lands between the caller's check and what it does next."""

    def __init__(self, real, publish) -> None:
        self._real, self._publish = real, publish

    def find_one(self, *args, **kwargs):
        found = self._real.find_one(*args, **kwargs)
        if self._publish is not None:
            publish, self._publish = self._publish, None
            publish()
        return found

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.fixture
def transports():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.transport import MongoTransport

    client = mongomock.MongoClient()
    return MongoTransport(client), MongoTransport(client)


def test_a_publish_between_acks_check_and_delete_keeps_its_group(transports):
    worker, publisher = transports
    worker.publish("G", Event(kind="first"))
    lease = worker.claim("w", 30)
    worker._msgs = _PublishInTheWindow(worker._msgs, lambda: publisher.publish("G", Event(kind="second")))

    worker.ack(lease)  # finds G drained, then the publish lands, then G's lock is deleted

    nxt = publisher.claim("w", 30)
    assert nxt is not None and nxt.event.kind == "second"


def test_a_publish_while_claim_drops_an_empty_group_keeps_it(transports):
    worker, publisher = transports
    worker.publish("G", Event(kind="lost"))
    worker._msgs.delete_many({"group_id": "G"})  # a group left with no messages
    worker._msgs = _PublishInTheWindow(worker._msgs, lambda: publisher.publish("G", Event(kind="new")))

    lease = worker.claim("w", 30)  # leases G, finds no head, a publish lands, G is dropped

    assert lease is not None and lease.event.kind == "new"
