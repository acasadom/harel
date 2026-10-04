"""A publish racing an ack on Postgres: the ack finds the group drained while a publish into it
has inserted its message but not committed. Both lock the group's row, so the ack waits for the
publish, then sees its message and keeps the group. Without the lock, the ack deleted the group
and the message was left without one — unclaimable for ever. (Mongo's equivalent, a re-check:
test/unit/engine/test_transport_race.py.)"""

import os
import threading
import time
import uuid

import pytest

from harel import Event

pytestmark = pytest.mark.stack


class _CommitWhenTold:
    """A connection whose `commit` waits for the test — so a publish stays open, its locks held."""

    def __init__(self, conn) -> None:
        self.conn = conn

    def cursor(self):
        return self.conn.cursor()

    def commit(self) -> None:
        pass


def test_an_ack_racing_a_publish_into_its_group_keeps_the_message():
    if os.environ.get("HAREL_TRANSPORT_BACKEND") != "postgres" or not os.environ.get("HAREL_POSTGRES_DSN"):
        pytest.skip("not the postgres stack")
    import psycopg

    from harel.engine.transport import PostgresTransport

    dsn = os.environ["HAREL_POSTGRES_DSN"]
    prefix = f"r{uuid.uuid4().hex[:8]}"
    worker = PostgresTransport(psycopg.connect(dsn), prefix=prefix)
    held = _CommitWhenTold(psycopg.connect(dsn))
    publisher = PostgresTransport(held, prefix=prefix, create_schema=False)

    worker.publish("G", Event(kind="first"))
    lease = worker.claim("w", 30)
    publisher.publish("G", Event(kind="second"))  # inserted, not committed

    acked = threading.Event()
    ack = threading.Thread(target=lambda: (worker.ack(lease), acked.set()))
    ack.start()
    time.sleep(0.5)
    waited = not acked.is_set()  # the ack waits on the group's row the publish holds
    held.conn.commit()
    ack.join(timeout=10)

    assert waited and acked.is_set()
    nxt = worker.claim("w", 30)
    assert nxt is not None and nxt.event.kind == "second"
