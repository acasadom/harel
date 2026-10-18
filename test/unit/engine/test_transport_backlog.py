"""The backlog contract (`harel.testing.assert_backlog_contract`) over every in-process
transport, sync and async (no Docker). The networked ones run it in the stack
(test/integration/test_transport_backlog.py)."""

import itertools

import pytest

from harel.engine.aio_transport import AsyncInMemoryTransport, AsyncSqliteTransport
from harel.engine.transport import InMemoryTransport, SqliteTransport
from harel.spec.states import Event
from harel.testing import assert_async_backlog_contract, assert_backlog_contract

_n = itertools.count()


def test_in_memory():
    assert_backlog_contract(lambda clock: InMemoryTransport(clock=clock))


def test_sqlite(tmp_path):
    assert_backlog_contract(lambda clock: SqliteTransport(tmp_path / f"q{next(_n)}.db", clock=clock))


def test_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.transport import LibsqlTransport

    assert_backlog_contract(lambda clock: LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock))


def test_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport

    assert_backlog_contract(lambda clock: RedisTransport(fakeredis.FakeStrictRedis(), clock=clock))


def test_mongo():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.transport import MongoTransport

    assert_backlog_contract(lambda clock: MongoTransport(mongomock.MongoClient(), clock=clock))


async def test_async_in_memory():
    await assert_async_backlog_contract(lambda clock: AsyncInMemoryTransport(clock=clock))


async def test_async_sqlite(tmp_path):
    await assert_async_backlog_contract(
        lambda clock: AsyncSqliteTransport.create(str(tmp_path / f"q{next(_n)}.db"), clock=clock)
    )


async def test_async_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.aio_transport import AsyncLibsqlTransport
    from harel.engine.transport import LibsqlTransport

    await assert_async_backlog_contract(
        lambda clock: AsyncLibsqlTransport(LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock))
    )


async def test_async_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.aio_transport import AsyncRedisTransport

    await assert_async_backlog_contract(
        lambda clock: AsyncRedisTransport(fakeredis.aioredis.FakeRedis(), clock=clock)
    )


def test_a_redis_group_from_before_the_publish_times_has_an_unknown_wait():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport

    client = fakeredis.FakeStrictRedis()
    client.rpush("harel:q:old", Event(kind="E").model_dump_json())  # queued by an earlier harel
    client.hset("harel:prio", "old", 0)
    client.zadd("harel:ready:0", {"old": 0})
    t = RedisTransport(client)
    t.publish("old", Event(kind="E"), routing_key="order")  # a new message into that group
    [row] = t.backlog().machines
    assert (row.messages, row.oldest_published_at) == (2, None)  # the head's time is unknown
    lease = t.claim("w", 30)
    t.ack(lease)  # the old head goes: the timestamps line up with the queue again
    [row] = t.backlog().machines
    assert row.messages == 1 and row.oldest_published_at is not None


def test_a_sqlite_transport_from_before_the_publish_times_gets_its_column(tmp_path):
    import sqlite3

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)  # the messages table as SCHEMA_VERSION 5 left it
    conn.execute(
        "CREATE TABLE harel_transport_messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
        "group_id TEXT NOT NULL, event TEXT NOT NULL, locked_by TEXT, lock_expiry REAL)"
    )
    conn.commit()
    conn.close()
    t = SqliteTransport(db, clock=lambda: 50.0)
    t.publish("g", Event(kind="e"), routing_key="order")
    assert t.backlog().machines[0].oldest_published_at == 50.0


def test_the_redis_backlog_reads_the_groups_a_chunk_per_call(monkeypatch):
    """Redis runs one script at a time: the backlog reads a bounded chunk of groups per call, so
    it never holds Redis for the whole queue — and the chunks add up to the whole."""
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport
    from harel.engine.transport import redis as redis_transport

    monkeypatch.setattr(redis_transport, "_BACKLOG_CHUNK", 7)
    t = RedisTransport(fakeredis.FakeStrictRedis(), clock=lambda: 10.0)
    for i in range(50):
        t.publish(f"g{i}", Event(kind="E"), routing_key="order" if i % 2 else "payment")
    calls = []
    script = t._backlog_script
    t._backlog_script = lambda **kw: calls.append(kw) or script(**kw)
    rows = {b.routing_key: (b.messages, b.executions, b.oldest_published_at) for b in t.backlog().machines}
    assert rows == {"order": (25, 25, 10.0), "payment": (25, 25, 10.0)}
    assert len(calls) > 1 and all(c["args"][2] == 7 for c in calls)
