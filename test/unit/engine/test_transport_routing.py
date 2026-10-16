"""Routing claims by machine (`harel.testing.assert_machine_routing_contract`) over every
in-process transport that routes, sync and async, and the refusal of the ones that can't (no
Docker). The networked ones run in the stack (test/integration/test_transport_routing.py)."""

import itertools

import pytest

from harel.engine.aio_transport import AsyncInMemoryTransport, AsyncSqliteTransport
from harel.engine.transport import InMemoryTransport, SqliteTransport
from harel.spec.states import Event
from harel.testing import (
    assert_async_machine_routing_contract,
    assert_async_refuses_machine_routing,
    assert_machine_routing_contract,
)

_n = itertools.count()


def test_in_memory():
    assert_machine_routing_contract(lambda clock: InMemoryTransport(clock=clock))


def test_sqlite(tmp_path):
    assert_machine_routing_contract(lambda clock: SqliteTransport(tmp_path / f"q{next(_n)}.db", clock=clock))


def test_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.transport import LibsqlTransport

    assert_machine_routing_contract(
        lambda clock: LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock)
    )


def test_mongo():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.transport import MongoTransport

    assert_machine_routing_contract(lambda clock: MongoTransport(mongomock.MongoClient(), clock=clock))


def test_redis_refuses():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport
    from harel.testing import assert_refuses_machine_routing

    assert_refuses_machine_routing(lambda clock: RedisTransport(fakeredis.FakeStrictRedis(), clock=clock))


def test_sqs_refuses_before_any_call():
    from harel.engine.transport.sqs import SqsTransport

    with pytest.raises(NotImplementedError, match="SqsTransport can't restrict a claim"):
        SqsTransport(object(), "q.fifo").claim("w", 30, routing_keys=frozenset({"order"}))


async def test_async_in_memory():
    await assert_async_machine_routing_contract(lambda clock: AsyncInMemoryTransport(clock=clock))


async def test_async_sqlite(tmp_path):
    await assert_async_machine_routing_contract(
        lambda clock: AsyncSqliteTransport.create(str(tmp_path / f"q{next(_n)}.db"), clock=clock)
    )


async def test_async_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.aio_transport import AsyncLibsqlTransport
    from harel.engine.transport import LibsqlTransport

    await assert_async_machine_routing_contract(
        lambda clock: AsyncLibsqlTransport(LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock))
    )


async def test_async_redis_refuses():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.aio_transport import AsyncRedisTransport

    await assert_async_refuses_machine_routing(
        lambda clock: AsyncRedisTransport(fakeredis.aioredis.FakeRedis(), clock=clock)
    )


async def test_async_sqs_refuses_before_any_call():
    from harel.engine.aio_transport.sqs import AsyncSqsTransport

    with pytest.raises(NotImplementedError, match="AsyncSqsTransport can't restrict a claim"):
        await AsyncSqsTransport(object(), "q.fifo").claim("w", 30, routing_keys=frozenset({"order"}))


def test_a_sqlite_transport_from_before_routing_gets_its_column(tmp_path):
    import sqlite3

    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)  # the groups table as SCHEMA_VERSION 4 left it, with a message
    conn.execute(
        "CREATE TABLE harel_transport_groups (group_id TEXT PRIMARY KEY, "
        "last_claimed_at REAL NOT NULL DEFAULT 0.0, priority INT NOT NULL DEFAULT 0)"
    )
    conn.execute("INSERT INTO harel_transport_groups (group_id) VALUES ('old')")
    conn.commit()
    conn.close()

    t = SqliteTransport(db)
    t.publish("new", Event(kind="e"), routing_key="order")
    assert t.claim("w", 30, routing_keys=frozenset({"order"})).group_id == "new"
