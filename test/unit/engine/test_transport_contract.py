"""The `Transport` contract (`harel.testing.assert_transport_contract`) over every in-process
transport, sync and async (no Docker). The networked ones run it in the stack
(test/integration/test_transport_contract.py)."""

import itertools

import pytest

from harel.engine.aio_transport import AsyncInMemoryTransport, AsyncSqliteTransport
from harel.engine.transport import InMemoryTransport, SqliteTransport
from harel.testing import assert_async_transport_contract, assert_transport_contract

_n = itertools.count()


def test_in_memory():
    assert_transport_contract(lambda clock: InMemoryTransport(clock=clock))


def test_sqlite(tmp_path):
    assert_transport_contract(lambda clock: SqliteTransport(tmp_path / f"q{next(_n)}.db", clock=clock))


def test_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.transport import LibsqlTransport

    assert_transport_contract(lambda clock: LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock))


def test_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport

    # Redis keeps its group locks with server-side TTLs (real time), so an injected clock can't
    # move a park or a lease past its deadline: the timing checks don't apply
    assert_transport_contract(
        lambda clock: RedisTransport(fakeredis.FakeStrictRedis(), clock=clock), timing=False
    )


def test_mongo():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.transport import MongoTransport

    assert_transport_contract(lambda clock: MongoTransport(mongomock.MongoClient(), clock=clock))


async def test_async_in_memory():
    await assert_async_transport_contract(lambda clock: AsyncInMemoryTransport(clock=clock))


async def test_async_sqlite(tmp_path):
    await assert_async_transport_contract(
        lambda clock: AsyncSqliteTransport.create(str(tmp_path / f"q{next(_n)}.db"), clock=clock)
    )


async def test_async_libsql(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.aio_transport import AsyncLibsqlTransport
    from harel.engine.transport import LibsqlTransport

    await assert_async_transport_contract(
        lambda clock: AsyncLibsqlTransport(LibsqlTransport(str(tmp_path / f"q{next(_n)}.db"), clock=clock))
    )


async def test_async_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.aio_transport import AsyncRedisTransport

    await assert_async_transport_contract(
        lambda clock: AsyncRedisTransport(fakeredis.aioredis.FakeRedis(), clock=clock), timing=False
    )
