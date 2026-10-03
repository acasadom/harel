"""`purge` contract over every in-process backend, sync and async (no Docker). The
networked servers (Postgres, rqlite, Mongo, DynamoDB-on-LocalStack) are covered in
test/integration/test_store_purge.py (stack). Shared seed + assertions live in
`harel.testing`."""

import pytest

from harel.engine.aio_store import AsyncDictStore, AsyncSqliteStore
from harel.engine.store import DictStore, SqliteStore
from harel.testing import assert_async_purge_contract, assert_purge_contract


def test_dict_purge():
    assert_purge_contract(DictStore())


def test_sqlite_purge(tmp_path):
    store = SqliteStore(tmp_path / "stm.db")
    try:
        assert_purge_contract(store)
    finally:
        store.close()


def test_libsql_purge(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.store import LibsqlStore

    store = LibsqlStore(str(tmp_path / "stm.db"))
    try:
        assert_purge_contract(store)
    finally:
        store.close()


def test_redis_purge():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.store import RedisStore

    assert_purge_contract(RedisStore(fakeredis.FakeStrictRedis()))


def test_mongo_purge():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore

    assert_purge_contract(MongoStore(mongomock.MongoClient()))


def test_dynamodb_purge():
    moto = pytest.importorskip("moto")
    import boto3

    from harel.engine.store import DynamoDBStore

    with moto.mock_aws():
        assert_purge_contract(DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1")))


async def test_async_dict_purge():
    await assert_async_purge_contract(AsyncDictStore())


async def test_async_sqlite_purge(tmp_path):
    store = await AsyncSqliteStore.create(str(tmp_path / "stm.db"))
    try:
        await assert_async_purge_contract(store)
    finally:
        await store.close()


async def test_async_libsql_purge(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.aio_store import AsyncLibsqlStore

    store = await AsyncLibsqlStore.create(str(tmp_path / "stm.db"))
    try:
        await assert_async_purge_contract(store)
    finally:
        await store.close()


async def test_async_redis_purge():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.aio_store import AsyncRedisStore

    await assert_async_purge_contract(AsyncRedisStore(fakeredis.aioredis.FakeRedis()))


async def test_async_dynamodb_purge():
    aiomoto = pytest.importorskip("aiomoto")
    from harel.engine.aio_store import AsyncDynamoDBStore

    async with aiomoto.mock_aws():
        store = await AsyncDynamoDBStore.create(region="us-east-1")
        try:
            await assert_async_purge_contract(store)
        finally:
            await store.close()
