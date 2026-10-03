"""`list_executions` contract over every in-process backend, sync and async (no Docker):
Dict, Sqlite, libSQL, Redis (fakeredis), Mongo (mongomock, sync only), DynamoDB (moto).
The networked servers are covered in test/integration/ (stack).

The shared seed + assertions live in `harel.testing`; here we just build each store
and say whether its listing is order-stable (Redis/Dynamo Scan are unordered).
"""

import pytest

from harel.engine.aio_store import AsyncDictStore, AsyncSqliteStore
from harel.engine.store import DictStore, SqliteStore
from harel.testing import assert_async_listing_contract, assert_listing_contract


def test_dict_listing():
    assert_listing_contract(DictStore(), ordered=True)


def test_sqlite_listing(tmp_path):
    store = SqliteStore(tmp_path / "stm.db")
    try:
        assert_listing_contract(store, ordered=True)
    finally:
        store.close()


def test_redis_listing():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.store import RedisStore

    # SCAN is unordered and best-effort per page -> order-agnostic assertions only
    assert_listing_contract(RedisStore(fakeredis.FakeStrictRedis()), ordered=False)


def test_mongo_listing():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore

    assert_listing_contract(MongoStore(mongomock.MongoClient()), ordered=True)


def test_dynamodb_listing():
    moto = pytest.importorskip("moto")
    import boto3

    from harel.engine.store import DynamoDBStore

    with moto.mock_aws():
        # DynamoDB Scan is unordered
        assert_listing_contract(
            DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1")), ordered=False
        )


def test_libsql_listing(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.store import LibsqlStore

    store = LibsqlStore(str(tmp_path / "stm.db"))
    try:
        assert_listing_contract(store, ordered=True)
    finally:
        store.close()


async def test_async_dict_listing():
    await assert_async_listing_contract(AsyncDictStore(), ordered=True)


async def test_async_sqlite_listing(tmp_path):
    store = await AsyncSqliteStore.create(str(tmp_path / "stm.db"))
    try:
        await assert_async_listing_contract(store, ordered=True)
    finally:
        await store.close()


async def test_async_libsql_listing(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.aio_store import AsyncLibsqlStore

    store = await AsyncLibsqlStore.create(str(tmp_path / "stm.db"))
    try:
        await assert_async_listing_contract(store, ordered=True)
    finally:
        await store.close()


async def test_async_redis_listing():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.aio_store import AsyncRedisStore

    await assert_async_listing_contract(AsyncRedisStore(fakeredis.aioredis.FakeRedis()), ordered=False)


async def test_async_dynamodb_listing():
    aiomoto = pytest.importorskip("aiomoto")
    from harel.engine.aio_store import AsyncDynamoDBStore

    async with aiomoto.mock_aws():
        store = await AsyncDynamoDBStore.create(region="us-east-1")
        try:
            await assert_async_listing_contract(store, ordered=False)
        finally:
            await store.close()
