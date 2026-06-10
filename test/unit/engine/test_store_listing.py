"""`ExecutionStore.list_executions` contract over every in-process backend (no Docker):
Dict, Sqlite, RedisStore (fakeredis), MongoStore (mongomock), DynamoDBStore (moto).
The networked servers are covered in test/integration/ (stack).

The shared seed + assertions live in `_listing_contract`; here we just build each store
and say whether its listing is order-stable (Redis/Dynamo Scan are unordered).
"""


import pytest
from listing_contract import assert_contract  # noqa: E402 (test-root bare import)

from harel.engine.store import DictStore, SqliteStore


def test_dict_listing():
    assert_contract(DictStore(), ordered=True)


def test_sqlite_listing(tmp_path):
    store = SqliteStore(tmp_path / "stm.db")
    try:
        assert_contract(store, ordered=True)
    finally:
        store.close()


def test_redis_listing():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.store import RedisStore

    # SCAN is unordered and best-effort per page -> order-agnostic assertions only
    assert_contract(RedisStore(fakeredis.FakeStrictRedis()), ordered=False)


def test_mongo_listing():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore

    assert_contract(MongoStore(mongomock.MongoClient()), ordered=True)


def test_dynamodb_listing():
    moto = pytest.importorskip("moto")
    import boto3

    from harel.engine.store import DynamoDBStore

    with moto.mock_aws():
        # DynamoDB Scan is unordered
        assert_contract(DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1")), ordered=False)
