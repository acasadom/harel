"""`list_executions` contract against the REAL networked backends (Postgres / rqlite /
Mongo / DynamoDB-on-LocalStack), sync and async, in the stack.

The in-process fakes are covered in test/unit/engine/test_store_listing.py; this runs
the same shared contract (`harel.testing.assert_listing_contract`) over a real server, gated
by HAREL_STORE_BACKEND (one backend active per compose run; the rest skip). A unique `ns`
per run isolates the seed from any other executions sharing the backend's tables.
"""

import os
import uuid

import pytest

from harel.testing import assert_async_listing_contract, assert_listing_contract

pytestmark = pytest.mark.stack


def _ns() -> str:
    return f"lst-{uuid.uuid4().hex[:8]}-"


def test_postgres_listing():
    if os.environ.get("HAREL_STORE_BACKEND") != "postgres":
        pytest.skip("not the postgres backend")
    dsn = os.environ.get("HAREL_POSTGRES_DSN")
    if not dsn:
        pytest.skip("HAREL_POSTGRES_DSN not set")
    from harel.engine.store import PostgresStore

    store = PostgresStore.from_dsn(dsn)
    try:
        assert_listing_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_rqlite_listing():
    if os.environ.get("HAREL_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("HAREL_RQLITE_URL")
    if not url:
        pytest.skip("HAREL_RQLITE_URL not set")
    from harel.engine.store import RqliteStore

    store = RqliteStore.from_url(url)
    try:
        assert_listing_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_mongo_listing():
    if os.environ.get("HAREL_STORE_BACKEND") != "mongo":
        pytest.skip("not the mongo backend")
    url = os.environ.get("HAREL_MONGO_URL")
    if not url:
        pytest.skip("HAREL_MONGO_URL not set")
    from harel.engine.store import MongoStore

    store = MongoStore.from_url(url, os.environ.get("HAREL_MONGO_DB", "harel"))
    try:
        assert_listing_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_dynamodb_listing():
    if os.environ.get("HAREL_STORE_BACKEND") != "dynamodb":
        pytest.skip("not the dynamodb backend")
    endpoint = os.environ.get("HAREL_DYNAMODB_ENDPOINT")
    if not endpoint:
        pytest.skip("HAREL_DYNAMODB_ENDPOINT not set")
    from harel.engine.store import DynamoDBStore

    store = DynamoDBStore.create(endpoint, os.environ.get("HAREL_AWS_REGION", "us-east-1"))
    try:
        assert_listing_contract(store, ordered=False, ns=_ns())  # Scan is unordered
    finally:
        store.close()


def _env(backend: str, var: str) -> str:
    if os.environ.get("HAREL_STORE_BACKEND") != backend:
        pytest.skip(f"not the {backend} backend")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


async def test_async_postgres_listing():
    from harel.engine.aio_store import AsyncPostgresStore

    store = await AsyncPostgresStore.from_dsn(_env("postgres", "HAREL_POSTGRES_DSN"))
    try:
        await assert_async_listing_contract(store, ordered=True, ns=_ns())
    finally:
        await store.close()


async def test_async_rqlite_listing():
    from harel.engine.aio_store import AsyncRqliteStore

    store = await AsyncRqliteStore.from_url(_env("rqlite", "HAREL_RQLITE_URL"))
    try:
        await assert_async_listing_contract(store, ordered=True, ns=_ns())
    finally:
        await store.close()


async def test_async_mongo_listing():
    from harel.engine.aio_store import AsyncMongoStore

    store = await AsyncMongoStore.from_url(
        _env("mongo", "HAREL_MONGO_URL"), os.environ.get("HAREL_MONGO_DB", "harel")
    )
    try:
        await assert_async_listing_contract(store, ordered=True, ns=_ns())
    finally:
        await store.close()


async def test_async_dynamodb_listing():
    from harel.engine.aio_store import AsyncDynamoDBStore

    endpoint = _env("dynamodb", "HAREL_DYNAMODB_ENDPOINT")
    store = await AsyncDynamoDBStore.create(endpoint, os.environ.get("HAREL_AWS_REGION", "us-east-1"))
    try:
        await assert_async_listing_contract(store, ordered=False, ns=_ns())  # Scan is unordered
    finally:
        await store.close()
