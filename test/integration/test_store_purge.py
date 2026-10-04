"""`purge` contract against the REAL networked backends (Postgres / rqlite / Mongo /
DynamoDB-on-LocalStack), sync and async, in the stack.

The in-process backends are covered in test/unit/engine/test_store_purge.py; this runs
the same shared contract (`harel.testing`) over a real server, gated by
HAREL_STORE_BACKEND (one backend active per compose run; the rest skip). A unique `ns`
per run isolates the seed from any other executions sharing the backend's tables.
"""

import os
import uuid

import pytest

from harel.testing import assert_async_purge_contract, assert_purge_contract

pytestmark = pytest.mark.stack


def _ns() -> str:
    return f"prg-{uuid.uuid4().hex[:8]}-"


def _env(backend: str, var: str) -> str:
    if os.environ.get("HAREL_STORE_BACKEND") != backend:
        pytest.skip(f"not the {backend} backend")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def test_postgres_purge():
    from harel.engine.store import PostgresStore

    store = PostgresStore.from_dsn(_env("postgres", "HAREL_POSTGRES_DSN"))
    try:
        assert_purge_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_postgres_purge():
    from harel.engine.aio_store import AsyncPostgresStore

    store = await AsyncPostgresStore.from_dsn(_env("postgres", "HAREL_POSTGRES_DSN"))
    try:
        await assert_async_purge_contract(store, ns=_ns())
    finally:
        await store.close()


def test_rqlite_purge():
    from harel.engine.store import RqliteStore

    store = RqliteStore.from_url(_env("rqlite", "HAREL_RQLITE_URL"))
    try:
        assert_purge_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_rqlite_purge():
    from harel.engine.aio_store import AsyncRqliteStore

    store = await AsyncRqliteStore.from_url(_env("rqlite", "HAREL_RQLITE_URL"))
    try:
        await assert_async_purge_contract(store, ns=_ns())
    finally:
        await store.close()


def test_mongo_purge():
    from harel.engine.store import MongoStore

    store = MongoStore.from_url(_env("mongo", "HAREL_MONGO_URL"), os.environ.get("HAREL_MONGO_DB", "harel"))
    try:
        assert_purge_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_mongo_purge():
    from harel.engine.aio_store import AsyncMongoStore

    store = await AsyncMongoStore.from_url(
        _env("mongo", "HAREL_MONGO_URL"), os.environ.get("HAREL_MONGO_DB", "harel")
    )
    try:
        await assert_async_purge_contract(store, ns=_ns())
    finally:
        await store.close()


def test_dynamodb_purge():
    from harel.engine.store import DynamoDBStore

    endpoint = _env("dynamodb", "HAREL_DYNAMODB_ENDPOINT")
    store = DynamoDBStore.create(endpoint, os.environ.get("HAREL_AWS_REGION", "us-east-1"))
    try:
        assert_purge_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_dynamodb_purge():
    from harel.engine.aio_store import AsyncDynamoDBStore

    endpoint = _env("dynamodb", "HAREL_DYNAMODB_ENDPOINT")
    store = await AsyncDynamoDBStore.create(endpoint, os.environ.get("HAREL_AWS_REGION", "us-east-1"))
    try:
        await assert_async_purge_contract(store, ns=_ns())
    finally:
        await store.close()
