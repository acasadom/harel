"""Outbox-seq contract of `commit` against the REAL networked backends (Postgres /
rqlite / Mongo / DynamoDB-on-LocalStack), sync and async, in the stack.

The in-process backends are covered in test/unit/engine/test_store_outbox.py; this runs
the same shared contract (`harel.testing`) over a real server, gated by
STM_STORE_BACKEND (one backend active per compose run; the rest skip). A unique `ns`
per run isolates the seed from any other executions sharing the backend's tables.
"""

import os
import uuid

import pytest

from harel.testing import assert_async_outbox_contract, assert_outbox_contract

pytestmark = pytest.mark.stack


def _ns() -> str:
    return f"obx-{uuid.uuid4().hex[:8]}-"


def _env(backend: str, var: str) -> str:
    if os.environ.get("STM_STORE_BACKEND") != backend:
        pytest.skip(f"not the {backend} backend")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def test_postgres_outbox():
    from harel.engine.store import PostgresStore

    store = PostgresStore.from_dsn(_env("postgres", "STM_POSTGRES_DSN"))
    try:
        assert_outbox_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_postgres_outbox():
    from harel.engine.aio_store import AsyncPostgresStore

    store = await AsyncPostgresStore.from_dsn(_env("postgres", "STM_POSTGRES_DSN"))
    try:
        await assert_async_outbox_contract(store, ns=_ns())
    finally:
        await store.close()


def test_rqlite_outbox():
    from harel.engine.store import RqliteStore

    store = RqliteStore.from_url(_env("rqlite", "STM_RQLITE_URL"))
    try:
        assert_outbox_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_rqlite_outbox():
    from harel.engine.aio_store import AsyncRqliteStore

    store = await AsyncRqliteStore.from_url(_env("rqlite", "STM_RQLITE_URL"))
    try:
        await assert_async_outbox_contract(store, ns=_ns())
    finally:
        await store.close()


def test_mongo_outbox():
    from harel.engine.store import MongoStore

    store = MongoStore.from_url(_env("mongo", "STM_MONGO_URL"), os.environ.get("STM_MONGO_DB", "harel"))
    try:
        assert_outbox_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_mongo_outbox():
    from harel.engine.aio_store import AsyncMongoStore

    store = await AsyncMongoStore.from_url(
        _env("mongo", "STM_MONGO_URL"), os.environ.get("STM_MONGO_DB", "harel")
    )
    try:
        await assert_async_outbox_contract(store, ns=_ns())
    finally:
        await store.close()


def test_dynamodb_outbox():
    from harel.engine.store import DynamoDBStore

    endpoint = _env("dynamodb", "STM_DYNAMODB_ENDPOINT")
    store = DynamoDBStore.create(endpoint, os.environ.get("STM_AWS_REGION", "us-east-1"))
    try:
        assert_outbox_contract(store, ns=_ns())
    finally:
        store.close()


async def test_async_dynamodb_outbox():
    from harel.engine.aio_store import AsyncDynamoDBStore

    endpoint = _env("dynamodb", "STM_DYNAMODB_ENDPOINT")
    store = await AsyncDynamoDBStore.create(endpoint, os.environ.get("STM_AWS_REGION", "us-east-1"))
    try:
        await assert_async_outbox_contract(store, ns=_ns())
    finally:
        await store.close()
