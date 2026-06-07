"""`ExecutionStore.list_executions` contract against the REAL networked backends
(Postgres / rqlite / Mongo / SurrealDB / DynamoDB-on-LocalStack) in the stack.

The in-process fakes are covered in test/unit/engine/test_store_listing.py; this runs
the same shared contract (`listing_contract.assert_contract`) over a real server, gated
by STM_STORE_BACKEND (one backend active per compose run; the rest skip). A unique `ns`
per run isolates the seed from any other executions sharing the backend's tables.
"""

import os
import uuid

import pytest
from listing_contract import assert_contract

pytestmark = pytest.mark.stack


def _ns() -> str:
    return f"lst-{uuid.uuid4().hex[:8]}-"


def test_postgres_listing():
    if os.environ.get("STM_STORE_BACKEND") != "postgres":
        pytest.skip("not the postgres backend")
    dsn = os.environ.get("STM_POSTGRES_DSN")
    if not dsn:
        pytest.skip("STM_POSTGRES_DSN not set")
    from harel.engine.store import PostgresStore

    store = PostgresStore.from_dsn(dsn)
    try:
        assert_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_rqlite_listing():
    if os.environ.get("STM_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("STM_RQLITE_URL")
    if not url:
        pytest.skip("STM_RQLITE_URL not set")
    from harel.engine.store import RqliteStore

    store = RqliteStore.from_url(url)
    try:
        assert_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_mongo_listing():
    if os.environ.get("STM_STORE_BACKEND") != "mongo":
        pytest.skip("not the mongo backend")
    url = os.environ.get("STM_MONGO_URL")
    if not url:
        pytest.skip("STM_MONGO_URL not set")
    from harel.engine.store import MongoStore

    store = MongoStore.from_url(url, os.environ.get("STM_MONGO_DB", "harel"))
    try:
        assert_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_surreal_listing():
    if os.environ.get("STM_STORE_BACKEND") != "surrealdb":
        pytest.skip("not the surrealdb backend")
    url = os.environ.get("STM_SURREAL_URL")
    if not url:
        pytest.skip("STM_SURREAL_URL not set")
    from harel.engine.store import SurrealStore

    store = SurrealStore.from_url(
        url,
        namespace=os.environ.get("STM_SURREAL_NS", "harel"),
        database=os.environ.get("STM_SURREAL_DB", "harel"),
        username=os.environ.get("STM_SURREAL_USER"),
        password=os.environ.get("STM_SURREAL_PASS", ""),
    )
    try:
        assert_contract(store, ordered=True, ns=_ns())
    finally:
        store.close()


def test_dynamodb_listing():
    if os.environ.get("STM_STORE_BACKEND") != "dynamodb":
        pytest.skip("not the dynamodb backend")
    endpoint = os.environ.get("STM_DYNAMODB_ENDPOINT")
    if not endpoint:
        pytest.skip("STM_DYNAMODB_ENDPOINT not set")
    from harel.engine.store import DynamoDBStore

    store = DynamoDBStore.create(endpoint, os.environ.get("STM_AWS_REGION", "us-east-1"))
    try:
        assert_contract(store, ordered=False, ns=_ns())  # Scan is unordered
    finally:
        store.close()
