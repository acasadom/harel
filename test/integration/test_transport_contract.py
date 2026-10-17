"""The `Transport` contract (`harel.testing.assert_transport_contract` and its async twin) against
the REAL networked transports (Postgres / rqlite / Mongo / Redis / SQS-on-LocalStack) in the stack, gated by
HAREL_TRANSPORT_BACKEND (one backend per compose run; the rest skip). The in-process transports
are covered in test/unit/engine/test_transport_contract.py.

The contract needs an empty transport for each check: each gets a fresh prefix (SQS a fresh
queue)."""

import os
import uuid

import pytest

from harel.testing import assert_async_transport_contract, assert_transport_contract

pytestmark = pytest.mark.stack


def _fresh() -> str:
    return f"c{uuid.uuid4().hex[:8]}"


def _env(backend: str, var: str) -> str:
    if os.environ.get("HAREL_TRANSPORT_BACKEND") != backend:
        pytest.skip(f"not the {backend} transport")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def test_postgres_transport_contract():
    import psycopg

    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")

    assert_transport_contract(
        lambda clock: PostgresTransport(psycopg.connect(dsn), clock=clock, prefix=_fresh())
    )


def test_rqlite_transport_contract():
    from harel.engine.transport import RqliteTransport

    url = _env("rqlite", "HAREL_RQLITE_URL")

    assert_transport_contract(lambda clock: RqliteTransport(url, clock=clock, prefix=_fresh()))


def test_mongo_transport_contract():
    import pymongo

    from harel.engine.transport import MongoTransport

    url = _env("mongo", "HAREL_MONGO_URL")
    db = os.environ.get("HAREL_MONGO_DB", "harel")
    assert_transport_contract(
        lambda clock: MongoTransport(pymongo.MongoClient(url), db, prefix=_fresh(), clock=clock)
    )


def test_sqs_transport_contract():
    from harel.engine.transport import SqsTransport

    endpoint = _env("sqs", "HAREL_SQS_ENDPOINT")
    region = os.environ.get("HAREL_AWS_REGION", "us-east-1")
    # SQS keeps visibility timeouts itself (no injected clock) and has no priorities
    assert_transport_contract(
        lambda clock: SqsTransport.create(endpoint, f"c{uuid.uuid4().hex[:8]}.fifo", region=region),
        timing=False,
        priorities=False,
    )


async def test_async_postgres_transport_contract():
    from harel.engine.aio_transport import AsyncPostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    await assert_async_transport_contract(
        lambda clock: AsyncPostgresTransport.from_dsn(dsn, clock=clock, prefix=_fresh())
    )


async def test_async_rqlite_transport_contract():
    from harel.engine.aio_transport import AsyncRqliteTransport

    url = _env("rqlite", "HAREL_RQLITE_URL")
    await assert_async_transport_contract(
        lambda clock: AsyncRqliteTransport.from_url(url, clock=clock, prefix=_fresh())
    )


async def test_async_mongo_transport_contract():
    from harel.engine.aio_transport import AsyncMongoTransport

    url = _env("mongo", "HAREL_MONGO_URL")
    db = os.environ.get("HAREL_MONGO_DB", "harel")
    await assert_async_transport_contract(
        lambda clock: AsyncMongoTransport.from_url(url, db, clock=clock, prefix=_fresh())
    )


async def test_async_sqs_transport_contract():
    from harel.engine.aio_transport import AsyncSqsTransport

    endpoint = _env("sqs", "HAREL_SQS_ENDPOINT")
    region = os.environ.get("HAREL_AWS_REGION", "us-east-1")
    await assert_async_transport_contract(
        lambda clock: AsyncSqsTransport.create(endpoint, f"c{uuid.uuid4().hex[:8]}.fifo", region=region),
        timing=False,
        priorities=False,
    )


def test_redis_transport_contract():
    import redis

    from harel.engine.transport import RedisTransport

    url = _env("redis", "HAREL_REDIS_URL")
    # Redis keeps its group locks with server-side TTLs (real time): the timing checks don't apply
    assert_transport_contract(
        lambda clock: RedisTransport(redis.Redis.from_url(url), clock=clock, prefix=_fresh()), timing=False
    )


async def test_async_redis_transport_contract():
    import redis.asyncio as aioredis

    from harel.engine.aio_transport import AsyncRedisTransport

    url = _env("redis", "HAREL_REDIS_URL")
    await assert_async_transport_contract(
        lambda clock: AsyncRedisTransport(aioredis.Redis.from_url(url), clock=clock, prefix=_fresh()),
        timing=False,
    )
