"""Routing claims by machine (`harel.testing.assert_machine_routing_contract`) against the REAL
networked transports (Postgres / rqlite / Mongo), and the refusal of SQS-on-LocalStack, in the
stack, gated by HAREL_TRANSPORT_BACKEND (one backend per compose run; the rest skip). The
in-process transports are covered in test/unit/engine/test_transport_routing.py.

Each check gets a fresh prefix (SQS a fresh queue), so it starts from an empty transport."""

import os
import uuid

import pytest

from harel.testing import (
    assert_async_machine_routing_contract,
    assert_machine_routing_contract,
    assert_refuses_machine_routing,
)

pytestmark = pytest.mark.stack


def _fresh() -> str:
    return f"r{uuid.uuid4().hex[:8]}"


def _env(backend: str, var: str) -> str:
    if os.environ.get("HAREL_TRANSPORT_BACKEND") != backend:
        pytest.skip(f"not the {backend} transport")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def test_postgres_machine_routing():
    import psycopg

    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    assert_machine_routing_contract(
        lambda clock: PostgresTransport(psycopg.connect(dsn), clock=clock, prefix=_fresh())
    )


async def test_async_postgres_machine_routing():
    from harel.engine.aio_transport import AsyncPostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    await assert_async_machine_routing_contract(
        lambda clock: AsyncPostgresTransport.from_dsn(dsn, clock=clock, prefix=_fresh())
    )


def test_rqlite_machine_routing():
    from harel.engine.transport import RqliteTransport

    url = _env("rqlite", "HAREL_RQLITE_URL")
    assert_machine_routing_contract(lambda clock: RqliteTransport(url, clock=clock, prefix=_fresh()))


async def test_async_rqlite_machine_routing():
    from harel.engine.aio_transport import AsyncRqliteTransport

    url = _env("rqlite", "HAREL_RQLITE_URL")
    await assert_async_machine_routing_contract(
        lambda clock: AsyncRqliteTransport.from_url(url, prefix=_fresh())
    )


def test_mongo_machine_routing():
    import pymongo

    from harel.engine.transport import MongoTransport

    url = _env("mongo", "HAREL_MONGO_URL")
    db = os.environ.get("HAREL_MONGO_DB", "harel")
    assert_machine_routing_contract(
        lambda clock: MongoTransport(pymongo.MongoClient(url), db, prefix=_fresh(), clock=clock)
    )


async def test_async_mongo_machine_routing():
    from harel.engine.aio_transport import AsyncMongoTransport

    url = _env("mongo", "HAREL_MONGO_URL")
    db = os.environ.get("HAREL_MONGO_DB", "harel")
    await assert_async_machine_routing_contract(
        lambda clock: AsyncMongoTransport.from_url(url, db, prefix=_fresh())
    )


def test_sqs_refuses_machine_routing():
    from harel.engine.transport import SqsTransport

    endpoint = _env("sqs", "HAREL_SQS_ENDPOINT")
    region = os.environ.get("HAREL_AWS_REGION", "us-east-1")
    assert_refuses_machine_routing(
        lambda clock: SqsTransport.create(endpoint, f"r{uuid.uuid4().hex[:8]}.fifo", region=region)
    )
