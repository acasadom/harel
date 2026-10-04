"""The `Transport` contract (`harel.testing.assert_transport_contract`) against the REAL
networked transports (Postgres / rqlite / Mongo / SQS-on-LocalStack) in the stack, gated by
HAREL_TRANSPORT_BACKEND (one backend per compose run; the rest skip). The in-process transports
are covered in test/unit/engine/test_transport_contract.py.

The contract needs an empty transport for each check: each gets a fresh prefix (SQS a fresh
queue)."""

import os
import uuid

import pytest

from harel.testing import assert_transport_contract

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
