"""The `Transport` contract (`harel.testing.assert_transport_contract`) against the REAL
networked transports (Postgres / rqlite / Mongo / SQS-on-LocalStack) in the stack, gated by
STM_TRANSPORT_BACKEND (one backend per compose run; the rest skip). The in-process transports
are covered in test/unit/engine/test_transport_contract.py.

The contract needs an empty transport for each check: Mongo and SQS get a fresh prefix or
queue; Postgres and rqlite (fixed table names) are emptied first — the stack runs one group
at a time, so nothing else is using them."""

import os
import uuid

import pytest

from harel.testing import assert_transport_contract

pytestmark = pytest.mark.stack


def _env(backend: str, var: str) -> str:
    if os.environ.get("STM_TRANSPORT_BACKEND") != backend:
        pytest.skip(f"not the {backend} transport")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def test_postgres_transport_contract():
    import psycopg

    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "STM_POSTGRES_DSN")

    def make(clock):
        t = PostgresTransport(psycopg.connect(dsn), clock=clock)
        with t._conn.cursor() as cur:
            cur.execute("TRUNCATE transport_messages, transport_groups")
        t._conn.commit()
        return t

    assert_transport_contract(make)


def test_rqlite_transport_contract():
    from harel.engine.transport import RqliteTransport

    url = _env("rqlite", "STM_RQLITE_URL")

    def make(clock):
        t = RqliteTransport(url, clock=clock)
        t._execute([["DELETE FROM messages"], ["DELETE FROM groups"]])
        return t

    assert_transport_contract(make)


def test_mongo_transport_contract():
    import pymongo

    from harel.engine.transport import MongoTransport

    url = _env("mongo", "STM_MONGO_URL")
    db = os.environ.get("STM_MONGO_DB", "harel")
    assert_transport_contract(
        lambda clock: MongoTransport(
            pymongo.MongoClient(url), db, prefix=f"c{uuid.uuid4().hex[:8]}", clock=clock
        )
    )


def test_sqs_transport_contract():
    from harel.engine.transport import SqsTransport

    endpoint = _env("sqs", "STM_SQS_ENDPOINT")
    region = os.environ.get("STM_AWS_REGION", "us-east-1")
    # SQS keeps visibility timeouts itself (no injected clock) and has no priorities
    assert_transport_contract(
        lambda clock: SqsTransport.create(endpoint, f"c{uuid.uuid4().hex[:8]}.fifo", region=region),
        timing=False,
        priorities=False,
    )
