"""`prefix` and `create_schema` on the real SQL servers in the stack (Postgres, rqlite), gated
by HAREL_STORE_BACKEND like the other stack tests: the contracts under a fresh prefix, two
prefixes in one database, and a backend running on the schema a migration tool applied from
`sql_schema` (`create_schema=False`). The in-process backends: test/unit/engine/test_schema.py."""

import os
import uuid

import pytest

from harel import Event
from harel.engine.execution import Execution
from harel.engine.schema import Names, sql_schema
from harel.testing import (
    assert_async_listing_contract,
    assert_listing_contract,
    assert_outbox_contract,
    assert_purge_contract,
    assert_transport_contract,
)

pytestmark = pytest.mark.stack


def _fresh() -> str:
    return f"s{uuid.uuid4().hex[:8]}"


def _env(backend: str, var: str) -> str:
    if os.environ.get("HAREL_STORE_BACKEND") != backend:
        pytest.skip(f"not the {backend} stack")
    value = os.environ.get(var)
    if not value:
        pytest.skip(f"{var} not set")
    return value


def _isolated(a, b) -> None:
    for store, eid in ((a, "only-in-a"), (b, "only-in-b")):
        store.commit(Execution(id=eid, definition_id="M"), [(eid, Event(kind="Go"))], processed_event_id="e1")
    assert a.load("only-in-b") is None and b.load("only-in-a") is None
    assert [e.target_id for e in a.pending_outbox()] == ["only-in-a"]
    assert a.is_processed("only-in-a", "e1") and not b.is_processed("only-in-a", "e1")


# --- Postgres ---------------------------------------------------------------------------------
def test_postgres_under_a_prefix():
    import psycopg

    from harel.engine.store import PostgresStore
    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    prefix = _fresh()
    store = PostgresStore(psycopg.connect(dsn), prefix=prefix)
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)
    _isolated(store, PostgresStore(psycopg.connect(dsn), prefix=_fresh()))
    assert_transport_contract(
        lambda clock: PostgresTransport(psycopg.connect(dsn), clock=clock, prefix=_fresh())
    )
    with psycopg.connect(dsn) as conn:
        functions = {
            r[0] for r in conn.execute("SELECT proname FROM pg_proc WHERE proname LIKE %s", (prefix + "%",))
        }
    assert functions == {Names(prefix).commit_cas}


async def test_async_postgres_under_a_prefix():
    from harel.engine.aio_store import AsyncPostgresStore

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    store = await AsyncPostgresStore.from_dsn(dsn, prefix=_fresh())
    try:
        await assert_async_listing_contract(store, ordered=True)
    finally:
        await store.close()


def test_postgres_on_a_migrated_schema():
    import psycopg

    from harel.engine.store import PostgresStore
    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    prefix = _fresh()
    with psycopg.connect(dsn) as conn:  # what a migration tool would apply
        for stmt in sql_schema("postgres", prefix):
            conn.execute(stmt)
    assert_outbox_contract(PostgresStore(psycopg.connect(dsn), prefix=prefix, create_schema=False))
    transport = PostgresTransport(psycopg.connect(dsn), prefix=prefix, create_schema=False)
    transport.publish("G", Event(kind="e1"))
    lease = transport.claim("w", 30)
    assert lease is not None and lease.event.kind == "e1"
    transport.ack(lease)


def test_postgres_without_create_schema_creates_nothing():
    import psycopg

    from harel.engine.store import PostgresStore

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    store = PostgresStore(psycopg.connect(dsn), prefix=_fresh(), create_schema=False)
    with pytest.raises(psycopg.errors.UndefinedTable):
        store.load("x")


# --- rqlite -----------------------------------------------------------------------------------
def test_rqlite_under_a_prefix():
    from harel.engine.store import RqliteStore
    from harel.engine.transport import RqliteTransport

    url = _env("rqlite", "HAREL_RQLITE_URL")
    store = RqliteStore(url, prefix=_fresh())
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)
    _isolated(store, RqliteStore(url, prefix=_fresh()))
    assert_transport_contract(lambda clock: RqliteTransport(url, clock=clock, prefix=_fresh()))


def test_rqlite_on_a_migrated_schema():
    from harel.engine.store import RqliteStore

    url = _env("rqlite", "HAREL_RQLITE_URL")
    prefix = _fresh()
    RqliteStore(url, prefix="setup", create_schema=False)._execute(sql_schema("sqlite", prefix))
    store = RqliteStore.from_url(url, prefix=prefix, create_schema=False)
    assert_outbox_contract(store)


def _upgrade_recipe(section: str) -> list[str]:
    """The SQL statements the upgrade guide gives for `section` — what this test runs, so the
    guide can't drift from what works."""
    import re
    from pathlib import Path

    guide = (Path(__file__).resolve().parents[2] / "docs/guide/upgrading.md").read_text()
    body = guide.split(f"## {section}\n", 1)[1]
    sql = re.search(r"```sql\n(.*?)```", body, re.S).group(1)
    lines = [ln.split("--")[0].strip() for ln in sql.splitlines()]
    return [s.strip() for s in " ".join(ln for ln in lines if ln).split(";") if s.strip()]


def test_postgres_upgrade_recipe():
    """A database as 0.6 left it — in its own Postgres schema, so it doesn't meet the rest —
    upgraded with the guide's statements, is what a 0.7 store and transport read."""
    import psycopg

    from harel.engine.store import PostgresStore
    from harel.engine.transport import PostgresTransport

    dsn = _env("postgres", "HAREL_POSTGRES_DSN")
    legacy = _fresh()

    def connect():
        conn = psycopg.connect(dsn)
        conn.execute(f"SET search_path TO {legacy}")
        return conn

    with psycopg.connect(dsn) as setup:
        setup.execute(f"CREATE SCHEMA {legacy}")
    exe = Execution(id="order-1", definition_id="order", version=3)
    with connect() as old:
        for ddl in (
            "CREATE TABLE executions (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, "
            "version INT NOT NULL)",
            "CREATE TABLE outbox (seq BIGSERIAL PRIMARY KEY, target_id TEXT, event TEXT NOT NULL)",
            "CREATE TABLE processed_events (execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, event_id))",
            "CREATE TABLE timers (execution_id TEXT NOT NULL, path TEXT NOT NULL, "
            "fire_at DOUBLE PRECISION NOT NULL, PRIMARY KEY (execution_id, path))",
            "CREATE TABLE spawns (seq BIGSERIAL PRIMARY KEY, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
            "root_path TEXT NOT NULL, context TEXT NOT NULL)",
            "CREATE TABLE trace (execution_id TEXT NOT NULL, idx INT NOT NULL, entry TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, idx))",
            "CREATE TABLE transport_messages (seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)",
            "CREATE INDEX transport_messages_group ON transport_messages (group_id, seq)",
            "CREATE TABLE transport_groups (group_id TEXT PRIMARY KEY, locked_by TEXT, "
            "lock_expiry DOUBLE PRECISION, priority INT NOT NULL DEFAULT 0)",
            "CREATE INDEX transport_groups_claimable ON transport_groups (lock_expiry)",
        ):
            old.execute(ddl)
        old.execute(
            "INSERT INTO executions VALUES (%s, %s, %s, %s)",
            (exe.id, exe.definition_id, exe.model_dump_json(), 3),
        )
        old.execute(
            "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
            ("order-1", Event(kind="Paid").model_dump_json()),
        )
        old.execute("INSERT INTO transport_groups (group_id) VALUES ('order-1')")

    with connect() as conn:  # the upgrade
        for statement in _upgrade_recipe("Postgres"):
            if statement not in ("BEGIN", "COMMIT"):
                conn.execute(statement)

    assert PostgresStore(connect()).load("order-1").version == 3
    lease = PostgresTransport(connect()).claim("worker", 30)
    assert lease is not None and lease.event.kind == "Paid"
