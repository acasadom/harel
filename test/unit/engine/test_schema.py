"""`harel.engine.schema`: the names a backend creates under a `prefix`, the SQL schema as data,
and `create_schema`. The contracts run under a non-default prefix on every in-process backend;
two prefixes share one database without touching each other's data. Postgres and rqlite run
the same checks in the stack (test/integration/test_schema_stack.py)."""

import sqlite3

import pytest

from harel import Event
from harel.config import Config
from harel.engine.aio_store import AsyncSqliteStore
from harel.engine.aio_transport import AsyncSqliteTransport
from harel.engine.execution import Execution
from harel.engine.schema import (
    DEFAULT_PREFIX,
    SCHEMA_VERSION,
    Names,
    sql_schema,
    store_schema,
    transport_schema,
)
from harel.engine.store import SqliteStore
from harel.engine.transport import SqliteTransport
from harel.testing import (
    assert_async_listing_contract,
    assert_async_transport_contract,
    assert_listing_contract,
    assert_outbox_contract,
    assert_purge_contract,
    assert_transport_contract,
)

PREFIX = "acme_wf"


# --- names and DDL ----------------------------------------------------------------------------
def test_the_default_prefix_names_everything_harel():
    names = Names()
    assert DEFAULT_PREFIX == "harel" and SCHEMA_VERSION >= 1
    assert (names.executions, names.groups, names.messages, names.commit_cas, names.claim) == (
        "harel_executions",
        "harel_transport_groups",
        "harel_transport_messages",
        "harel_commit_cas",
        "harel_claim",
    )


@pytest.mark.parametrize("bad", ["", "1abc", "has-dash", "has space", "a;drop", "x" * 31, None])
def test_a_prefix_must_be_a_short_identifier(bad):
    with pytest.raises(ValueError, match="prefix must be an identifier"):
        Names(bad)


@pytest.mark.parametrize("dialect", ["sqlite", "postgres"])
def test_the_schema_is_all_under_the_prefix(dialect):
    ddl = "\n".join(sql_schema(dialect, PREFIX))
    assert sql_schema(dialect, PREFIX) == store_schema(dialect, PREFIX) + transport_schema(dialect, PREFIX)
    assert "{" not in ddl  # every placeholder filled
    for name in ("executions", "outbox", "processed_events", "spawns", "timers", "trace"):
        assert f"IF NOT EXISTS {PREFIX}_{name} " in ddl
    assert f"IF NOT EXISTS {PREFIX}_transport_messages " in ddl
    assert "harel" not in ddl
    if dialect == "postgres":
        for fn in ("commit_cas", "claim", "ack"):
            assert f"FUNCTION {PREFIX}_{fn}(" in ddl


def test_an_unknown_dialect_is_refused():
    with pytest.raises(ValueError, match="dialect must be one of"):
        sql_schema("mysql")


def test_the_sqlite_backends_create_exactly_the_described_schema(tmp_path):
    db = tmp_path / "s.db"
    SqliteStore(db, prefix=PREFIX).close()
    SqliteTransport(db, prefix=PREFIX).close()
    created = {
        r[0]
        for r in sqlite3.connect(db).execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
    }
    reference = tmp_path / "ref.db"
    conn = sqlite3.connect(reference)
    for stmt in sql_schema("sqlite", PREFIX):
        conn.execute(stmt)
    described = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}
    assert created == described and all(n.startswith(PREFIX + "_") for n in created)


# --- the contracts under a non-default prefix -------------------------------------------------
def test_sqlite_contracts_under_a_prefix(tmp_path):
    store = SqliteStore(tmp_path / "s.db", prefix=PREFIX)
    try:
        assert_listing_contract(store, ordered=True)
        assert_purge_contract(store)
        assert_outbox_contract(store)
    finally:
        store.close()
    n = iter(range(100))  # a fresh file per check: the contract wants an empty transport each time
    assert_transport_contract(
        lambda clock: SqliteTransport(tmp_path / f"t{next(n)}.db", clock=clock, prefix=PREFIX)
    )


async def test_async_sqlite_contracts_under_a_prefix(tmp_path):
    store = await AsyncSqliteStore.create(str(tmp_path / "s.db"), prefix=PREFIX)
    try:
        await assert_async_listing_contract(store, ordered=True)
    finally:
        await store.close()
    n = iter(range(100))
    await assert_async_transport_contract(
        lambda clock: AsyncSqliteTransport.create(
            str(tmp_path / f"t{next(n)}.db"), clock=clock, prefix=PREFIX
        )
    )


def test_libsql_contracts_under_a_prefix(tmp_path):
    pytest.importorskip("libsql")
    from harel.engine.store import LibsqlStore
    from harel.engine.transport import LibsqlTransport

    store = LibsqlStore(str(tmp_path / "s.db"), prefix=PREFIX)
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)
    n = iter(range(100))
    assert_transport_contract(
        lambda clock: LibsqlTransport(str(tmp_path / f"t{next(n)}.db"), clock=clock, prefix=PREFIX)
    )


def test_redis_contracts_under_a_prefix():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.store import RedisStore
    from harel.engine.transport import RedisTransport

    store = RedisStore(fakeredis.FakeStrictRedis(), prefix=PREFIX)
    assert_listing_contract(store, ordered=False)
    assert_purge_contract(store)
    assert_outbox_contract(store)
    assert_transport_contract(
        lambda clock: RedisTransport(fakeredis.FakeStrictRedis(), clock=clock, prefix=PREFIX), timing=False
    )


def test_mongo_contracts_under_a_prefix():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore
    from harel.engine.transport import MongoTransport

    store = MongoStore(mongomock.MongoClient(), prefix=PREFIX)
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)
    assert_transport_contract(
        lambda clock: MongoTransport(mongomock.MongoClient(), clock=clock, prefix=PREFIX)
    )


def test_dynamodb_contracts_under_a_prefix():
    moto = pytest.importorskip("moto")
    import boto3

    from harel.engine.store import DynamoDBStore

    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        store = DynamoDBStore(client, prefix=PREFIX)
        assert_listing_contract(store, ordered=False)
        assert_purge_contract(store)
        assert all(t.startswith(PREFIX + "_") for t in client.list_tables()["TableNames"])


# --- two prefixes, one database ---------------------------------------------------------------
def _advance(store, exe_id: str) -> None:
    exe = Execution(id=exe_id, definition_id="M")
    store.commit(exe, [(exe_id, Event(kind="Go"))], processed_event_id="e1")


def _isolated(a, b) -> None:
    """What `a` writes, `b` doesn't see — executions, outbox, dedupe — and vice versa."""
    _advance(a, "only-in-a")
    _advance(b, "only-in-b")
    assert a.load("only-in-b") is None and b.load("only-in-a") is None
    assert [e.target_id for e in a.pending_outbox()] == ["only-in-a"]
    assert [e.target_id for e in b.pending_outbox()] == ["only-in-b"]
    assert a.is_processed("only-in-a", "e1") and not b.is_processed("only-in-a", "e1")


def _transports_isolated(a, b) -> None:
    a.publish("G", Event(kind="for-a"))
    assert b.claim("w", 30) is None
    lease = a.claim("w", 30)
    assert lease is not None and lease.event.kind == "for-a"


def test_two_prefixes_share_one_sqlite_file(tmp_path):
    db = tmp_path / "shared.db"
    a, b = SqliteStore(db, prefix="app_a"), SqliteStore(db, prefix="app_b")
    _isolated(a, b)
    _transports_isolated(SqliteTransport(db, prefix="app_a"), SqliteTransport(db, prefix="app_b"))


def test_two_prefixes_share_one_redis():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.store import RedisStore
    from harel.engine.transport import RedisTransport

    server = fakeredis.FakeServer()
    client = lambda: fakeredis.FakeStrictRedis(server=server)  # noqa: E731
    _isolated(RedisStore(client(), prefix="app_a"), RedisStore(client(), prefix="app_b"))
    _transports_isolated(RedisTransport(client(), prefix="app_a"), RedisTransport(client(), prefix="app_b"))


def test_two_prefixes_share_one_mongo_database():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore
    from harel.engine.transport import MongoTransport

    client = mongomock.MongoClient()
    _isolated(MongoStore(client, prefix="app_a"), MongoStore(client, prefix="app_b"))
    _transports_isolated(MongoTransport(client, prefix="app_a"), MongoTransport(client, prefix="app_b"))


# --- create_schema=False ----------------------------------------------------------------------
def test_without_create_schema_nothing_is_created(tmp_path):
    db = tmp_path / "owned.db"
    store = SqliteStore(db, prefix=PREFIX, create_schema=False)
    SqliteTransport(db, prefix=PREFIX, create_schema=False).close()
    assert sqlite3.connect(db).execute("SELECT count(*) FROM sqlite_master").fetchone()[0] == 0
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        store.load("x")


def _migrated(db) -> None:
    conn = sqlite3.connect(db)
    for stmt in sql_schema("sqlite", PREFIX):  # what a migration tool would apply
        conn.execute(stmt)
    conn.commit()
    conn.close()


def test_without_create_schema_it_runs_on_the_schema_a_migration_applied(tmp_path):
    _migrated(tmp_path / "migrated.db")
    store = SqliteStore(tmp_path / "migrated.db", prefix=PREFIX, create_schema=False)
    assert_outbox_contract(store)
    store.close()
    n = iter(range(100))

    def migrated_transport(clock):
        db = tmp_path / f"t{next(n)}.db"
        _migrated(db)
        return SqliteTransport(db, clock=clock, prefix=PREFIX, create_schema=False)

    assert_transport_contract(migrated_transport)


def test_dynamodb_without_create_schema_needs_its_tables():
    moto = pytest.importorskip("moto")
    import boto3
    from botocore.exceptions import ClientError

    from harel.engine.store import DynamoDBStore

    with moto.mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        with pytest.raises(ClientError):
            DynamoDBStore(client, prefix=PREFIX, create_schema=False)
        DynamoDBStore(client, prefix=PREFIX)  # creates them
        DynamoDBStore(client, prefix=PREFIX, create_schema=False)  # and now they're there


def test_mongo_without_create_schema_creates_no_index():
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.transport import MongoTransport

    client = mongomock.MongoClient()
    MongoTransport(client, prefix=PREFIX, create_schema=False)
    assert "available_at_1" not in client["harel"][f"{PREFIX}_transport_locks"].index_information()
    MongoTransport(client, prefix=PREFIX)
    assert "available_at_1" in client["harel"][f"{PREFIX}_transport_locks"].index_information()


# --- configuration ----------------------------------------------------------------------------
def test_the_environment_sets_the_prefix_and_schema_ownership():
    assert Config.from_env({}).schema_kwargs() == {"prefix": "harel", "create_schema": True}
    cfg = Config.from_env({"HAREL_PREFIX": PREFIX, "HAREL_CREATE_SCHEMA": "false"})
    assert cfg.schema_kwargs() == {"prefix": PREFIX, "create_schema": False}


def test_the_mongo_upgrade_renames_reach_the_new_names():
    """The upgrade guide's Mongo renames (old collection -> new) are the names 0.7 reads."""
    mongomock = pytest.importorskip("mongomock")
    from harel.engine.store import MongoStore
    from harel.engine.transport import MongoTransport

    client = mongomock.MongoClient()
    db = client["harel"]
    # data written under a scratch prefix, then put where 0.6 kept it
    staged = MongoStore(client, prefix="staged")
    staged.commit(Execution(id="order-1", definition_id="order"), [("order-1", Event(kind="Go"))])
    MongoTransport(client, prefix="staged").publish("order-1", Event(kind="Paid"))
    for scratch, legacy in [
        ("staged_executions", "executions"),
        ("staged_counters", "counters"),
        ("staged_transport_messages", "stm_messages"),
        ("staged_transport_locks", "stm_locks"),
        ("staged_transport_counters", "stm_counters"),
    ]:
        db[scratch].rename(legacy)
    for legacy, new in [  # the guide's renames
        ("executions", "harel_executions"),
        ("counters", "harel_counters"),
        ("stm_messages", "harel_transport_messages"),
        ("stm_locks", "harel_transport_locks"),
        ("stm_counters", "harel_transport_counters"),
    ]:
        db[legacy].rename(new)
    store = MongoStore(client)
    assert store.load("order-1") is not None and [e.event.kind for e in store.pending_outbox()] == ["Go"]
    lease = MongoTransport(client).claim("w", 30)
    assert lease is not None and lease.event.kind == "Paid"
