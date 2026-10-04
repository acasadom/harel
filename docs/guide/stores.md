# Stores

A **store** (`ExecutionStore`) is how a runner persists and reloads an `Execution`. This is the
hub for the per-backend reference: the contract every backend implements, the durability shape
they share, the opt-in execution trace — and a page per backend with its **exact data model and
every operation** (what each table/key/document holds, how each update is done, and why). For the
concepts (the seam, surviving a restart, idempotency) start with [durability](durability); to
select a store at the worker see [`HAREL_STORE_BACKEND`](distribution).

## The contract every backend implements

`ExecutionStore` is a small `Protocol` ([`store/_base.py`](https://github.com/acasadom/harel/blob/main/src/harel/engine/store/_base.py)) —
each backend is a sibling module under `harel/engine/store/`, re-exported from the package, with
a twin under `harel/engine/aio_store/` (same data model, awaited IO):

| Method | Purpose |
| --- | --- |
| `load(id)` / `load_for_event(id, event_id)` | rehydrate the Execution (the latter folds the dedupe check into the same round-trip) |
| `save(exe)` | persist with the version CAS (raises `StoreConflict` on a stale write) |
| `commit(exe, emits, processed_event_id, timers, spawns, trace)` | the **one atomic write per event** — the Execution + outbox + dedupe + timer ops + spawn intents + (opt-in) trace step, all-or-nothing; returns the `seq` of each outbox entry it enqueued, in `emits` order |
| `is_processed(id, event_id)` | dedupe lookup |
| `pending_outbox()` / `ack_outbox(seq)` | the relay drains emitted events (a caller that delivers an entry itself acks it by the `seq` `commit` returned) |
| `pending_spawns()` / `ack_spawn(seq)` | the relay drains orthogonal child-creation intents |
| `due_timers(now)` / `delete_timer(id, path, fire_at)` | the timer sweep |
| `read_trace(id)` / `append_trace(id, entry)` | the opt-in execution timeline (see below) |
| `list_executions(...)` | a page of lightweight summaries, for the monitor and `purge_finished` |
| `ids_with_prefix(prefix)` | ids of the stored Executions starting with `prefix` — how `purge` finds every descendant of a tree (a child's id extends its parent's), including those the parent no longer lists |
| `purge(id, expected_version)` | permanently delete one Execution and every row keyed by it (dedupe, trace, timers, outbox entries addressed to it, spawn intents it issued), iff still at `expected_version` — see [purge](control-plane.md#purge) |
| `close()` | release the connection/client |

Everything `commit` writes lives in **one transaction** (or one atomic request), which is the
property that makes a crash safe — see [durability](durability). The common shape across the
durable backends, which each per-backend page then spells out exactly:

- **CAS** — a `version` per record; the write applies only if the stored version still matches,
  else `StoreConflict`. This is the single-writer-per-execution backstop.
- **Outbox / spawns** — emitted events and region-spawn intents are persisted *with* the advance
  and delivered by the relay afterwards, so a crash never loses a `Finished` or a fork.
- **Dedupe** — `processed_events` (id + event id) makes at-least-once delivery effect-once.
- **Timers** — `(execution_id, path) → fire_at`, armed/cancelled in the same commit.

## Writing your own backend

A store outside harel — over a framework's ORM, a database harel doesn't ship — builds on
`harel.engine.store.base`, the stable API for backend authors: the protocols
(`ExecutionStore`, `AsyncExecutionStore`), the records (`OutboxEntry`, `SpawnEntry`, `TimerOp`,
`ExecutionSummary`, `ExecutionPage`), the errors (`StoreConflict`, `ExecutionAlreadyExists`) and
the helpers the built-in backends share (`encode_offset`/`decode_offset` for the listing cursor,
`matches`, `listing_page`, `like_prefix`).

`harel.testing` holds the contracts harel runs on its own backends; run them on yours, one call
each:

```text
from harel.testing import assert_listing_contract, assert_outbox_contract, assert_purge_contract

def test_my_store():
    assert_listing_contract(MyStore(), ordered=True)   # ordered=False for an unordered scan
    assert_purge_contract(MyStore())
    assert_outbox_contract(MyStore())
```

Each has an `assert_async_*` twin for an async store, and takes `ns=` to prefix the ids it seeds
on a backend shared with other data. When the protocol changes, they are what tells you.

## Execution trace (opt-in)

When tracing is on (`HAREL_TRACE=1`, or `DurableRunner(..., trace=True)`), `commit` also appends
one **timeline step** — event in, transition `from → to`, the actions that ran, what a `set`
wrote (`assigned`, only when there was one), and the resulting `context_out` — in the *same*
transaction as the advance. It is **off by default**, so
the hot path pays nothing; when on it costs ~one extra in-transaction write (~+10 µs/commit on a
local SQLite, no extra round-trip or fsync), and `load` is unaffected (it still reads the
snapshot — there is no event replay). Each backend keeps a **ring of the last `HAREL_TRACE_MAX`
steps** (default 200) using its natural primitive (a capped table, an `LTRIM`'d list, a
`$slice`'d array, a Put+Delete window); the monitor renders it as the [timeline](monitor). Only
`context_out` is stored (the monitor derives each step's `context_in` from the previous step).

```python
from harel import definition_from_dsl, DurableRunner, SqliteStore, Event

defn = definition_from_dsl(
    "event Go {} machine m { initial A  state A {}  final Done success {}  from A to Done on Go }", "m"
)
store = SqliteStore(":memory:")
runner = DurableRunner(store, {defn.id: defn}, trace=True)   # opt-in

exe = runner.create(defn.id)
runner.process(exe.id, Event(kind="Go"))
for step in store.read_trace(exe.id):
    print(step["index"], step["event_kind"], step["from_path"], "->", step["to_path"])
```

```text
0 Start None -> A
1 Go A -> Done
```

## The backends

Each backend has its own page with the full schema/key-space and every operation broken down with
the real SQL/commands and the reasoning:

```{toctree}
:maxdepth: 1

stores/dict
stores/sqlite
stores/libsql
stores/redis
stores/postgres
stores/rqlite
stores/mongo
stores/dynamodb
```

| Backend | For | CAS mechanism |
| --- | --- | --- |
| [DictStore](stores/dict) | tests, embedding (in-memory, not durable) | object identity |
| [SqliteStore](stores/sqlite) | one machine / shared volume, zero infra | `UPDATE … WHERE version=old` |
| [LibsqlStore](stores/libsql) | libSQL/Turso — file, `sqld`, or embedded replica *(experimental)* | `UPDATE … WHERE version=old` |
| [RedisStore](stores/redis) | all-network, pairs with `RedisTransport` | atomic Lua CAS (state-only fast path); `WATCH`/`MULTI`/`EXEC` for complex commits |
| [PostgresStore](stores/postgres) | distributed SQL | `UPDATE … WHERE version=old` (row lock); state-only commit folds into one `harel_commit_cas` round-trip |
| [RqliteStore](stores/rqlite) | HA SQLite over Raft (HTTP) | guarded upsert, one request |
| [MongoStore](stores/mongo) | document store, single-document atomic | `update_one({_id, version})` |
| [DynamoDBStore](stores/dynamodb) | AWS serverless, pairs with `SqsTransport` | conditional write + `TransactWriteItems` |

## Naming and schema ownership

Every persistent backend takes a **`prefix`** (default `"harel"`) and names everything it creates
with it, so several harel deployments — or harel and other applications — share one database
without colliding:

| backend | what `prefix="harel"` names |
|---|---|
| SQLite, libSQL, rqlite, Postgres | tables `harel_executions`, `harel_outbox`, `harel_processed_events`, `harel_spawns`, `harel_timers`, `harel_trace`; the transport's `harel_transport_messages`, `harel_transport_groups`; their indexes; Postgres' functions `harel_commit_cas`, `harel_claim`, `harel_ack` |
| Redis | every key: `harel:exe:<id>`, `harel:outbox`, …, the transport's `harel:q:<group>`, … |
| Mongo | collections (under `db_name`): `harel_executions`, `harel_counters`; the transport's `harel_transport_messages`, `harel_transport_locks`, `harel_transport_counters` |
| DynamoDB | tables `harel_executions`, `harel_outbox`, `harel_spawns`, `harel_timers`, `harel_processed`, `harel_counters`, `harel_trace` |

A prefix is an identifier — a letter or `_`, then letters, digits or `_` — of at most 30
characters (so every name built from it fits Postgres' 63). `harel.engine.schema.Names(prefix)`
gives each name.

Every backend that has a schema also takes **`create_schema`** (default `True`): it creates its
tables, indexes and functions when it is built, idempotently. With `create_schema=False` it
creates nothing and expects them to be there — when something else owns the schema: a migration
tool, infrastructure-as-code. For the SQL backends the schema is data:

```python
from harel.engine.schema import SCHEMA_VERSION, sql_schema

statements = sql_schema("postgres", prefix="harel")  # or "sqlite" (SQLite, libSQL, rqlite)
assert all(s.startswith("CREATE") for s in statements)
```

`sql_schema` returns the statements the backends run themselves — the store's and the
transport's (`store_schema` / `transport_schema` for one of them) — each idempotent (`IF NOT
EXISTS`, `CREATE OR REPLACE`). Hand them to the migration tool, and build the backends with
`create_schema=False`. `SCHEMA_VERSION` changes whenever the schema does, and the CHANGELOG says
how to move from one version to the next. The worker reads both settings from `HAREL_PREFIX` and
`HAREL_CREATE_SCHEMA` (`"0"`/`"false"` to turn it off).

Upgrading a deployment from before 0.7, whose tables had no prefix: see
[upgrading to 0.7](upgrading).

## Async ports

Every store has an `Async…` twin under `harel/engine/aio_store/` with the **same data model** —
the async worker (`HAREL_CONCURRENCY` events in flight on one loop) talks to those. Most are native
async (aiosqlite, redis.asyncio, psycopg async pool, motor, aioboto3, httpx for rqlite);
`AsyncLibsqlStore` wraps the sync driver on a thread (the `libsql` package is sync-only). The
synchronous `Store` classes are what the sync `DurableRunner`/`DistributedRunner` and the monitor
use — bridged to the async core through the anyio portal by default, or called directly in the
caller's thread with `execution="inline"` (see [execution models](execution)). Each per-backend page has an
*Async twin* section with the specifics.
