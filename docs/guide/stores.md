# Stores

A **store** (`ExecutionStore`) is how a runner persists and reloads an `Execution`. This is the
hub for the per-backend reference: the contract every backend implements, the durability shape
they share, the opt-in execution trace — and a page per backend with its **exact data model and
every operation** (what each table/key/document holds, how each update is done, and why). For the
concepts (the seam, surviving a restart, idempotency) start with [durability](durability); to
select a store at the worker see [`STM_STORE_BACKEND`](distribution).

## The contract every backend implements

`ExecutionStore` is a small `Protocol` ([`store/_base.py`](https://github.com/acasadom/harel/blob/main/src/harel/engine/store/_base.py)) —
each backend is a sibling module under `harel/engine/store/`, re-exported from the package, with
a twin under `harel/engine/aio_store/` (same data model, awaited IO):

| Method | Purpose |
| --- | --- |
| `load(id)` / `load_for_event(id, event_id)` | rehydrate the Execution (the latter folds the dedupe check into the same round-trip) |
| `save(exe)` | persist with the version CAS (raises `StoreConflict` on a stale write) |
| `commit(exe, emits, processed_event_id, timers, spawns, trace)` | the **one atomic write per event** — the Execution + outbox + dedupe + timer ops + spawn intents + (opt-in) trace step, all-or-nothing |
| `is_processed(id, event_id)` | dedupe lookup |
| `pending_outbox()` / `ack_outbox(seq)` | the relay drains emitted events |
| `pending_spawns()` / `ack_spawn(seq)` | the relay drains orthogonal child-creation intents |
| `due_timers(now)` / `delete_timer(id, path, fire_at)` | the timer sweep |
| `read_trace(id)` / `append_trace(id, entry)` | the opt-in execution timeline (see below) |
| `list_executions(...)` | a page of lightweight summaries for the monitor |
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

## Execution trace (opt-in)

When tracing is on (`STM_TRACE=1`, or `DurableRunner(..., trace=True)`), `commit` also appends
one **timeline step** — event in, transition `from → to`, the actions that ran, and the
resulting `context_out` — in the *same* transaction as the advance. It is **off by default**, so
the hot path pays nothing; when on it costs ~one extra in-transaction write (~+10 µs/commit on a
local SQLite, no extra round-trip or fsync), and `load` is unaffected (it still reads the
snapshot — there is no event replay). Each backend keeps a **ring of the last `STM_TRACE_MAX`
steps** (default 200) using its natural primitive (a capped table, an `LTRIM`'d list, a
`$slice`'d array, a Put+Delete window); the monitor renders it as the [timeline](monitor). Only
`context_out` is stored (the monitor derives each step's `context_in` from the previous step).

```python
from harel import definition_from_dsl, DurableRunner, SqliteStore, Event

defn = definition_from_dsl(
    "machine m { initial A  state A {}  final Done success {}  from A to Done on Go }", "m"
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
| [RedisStore](stores/redis) | all-network, pairs with `RedisTransport` | `WATCH`/`MULTI`/`EXEC` |
| [PostgresStore](stores/postgres) | distributed SQL | `UPDATE … WHERE version=old` (row lock) |
| [RqliteStore](stores/rqlite) | HA SQLite over Raft (HTTP) | guarded upsert, one request |
| [MongoStore](stores/mongo) | document store, single-document atomic | `update_one({_id, version})` |
| [DynamoDBStore](stores/dynamodb) | AWS serverless, pairs with `SqsTransport` | conditional write + `TransactWriteItems` |

## Async ports

Every store has an `Async…` twin under `harel/engine/aio_store/` with the **same data model** —
the async worker (`STM_CONCURRENCY` events in flight on one loop) talks to those. Most are native
async (aiosqlite, redis.asyncio, psycopg async pool, motor, aioboto3, httpx for rqlite);
`AsyncLibsqlStore` wraps the sync driver on a thread (the `libsql` package is sync-only). The
synchronous `Store` classes are what the embedded `DurableRunner`/`DistributedRunner` façades and
the monitor use, bridged to the async core through the anyio portal. Each per-backend page has an
*Async twin* section with the specifics.
