# Stores — backend reference

A **store** (`ExecutionStore`) is how a runner persists and reloads an `Execution`. This page is
the detailed, per-backend reference: what each one is for and **how it actually lays the data
out**. For the concepts (the seam, the durability guarantees, surviving a restart) start with
[durability](durability); for choosing a store at the worker, see the
[`STM_STORE_BACKEND`](distribution) env var.

## The contract every backend implements

`ExecutionStore` is a small `Protocol` ([`store/_base.py`](../../src/harel/engine/store/_base.py)) —
each backend is a sibling module under `harel/engine/store/`, re-exported from the package, with
a native-async twin under `harel/engine/aio_store/` (same data model, awaited IO):

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
property that makes a crash safe — see [durability](durability). The
common shape across the durable backends:

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
steps** (default 200) using its natural primitive; the monitor renders it as the
[timeline](monitor). Every backend below records it; only `context_out` is stored (the monitor
derives each step's `context_in` from the previous step).

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

---

## `DictStore` — in-memory

The default for tests and embedding. A plain dict of `id → Execution` plus in-memory lists for
the outbox, spawns, timers and trace. It returns the **same object** it was handed (no
serialization round-trip), so a caller holding a reference sees mutations. Not durable — state
dies with the process. The CAS only bites if a *different* object is stored under the same id (a
genuine concurrent writer in a test). Trace is a capped Python list with a per-id monotonic index.

## `SqliteStore` — one machine, zero infrastructure

A durable file (or `:memory:`) over stdlib `sqlite3`, WAL mode. The whole `commit` is one SQLite
transaction. Tables:

```text
executions(id PK, definition_id, data TEXT, version)     -- data = the Execution JSON
outbox(seq PK AUTOINCREMENT, target_id, event)
processed_events(execution_id, event_id, PK(both))
timers(execution_id, path, fire_at, PK(both))
spawns(seq PK AUTOINCREMENT, parent_id, child_id, root_path, context)
trace(execution_id, idx, entry, PK(both))                -- the opt-in timeline
```

- **CAS** — `UPDATE executions SET data=?, version=old+1 WHERE id=? AND version=old`; `rowcount==0`
  means either a brand-new row (`INSERT` when `old==0`) or a stale write (`StoreConflict`).
- **Trace ring** — two statements in the commit txn: `INSERT … SELECT MAX(idx)+1` (the index is
  computed inline, no pre-read) then a `DELETE … WHERE idx <= MAX-trace_max`; `read_trace` takes
  `index` from the `idx` column.
- `WAL` + `busy_timeout` let readers (the monitor) not block the single writer. Pick it for a
  single machine or a shared volume; for many machines across hosts use a networked backend.

## `LibsqlStore` — libSQL / Turso *(experimental)*

libSQL is Turso's SQLite fork; the `libsql` driver is DB-API compatible, so the SQL, the CAS and
the one-transaction `commit` are **identical to `SqliteStore`** (same six tables, same trace
ring). What it adds is *where the database lives*, chosen by constructor args:

- a **local file** — like SQLite;
- an **embedded replica** (`sync_url=` + `auth_token=`) — local reads from a file, writes routed
  to a Turso/`sqld` primary and synced back;

so one backend is both a single-file embed and a distributed (Turso) store. **Experimental:** the
local-file path is covered in-process by the test suite; the Turso/`sqld` path is wired but not
yet validated against a real account, and primary-follower replication is eventually consistent
(expect extra `StoreConflict` retries, or read from the primary for the CAS).

## `RedisStore` — all-network, no shared filesystem

A durable store over Redis — the natural partner of `RedisTransport` for a pure-Redis stack. The
client is injected (so `redis` is an optional extra; tests use fakeredis). Keys under `prefix`:

```text
exe:{id}            -> the Execution JSON
outbox              -> hash {seq -> {t: target_id, e: event}}      outbox:seq -> INCR counter
spawns              -> hash {seq -> {...}}                          spawns:seq -> INCR counter
processed:{id}      -> set of handled event ids
timers              -> ZSET member "{id}\x00{path}" scored by fire_at
trace:{id}          -> list (RPUSH) of step JSON   trace:seq:{id} -> per-execution INCR index
```

- **CAS** — `commit` is one `WATCH`/`MULTI`/`EXEC` on the `exe:{id}` key: the version is checked
  under `WATCH`, all writes go in one `EXEC`; a concurrent change → `StoreConflict` (no Lua, so
  fakeredis supports it). Monotonic seqs are allocated with `INCR` *before* the `MULTI` (a seq
  wasted by an aborted txn is harmless).
- **Trace ring** — `RPUSH` the step then `LTRIM trace:{id} -trace_max -1` inside the `MULTI`; the
  index is a per-execution `INCR`.
- `list_executions` is a `SCAN` of `exe:*` + client-side filter (Redis can't query inside a
  value), so its order is best-effort.

## `PostgresStore` — distributed SQL

A real SQL server for state shared across machines. Same table shape as SQLite (`INT`/`BIGSERIAL`
types). The connection is injected (`psycopg` optional). `commit` is one Postgres transaction:

- **CAS** — a plain `UPDATE … WHERE version=old`; Postgres row-locks serialize concurrent
  writers, so exactly one wins (`rowcount 1`) and the loser (`rowcount 0`) raises `StoreConflict`
  — no app-level locking.
- **Trace ring** — `INSERT … SELECT MAX(idx)+1` + the cap `DELETE`, in the commit txn (same as
  SQLite, `%s` placeholders).
- `from_dsn(...)` retries the connection so a worker starting alongside Postgres in compose waits
  rather than crashing. The async twin (`AsyncPostgresStore`) uses a `psycopg_pool` async pool so
  concurrent workers issue real parallel requests.

## `RqliteStore` — distributed SQLite over Raft

rqlite is distributed SQLite with Raft consensus, spoken over HTTP. Same logical schema as
SQLite, but rqlite has **no interactive (multi-round-trip) transaction** — so `commit` is **one
transactional request** whose writes are all *guarded on the CAS succeeding*:

- the Execution upsert applies only `… WHERE version=old`;
- every side-write (outbox, dedupe, timers, spawns, trace) runs only
  `… WHERE EXISTS (SELECT 1 FROM executions WHERE id=? AND data=?)` — i.e. *iff our exact write
  won the CAS*. A version mismatch makes the whole request a no-op, detected by the upsert's
  `rows_affected == 0` (→ `StoreConflict`).
- **Trace** — the step is an `INSERT … SELECT MAX(idx)+1 … WHERE EXISTS(…)` and the cap a guarded
  `DELETE`, both in the one request. Reads use `level=strong` (linearizable, via the leader).

Strong durability (Raft replicates every write); correspondingly the slowest backend — consensus
+ HTTP + fsync per write. Pick it when you want HA SQLite without running Postgres.

## `MongoStore` — document store

The document-store alternative, all-network. MongoDB has no multi-document transaction without a
replica set, so **everything for one Execution lives in its single document** and `commit` is one
atomic `update_one`:

```text
executions/{_id: id}: { data, version, outbox:[…], spawns:[…], processed:[…],
                        timers:{enc(path): fire_at}, trace:[…] }
counters: the monotonic outbox/spawn/trace seq allocator
```

- **CAS** — `update_one({_id, version: old}, {...})`; `matched_count==0` → brand-new (`insert_one`)
  or `StoreConflict`. Writes are **partial** (`$set` data/version, `$push` to the arrays,
  `$addToSet` dedupe, `$set`/`$unset` timers) — never a full `replace_one`, and reads are
  projected, so a growing `data` blob isn't dragged through every queue/timer scan.
- **Trace ring** — a native `$push` with `$slice: -trace_max` on the embedded `trace` array; the
  index is a per-execution counter. (Path keys encode `.` → `．` since Mongo treats `.` as a path
  operator.) Tests use mongomock; the async twin uses motor.

## `DynamoDBStore` — AWS serverless

The serverless backend and the natural store-side partner of `SqsTransport` for an all-AWS,
no-server stack. Runs against real DynamoDB or LocalStack/moto. DynamoDB gives the two primitives
directly: **conditional writes** are the CAS and **`TransactWriteItems`** makes `commit` atomic
across items. Tables (prefixed): `executions(id)`, `outbox`/`spawns(seq)`, `timers`/`processed`
(composite keys), `counters` (the seq allocator), and `trace(execution_id, idx)`.

- **CAS** — the Execution `Put` carries `attribute_not_exists(id)` (insert) or `version=:old`
  (update); a failed condition cancels the whole transaction (`TransactionCanceledException`).
- **Trace ring** — a `Put idx=K` plus a `Delete idx=K-N` in the *same* `TransactWriteItems`
  (deleting an absent key is a no-op). `idx` is `query(MAX)+1` (newest-first, `Limit 1`) — so it
  is contiguous per execution and a cancelled commit leaves no gap. **Caveat:** because the ring
  drops exactly one item per write, it keeps exactly the last N **only if `trace_max` is fixed
  from the first traced commit** (the production case — set once at startup); changing it
  mid-stream over-retains harmlessly until items age out.
- The relay/sweep reads (`pending_outbox`/`due_timers`) use `Scan` + a client-side sort (those
  tables drain and stay near-empty). The async twin uses native-async aioboto3.

## Async ports

Every store has an `Async…` twin under `harel/engine/aio_store/` with the **same data model** —
the async worker (`STM_CONCURRENCY` events in flight on one loop) talks to those. Most are native
async (aiosqlite, redis.asyncio, psycopg async pool, motor, aioboto3, httpx for rqlite);
`AsyncLibsqlStore` wraps the sync driver on a thread (the `libsql` package is sync-only). The
synchronous `Store` classes shown here are what the embedded `DurableRunner`/`DistributedRunner`
façades and the monitor use, bridged to the async core through the anyio portal.
