# PostgresStore — distributed SQL

`PostgresStore` is the durable `ExecutionStore` backend for the **distributed-SQL deployment**:
a real PostgreSQL server holding the statechart state, **shared across many machines** with no
filesystem to mount and no single-host bottleneck. It is the same shape as
[`SqliteStore`](sqlite) — version/CAS, transactional outbox, dedupe, durable timers, spawn
outbox, the optional trace ring — but over a networked engine that several worker hosts can hit
at once. Use it when you have run out of "one box" and want a battle-tested SQL server (often the
one you already operate) to be the single source of truth.

A few defining properties:

- **The connection is injected (duck-typed)**, so `psycopg` is an *optional* extra. `__init__`
  takes whatever `conn` you hand it and calls `conn.cursor()` / `conn.commit()` / `conn.rollback()`
  on it. The convenience constructor `from_dsn` imports `psycopg` lazily.
- **The whole `commit` is ONE Postgres transaction.** The CAS write of the Execution *and* every
  side effect of that step (outbox events, the dedupe marker, spawn intents, timer arms/disarms,
  the optional trace step) run on one cursor and land with a single `self._conn.commit()`. Either
  all of it commits or none of it does — that all-or-nothing property is what makes a crash safe.
  See [durability](../durability).
- **CAS is a version compare-and-swap** — there is **no app-level locking**. Postgres row-locks
  serialize concurrent writers to the same Execution row: exactly one writer wins the version CAS,
  and any writer that loaded the same old version and lost the race raises `StoreConflict`. The
  database does the serialization; the code does not. There are **two paths**: a **state-only
  commit** (no emits/spawns/timers/trace, the common case) takes a fast path — the server-side
  `harel_commit_cas` PL/pgSQL function does the version-CAS + write (+ dedupe) in **ONE round-trip**;
  a **complex commit** keeps the multi-statement `UPDATE ... WHERE version = old` path. See
  [the commit section](#the-cas-commit) below.
- **`from_dsn` retries the connection.** A worker container that starts alongside Postgres (e.g. in
  a Docker Compose stack) will often come up before Postgres accepts connections. `from_dsn` retries
  the connect (15 attempts, 1 s apart by default) so the worker **waits** for Postgres rather than
  crashing on startup.

```text
from harel.engine.store import PostgresStore

store = PostgresStore.from_dsn("postgresql://stm:stm@db:5432/stm")
# or inject your own psycopg connection:
#   store = PostgresStore(my_conn)
```

## Schema

Six tables, all created with `CREATE TABLE IF NOT EXISTS` in `__init__` (on one cursor) and then a
single `conn.commit()`. `__init__` also creates the `harel_commit_cas` PL/pgSQL function (the
fast-path commit), idempotently and under a `pg_advisory_xact_lock` so several workers opening
connections at once don't collide on `CREATE OR REPLACE FUNCTION` (which rewrites `pg_proc`). `data`
everywhere is `exe.model_dump_json()` — the whole Execution as JSON text — and a few scalars are
denormalized out of it so the hot paths (CAS, listing) never parse JSON.

### `executions` — the durable state

```text
CREATE TABLE IF NOT EXISTS executions
(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INT NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | TEXT, **PRIMARY KEY** | the `Execution.id` (a uuid hex). One row per Execution. |
| `definition_id` | TEXT NOT NULL | the id of the `Definition` this Execution runs — a broken-out column so `list_executions` can filter by it without parsing the blob. |
| `data` | TEXT NOT NULL | the **full Execution serialized** — `exe.model_dump_json()`. Holds everything: `status`, `outcome`, `error`, `active_path`, `history`, `context`, `children` (the join counter), `parent_id`/`child_id`, `invoke_seq`, `definition_fqn`, and `version`. Treated as an opaque blob except where the listing path casts it to `jsonb` and reaches in with `->>` for the summary projection. |
| `version` | INT NOT NULL | the **CAS token** — a broken-out copy of `Execution.version`. A write succeeds only if the stored `version` still equals the one the Execution was loaded at; the CAS compares this column (not the JSON) so it is a cheap indexed scalar. |

### `outbox` — the transactional event outbox

```text
CREATE TABLE IF NOT EXISTS outbox
(seq BIGSERIAL PRIMARY KEY, target_id TEXT, event TEXT NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `seq` | **BIGSERIAL** PRIMARY KEY | monotonic delivery sequence; Postgres assigns it. `pending_outbox` orders by it; `ack_outbox(seq)` deletes the delivered row. |
| `target_id` | TEXT (nullable) | the Execution to deliver the event to; `NULL` means no target (a broadcast/no-target emit). |
| `event` | TEXT NOT NULL | the `Event`, `event.model_dump_json()`. |

The outbox is what makes a `Finished` (or any emitted event) crash-safe: the event is written **in
the same transaction** as the state advance, so it cannot be lost after a successful commit. A relay
delivers it afterwards and acks.

### `processed_events` — the dedupe ledger

```text
CREATE TABLE IF NOT EXISTS processed_events
(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | part of the composite PK — the Execution that consumed the event. |
| `event_id` | TEXT NOT NULL | part of the composite PK — the `Event.id` already handled. |

The composite PK `(execution_id, event_id)` is the dedupe under at-least-once delivery: a redelivery
of an already-processed event hits the PK and is ignored (the insert uses `ON CONFLICT DO NOTHING`).

### `timers` — durable timers

```text
CREATE TABLE IF NOT EXISTS timers
(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at DOUBLE PRECISION NOT NULL,
 PRIMARY KEY (execution_id, path))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | part of the PK — the Execution the timer belongs to. |
| `path` | TEXT NOT NULL | part of the PK — the state path that armed the timer. |
| `fire_at` | **DOUBLE PRECISION** NOT NULL | the epoch time the timer is due (a float). `due_timers(now)` selects `fire_at <= now`. |

The PK `(execution_id, path)` means a state has at most one armed timer; re-arming the same path
**replaces** it (the upsert below). Floats are stored as `DOUBLE PRECISION` so the comparison is exact.

### `spawns` — the orthogonal-fork outbox

```text
CREATE TABLE IF NOT EXISTS spawns
(seq BIGSERIAL PRIMARY KEY, parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
 root_path TEXT NOT NULL, context TEXT NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `seq` | **BIGSERIAL** PRIMARY KEY | monotonic; `pending_spawns` orders by it, `ack_spawn(seq)` deletes. |
| `parent_id` | TEXT NOT NULL | the forking parent Execution. |
| `child_id` | TEXT NOT NULL | the deterministic id of the region child to create. |
| `root_path` | TEXT NOT NULL | the region's root path inside the shared Definition. |
| `context` | TEXT NOT NULL | the child's seed context, `json.dumps(context)`. |

Like the outbox, but for **child-Execution creations** (an orthogonal fork). A spawn intent commits
in the **same transaction** as the parent's advance + join expectations (its `children` dict in
`data`), so the fork is atomic and crash-safe; a relay later creates each child idempotently.

### `trace` — the optional execution-trace ring

```text
CREATE TABLE IF NOT EXISTS trace
(execution_id TEXT NOT NULL, idx INT NOT NULL, entry TEXT NOT NULL,
 PRIMARY KEY (execution_id, idx))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | part of the PK — the Execution being traced. |
| `idx` | **INT** NOT NULL | part of the PK — the step index (monotonic per execution); doubles as the stable `index` returned by `read_trace`. |
| `entry` | TEXT NOT NULL | one trace step, `json.dumps(entry)` (event/transition/actions/context_out). |

Opt-in (off by default); a ring capped at `trace_max` steps per execution — see
[`_write_trace`](#the-trace-ring-_write_trace).

(the-cas-commit)=
## The CAS + `commit`

`commit` is the heart of the store. It has **two paths**, picked by what the commit carries:

- **The fast path — a state-only commit** (no emits, spawns, timers or trace, the common case)
  calls `_commit_cas`, which runs the server-side `harel_commit_cas(id, defn, data, old, event)`
  PL/pgSQL function: the version-CAS, the write, and the optional dedupe in **ONE round-trip**. The
  function **returns `false`** on a version conflict — it does **not** `RAISE`, so the transaction
  stays clean; the caller rolls back and raises `StoreConflict` itself. See
  [`_commit_cas`](#pg-fast-path-commit-cas) below.
- **The complex path — anything to enqueue** (emits / spawns / timers / trace) takes the
  multi-statement body shown next.

```text
def commit(self, exe, emits, processed_event_id=None, timers=(), spawns=(), trace=None):
    if not emits and not spawns and not timers and trace is None:
        self._commit_cas(exe, processed_event_id)   # fast path: one round-trip via harel_commit_cas
        return
    # ... otherwise the multi-statement path below
```

The rest of this section walks the **complex path**. It bumps the version, serializes the
Execution, then runs the CAS and every side effect on one cursor and commits once. Here is the body
in order:

```text
old = exe.version
exe.version = old + 1
data = exe.model_dump_json()
try:
    with self._conn.cursor() as cur:
        cur.execute(
            "UPDATE executions SET data = %s, version = %s WHERE id = %s AND version = %s",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            cur.execute("SELECT version FROM executions WHERE id = %s", (exe.id,))
            row = cur.fetchone()
            if row is None and old == 0:
                cur.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (%s, %s, %s, %s)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old
                self._conn.rollback()
                raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
        for target_id, event in emits:
            cur.execute(
                "INSERT INTO outbox (target_id, event) VALUES (%s, %s)",
                (target_id, event.model_dump_json()),
            )
        if processed_event_id is not None:
            cur.execute(
                "INSERT INTO processed_events (execution_id, event_id) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING",
                (exe.id, processed_event_id),
            )
        for child_id, root_path, context in spawns:
            cur.execute(
                "INSERT INTO spawns (parent_id, child_id, root_path, context) "
                "VALUES (%s, %s, %s, %s)",
                (exe.id, child_id, root_path, json.dumps(context)),
            )
        for op in timers:
            if op.action == "schedule":
                cur.execute(
                    "INSERT INTO timers (execution_id, path, fire_at) VALUES (%s, %s, %s) "
                    "ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at",
                    (exe.id, op.path, op.fire_at),
                )
            else:
                cur.execute(
                    "DELETE FROM timers WHERE execution_id = %s AND path = %s", (exe.id, op.path)
                )
        if trace is not None:
            self._write_trace(cur, exe.id, trace)
    self._conn.commit()
except StoreConflict:
    raise
except Exception:
    exe.version = old
    self._conn.rollback()
    raise
```

Step by step, and why:

1. **Bump and serialize.** `old` is the version the Execution was loaded at; the new value is
   `old + 1`. `data = exe.model_dump_json()` snapshots the whole object once.
2. **The CAS write.** The `UPDATE ... WHERE id = %s AND version = %s` is the optimistic-concurrency
   check *and* the write in one statement. It matches the row only if the stored `version` is still
   `old`. **Postgres takes a row lock on the matched row**, so two workers racing to advance the same
   Execution are serialized by the database: the first commits and moves `version` to `old + 1`; the
   second's `WHERE version = old` no longer matches, so it updates **zero** rows. This is why no
   application-level lock is needed — row-lock serialization makes the CAS race-free.
3. **The `rowcount == 0` branch.** Zero rows updated means either the row does not exist yet (a first
   save) or someone else moved the version (we lost). We `SELECT version` to disambiguate:
   - `row is None and old == 0` → the row genuinely does not exist and this is the initial save, so
     **INSERT** it.
   - otherwise → a real conflict (the row exists at a different version, or it vanished after we
     thought it existed). Restore `exe.version = old`, **rollback**, and raise
     `StoreConflict(exe.id, expected=old, found=row[0] if row else None)` so the caller can reload
     and retry or drop the stale work.
4. **Outbox INSERTs.** Each emitted `(target_id, event)` is inserted into `outbox` so it is durable
   before delivery.
5. **Dedupe marker.** If a `processed_event_id` was given, `INSERT ... ON CONFLICT DO NOTHING`
   records that this Execution consumed that event; a redelivery later finds it and is skipped.
6. **Spawn INSERTs.** Each `(child_id, root_path, context)` fork intent goes into `spawns` — atomic
   with the parent's join expectations (which live in `data`).
7. **Timer ops.** `schedule` upserts the timer
   (`INSERT ... ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at` — re-arming
   a path replaces its `fire_at`); `cancel` does a `DELETE FROM timers WHERE execution_id = %s AND
   path = %s`.
8. **Trace.** If a `trace` step was passed, `_write_trace` appends it **on the same cursor** — inside
   this transaction, no extra round-trip.
9. **Commit once.** `self._conn.commit()` makes the whole bundle durable atomically.
10. **Error handling.** A `StoreConflict` (raised above, already rolled back) is re-raised as-is. Any
    other exception restores `exe.version = old` (so the in-memory object is consistent with what is
    persisted), rolls back the transaction, and re-raises — nothing partial is left behind.

`save(exe)` is just `commit(exe, [])` — the same CAS path with no side effects (a state-only
commit, so it takes the fast path below).

(pg-fast-path-commit-cas)=
## The fast-path commit (`_commit_cas`)

A state-only event — one that only advances the Execution — skips the multi-statement body above
for **one round-trip** to the server-side `harel_commit_cas` function:

```text
def _commit_cas(self, exe, processed_event_id):
    old = exe.version
    exe.version = old + 1
    with self._conn.cursor() as cur:
        cur.execute(
            "SELECT harel_commit_cas(%s, %s, %s, %s, %s)",
            (exe.id, exe.definition_id, exe.model_dump_json(), old, processed_event_id or ""))
        ok = cur.fetchone()[0]
    if not ok:                       # version conflict: the function returned false (no RAISE)
        exe.version = old
        self._conn.rollback()        # txn is clean, so the rollback is cheap
        raise StoreConflict(exe.id, expected=old, found=None)
    self._conn.commit()
```

The function folds the whole CAS into one server-side call: the `UPDATE ... WHERE version = old`,
the insert-or-conflict disambiguation (a first save inserts; a real conflict returns `false`), and
the optional dedupe `INSERT ... ON CONFLICT DO NOTHING`. The crucial detail is that on a version
conflict it **returns `false` rather than `RAISE`ing** — so the transaction is never aborted by the
function, the caller's `rollback()` is a clean no-op rollback, and `_commit_cas` raises
`StoreConflict` itself. This keeps the fast path's conflict handling identical in semantics to the
multi-statement path, just in one round-trip.

## The trace ring (`_write_trace`)

The trace is an opt-in, capped (ring) timeline of execution steps. `_write_trace` runs on the
cursor it is handed, so when called from `commit` it is part of **commit's transaction**:

```text
cur.execute(
    "INSERT INTO trace (execution_id, idx, entry) "
    "SELECT %s, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = %s), -1) + 1, %s",
    (execution_id, execution_id, json.dumps(entry)),
)
if self.trace_max:
    cur.execute(
        "DELETE FROM trace WHERE execution_id = %s AND idx <= "
        "(SELECT MAX(idx) FROM trace WHERE execution_id = %s) - %s",
        (execution_id, execution_id, self.trace_max),
    )
```

- **The `idx` is computed inline** by the `INSERT ... SELECT`: `COALESCE(MAX(idx), -1) + 1` makes the
  first step `0` and every subsequent step monotonic — **no pre-read** of the current max from the
  application, so there is no extra round-trip and no read-modify-write race within the transaction.
- **The cap is a ring.** When `trace_max` is set, the `DELETE` removes any step whose `idx` is at or
  below `MAX(idx) - trace_max`, keeping only the last `trace_max` steps per execution.
- **`read_trace` takes the index from the `idx` column** — it does not re-number on read, so the
  `index` a caller sees is the durable `idx`.

The standalone wrappers open their own transaction:

```text
def append_trace(self, execution_id: str, entry: dict) -> None:
    with self._conn.cursor() as cur:
        self._write_trace(cur, execution_id, entry)
    self._conn.commit()

def read_trace(self, execution_id: str) -> list[dict]:
    with self._conn.cursor() as cur:
        cur.execute("SELECT idx, entry FROM trace WHERE execution_id = %s ORDER BY idx", (execution_id,))
        rows = cur.fetchall()
    self._conn.commit()
    return [{**json.loads(entry), "index": idx} for idx, entry in rows]
```

`read_trace` returns each entry's JSON with the `idx` merged back in as `"index"`.

## Reads & sweeps

### `load`

```text
def load(self, execution_id: str) -> Optional[Execution]:
    with self._conn.cursor() as cur:
        cur.execute("SELECT data FROM executions WHERE id = %s", (execution_id,))
        row = cur.fetchone()
    self._conn.commit()  # end the read transaction so the next read sees fresh data
    return Execution.model_validate_json(row[0]) if row is not None else None
```

The `self._conn.commit()` **after a read** is deliberate: psycopg opens an implicit transaction on
the first statement, and under Postgres's default isolation the connection would otherwise keep
seeing the same snapshot for the life of that transaction. Committing closes the read transaction so
the **next** `load` (or `list_executions`) starts a fresh one and sees data committed by other
workers in the meantime. Every read method in this store ends with the same `commit()` for that
reason.

### `list_executions`

Builds a `WHERE` clause from the optional filters and projects only the lightweight summary fields —
it **never selects the heavy `data` blob**. Because `data` is stored as `TEXT`, it is cast to `jsonb`
and the scalars pulled out with `->>`:

```text
"SELECT id, definition_id, version, data::jsonb->>'status', "
"data::jsonb->>'outcome', data::jsonb->>'active_path', data::jsonb->>'parent_id' "
"FROM executions WHERE {' AND '.join(where)} ORDER BY id LIMIT %s OFFSET %s"
```

- `definition_id` filters with `definition_id = %s` (the broken-out column).
- `status` filters **inside the JSON**: `(data::jsonb->>'status') = ANY(%s)` with the list of status
  values — `= ANY(array)` matches any of them (OR).
- `roots_only` adds `(data::jsonb->>'parent_id') IS NULL`.
- Pagination is `LIMIT %s OFFSET %s` with `limit + 1`: the store fetches one extra row to know
  whether there is a next page, returns `rows[:limit]` as `ExecutionSummary`, and sets
  `next_cursor` (an opaque base64 offset) only when `len(rows) > limit`. Ordering is stable by `id`.

### Dedupe, outbox, spawn and timer queries

All are thin SELECT/DELETE wrappers, each committing to end its read transaction:

```text
is_processed(execution_id, event_id):
    SELECT 1 FROM processed_events WHERE execution_id = %s AND event_id = %s

pending_outbox():
    SELECT seq, target_id, event FROM outbox ORDER BY seq
ack_outbox(seq):
    DELETE FROM outbox WHERE seq = %s

pending_spawns():
    SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq
ack_spawn(seq):
    DELETE FROM spawns WHERE seq = %s

due_timers(now):
    SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= %s ORDER BY fire_at
delete_timer(execution_id, path, fire_at):
    DELETE FROM timers WHERE execution_id = %s AND path = %s AND fire_at = %s
```

`pending_outbox`/`pending_spawns` order by their `BIGSERIAL` `seq` (oldest first) and the relay acks
each by deleting it. `due_timers` returns every timer due at `now` as `(execution_id, path, fire_at)`,
oldest due first. `delete_timer` deletes **only if `fire_at` still matches** — so a concurrent
re-schedule of the same path to a new time survives a stale sweep (the old `fire_at` no longer
matches, the DELETE is a no-op).

`close()` calls `self._conn.close()`.

## Async twin

`AsyncPostgresStore` (in `harel/engine/aio_store/postgres.py`) is the async mirror, byte-for-byte the
same SQL and the same CAS logic, but over a **`psycopg_pool.AsyncConnectionPool`** instead of a single
shared connection.

```text
store = await AsyncPostgresStore.from_dsn("postgresql://stm:stm@db:5432/stm", pool_size=10)
```

The key differences:

- **A connection pool, not one connection.** `from_dsn` opens an `AsyncConnectionPool(conninfo=dsn,
  min_size=1, max_size=pool_size, open=False)`, awaits `pool.open()`, creates the six tables on a
  borrowed connection, and returns the store. `trace_max` is set in `__init__` (to `DEFAULT_TRACE_MAX`).
- **Each method checks out a pooled connection for the duration of one transaction**
  (`async with self._pool.connection() as conn:`) and commits/closes it back to the pool. Because
  every operation runs on its **own** connection, concurrent workers issue **real parallel DB
  requests** — the async core can have many `commit`s in flight against Postgres at once, which is the
  whole point of the distributed-SQL deployment. (The sync store, by contrast, serializes everything
  through its single connection.)
- **`load_for_event` — one round-trip.** The async store adds a method the worker's hot path uses:
  load the Execution **and** check whether an event is already processed in a single query, instead of
  a `load` followed by a separate `is_processed`:

  ```text
  "SELECT e.data, EXISTS(SELECT 1 FROM processed_events p "
  "WHERE p.execution_id = %s AND p.event_id = %s) "
  "FROM executions e WHERE e.id = %s"
  ```

  It returns `(Execution | None, already_processed: bool)` — `(None, False)` if the row is absent.

Everything else — both commit paths (the `harel_commit_cas` fast path for a state-only commit and
the multi-statement `UPDATE ... WHERE version` complex path), the `rowcount == 0`
insert-or-conflict branch, the outbox/dedupe/spawn/timer/trace statements, the inline-`idx` trace
ring — is identical to the sync store, just `await`ed (the `harel_commit_cas` function is created in
schema setup under the same `pg_advisory_xact_lock`).

## When to pick it / tradeoffs

Pick `PostgresStore` when:

- You need state **shared across multiple worker hosts** (the distributed deployment) and you do not
  want to operate Redis — Postgres can be both the store **and** the [transport](../transports)
  (`PostgresTransport`), so you can run an all-Postgres stack.
- You already operate Postgres and want the engine's state in the same SQL server you back up and
  monitor.
- You want strong, server-side serialization of concurrent writers for free: the row-lock CAS means
  no application coordination, and the single-transaction `commit` gives you crash safety.

Tradeoffs and scaling:

- It needs a Postgres server — heavier than [`SqliteStore`](sqlite) (which needs nothing) for a
  single-host run. If you only need one machine, use SQLite.
- Throughput is bounded by the Postgres instance. You scale **horizontally by sharding** Postgres
  instances (one shard owns a set of executions), the same pattern Temporal/DBOS use. On the queue
  side, the [`PostgresTransport`](../transports) claims work with `FOR UPDATE SKIP LOCKED`, which
  lets many workers pull from one queue without blocking each other — so a sharded Postgres deployment
  scales out cleanly.
- For the async deployment, size the pool (`pool_size`) to the number of concurrent workers you want
  hitting the DB in parallel.

## See also

- [Stores hub](../stores) — the seam and the full backend matrix.
- [Durability](../durability) — the version/CAS, outbox, dedupe, spawn-outbox and timer guarantees this
  store implements.
- [Transports](../transports) — `PostgresTransport` and the `FOR UPDATE SKIP LOCKED` claim.
- [`SqliteStore`](sqlite) — the single-host sibling and golden exemplar for the SQL family.
