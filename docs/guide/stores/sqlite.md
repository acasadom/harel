# SqliteStore — one machine, zero infrastructure

`SqliteStore` is the durable `ExecutionStore` backend for the **single-host** case: one
process (or several processes on the same box / a shared volume) running statecharts that must
**survive a restart** with no external service to operate. It is the simplest durable backend
and the **golden exemplar** for the SQL family — `PostgresStore` and `RqliteStore` are the same
shape over a networked engine. If you only need throwaway state, point it at `:memory:`; if you
need persistence, point it at a file path.

## What it is and how it stores things

- **Engine:** the Python stdlib `sqlite3` module — no driver to install, no server to run. The
  connection is opened once in `__init__` and reused for every operation:
  `sqlite3.connect(str(path), check_same_thread=False)` (the `check_same_thread=False` lets the
  same connection be used from a worker thread, e.g. the timer sweep).
- **Storage model:** each `Execution` is stored as **one row** in `harel_executions`, keyed by its
  `id`, with the whole object serialized to JSON in the `data` column (`exe.model_dump_json()`).
  Reloading rehydrates it with `Execution.model_validate_json(...)`. A fresh `SqliteStore`
  opened on the same file reads the committed rows, so a run resumes after a process restart.
- **One write per event is one transaction.** The central method, `commit`, performs the CAS
  write of the Execution *and* every side-effect of that step (outbox events, the dedupe marker,
  timer arms/disarms, spawn intents, the optional trace step) and then calls `self._conn.commit()`
  **once**. Either all of it lands or none of it does — that all-or-nothing property is what makes
  a crash safe (the state and everything the engine deferred are a single durable unit). See
  [durability](../durability).
- **PRAGMAs.** Two are set at open time:

  ```text
  PRAGMA journal_mode=WAL    -- readers don't block the single writer
  PRAGMA busy_timeout=5000   -- wait up to 5s for the write-lock instead of erroring SQLITE_BUSY
  ```

  WAL (write-ahead logging) lets the monitor's `list_executions`/`load` read while a writer
  holds the write-lock; `busy_timeout` makes a contended writer wait for the lock rather than
  immediately raising `database is locked`.

## Schema

Six tables, all created with `CREATE TABLE IF NOT EXISTS` in `__init__` and then committed.

### `harel_executions` — the durable state

```text
CREATE TABLE IF NOT EXISTS harel_executions
(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL,
 version INTEGER NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | TEXT, **PRIMARY KEY** | the `Execution.id` (a uuid hex). One row per Execution. |
| `definition_id` | TEXT NOT NULL | the id of the `Definition` this Execution runs — a broken-out column so `list_executions` can filter by it without parsing the blob. |
| `data` | TEXT NOT NULL | the **full Execution serialized**, `exe.model_dump_json()`. Holds everything: `status`, `outcome`, `error`, `active_path`, `history`, `context`, `children` (the join counter), `parent_id`/`child_id`, `invoke_seq`, `definition_fqn`, `created_at`/`updated_at`/`finished_at`, and `version`. The store treats it as an opaque blob except where it reaches inside with `json_extract` for the summary projection. |
| `version` | INTEGER NOT NULL | the **optimistic-concurrency token** — a broken-out copy of `Execution.version`. A write succeeds only if the stored `version` still matches the one the Execution was loaded at; the CAS uses this column (not the JSON) so the comparison is a cheap indexed scalar. |

`data` is the source of truth for the Execution; `definition_id` and `version` are denormalized
out of it purely so the hot paths (CAS, listing) don't have to parse JSON.

### `harel_outbox` — the transactional event outbox

```text
CREATE TABLE IF NOT EXISTS harel_outbox
(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `seq` | INTEGER, **PRIMARY KEY AUTOINCREMENT** | monotonic delivery order; the ack key. |
| `target_id` | TEXT (nullable) | the Execution to deliver the event to (`NULL` = no specific target). |
| `event` | TEXT NOT NULL | the `Event` serialized (`event.model_dump_json()`). |

Events emitted by a step are *not* delivered inline; they are inserted here in the same
transaction as the state advance, and a relay drains them after commit (`pending_outbox` /
`ack_outbox`). A crash between the state write and the delivery cannot lose an emitted
`Finished` — it is already durable in this table.

### `harel_processed_events` — at-least-once dedupe

```text
CREATE TABLE IF NOT EXISTS harel_processed_events
(execution_id TEXT NOT NULL, event_id TEXT NOT NULL,
 PRIMARY KEY (execution_id, event_id))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | the Execution that handled the event. |
| `event_id` | TEXT NOT NULL | the `Event.id` that was processed. |
| — | **PRIMARY KEY (execution_id, event_id)** | one marker per (execution, event). |

Under at-least-once delivery the same event can arrive twice; recording its id here (and
checking it on load) makes the effect take hold **once**. The composite primary key is what
makes the `INSERT OR IGNORE` in `commit` a no-op on a redelivery.

### `harel_timers` — durable timers

```text
CREATE TABLE IF NOT EXISTS harel_timers
(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL,
 PRIMARY KEY (execution_id, path))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | the Execution that armed the timer. |
| `path` | TEXT NOT NULL | the `full_path` of the state that armed it (a `timeout:` state). |
| `fire_at` | REAL NOT NULL | wall-clock epoch seconds at which the `Timeout` is due. |
| — | **PRIMARY KEY (execution_id, path)** | one live timer per state — re-arming the same path **replaces** it (the upsert in `commit`). |

A `timeout:` state arms a timer on enter and cancels it on exit, both applied in the same
`commit` as the transition that entered/exited it. The sweep (`due_timers`) finds rows whose
`fire_at <= now` and publishes a `Timeout` event.

### `harel_spawns` — the orthogonal-fork spawn outbox

```text
CREATE TABLE IF NOT EXISTS harel_spawns
(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
 root_path TEXT NOT NULL, context TEXT NOT NULL)
```

| Column | Type | Meaning |
| --- | --- | --- |
| `seq` | INTEGER, **PRIMARY KEY AUTOINCREMENT** | monotonic order; the ack key. |
| `parent_id` | TEXT NOT NULL | the orthogonal parent Execution forking the child. |
| `child_id` | TEXT NOT NULL | the deterministic id of the child Execution to create. |
| `root_path` | TEXT NOT NULL | the branch node the child runs (its `root_path`). |
| `context` | TEXT NOT NULL | the child's seed context, JSON (`json.dumps(context)`). |

Entering an AND-state does **not** create the region children inline; it enqueues one spawn
intent per region here, committed atomically with the parent's advance and its `children` join
expectations. A relay then creates each child idempotently (`pending_spawns` / `ack_spawn`).
This mirrors the event outbox and is what makes a crash-during-fork safe.

### `harel_trace` — the execution-timeline ring (opt-in)

```text
CREATE TABLE IF NOT EXISTS harel_trace
(execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL,
 PRIMARY KEY (execution_id, idx))
```

| Column | Type | Meaning |
| --- | --- | --- |
| `execution_id` | TEXT NOT NULL | the Execution whose timeline this step belongs to. |
| `idx` | INTEGER NOT NULL | the monotonic step index within that Execution (assigned inline, see below). |
| `entry` | TEXT NOT NULL | one step (event/transition/actions/context_out), JSON. |
| — | **PRIMARY KEY (execution_id, idx)** | one row per step. |

Off by default; written by `commit` only when a `trace` step is passed. The store keeps a ring
of the last `trace_max` steps per Execution (`DEFAULT_TRACE_MAX = 200`). Powers the monitor's
timeline without affecting `load` (which still reads the snapshot, not a replay).

## The CAS write (`_write`)

`_write` is the heart of the optimistic-concurrency guarantee. It is deliberately **separate
from the commit of the transaction** so it can be batched with the rest of `commit`'s
statements:

```text
UPDATE harel_executions SET data = ?, version = ? WHERE id = ? AND version = ?
```

The code:

```text
old = exe.version
exe.version = old + 1
data = exe.model_dump_json()
cur = self._conn.execute(
    "UPDATE harel_executions SET data = ?, version = ? WHERE id = ? AND version = ?",
    (data, exe.version, exe.id, old),
)
if cur.rowcount == 0:
    found = self._conn.execute("SELECT version FROM harel_executions WHERE id = ?", (exe.id,)).fetchone()
    if found is None and old == 0:
        self._conn.execute(
            "INSERT INTO harel_executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
            (exe.id, exe.definition_id, data, exe.version),
        )
    else:
        exe.version = old  # undo the in-memory bump; the commit did not happen
        raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)
```

Step by step:

1. **Capture `old = exe.version`**, then bump `exe.version = old + 1`. The bump happens **before
   the dump** so the JSON in `data` carries the *new* version too (the blob and the broken-out
   `version` column stay consistent — the blob's own `version` field equals the column).
2. **The CAS:** the `UPDATE ... WHERE id = ? AND version = ?` matches the row only if its stored
   `version` is still `old`. If another writer advanced the row in the meantime, the `WHERE`
   matches nothing and `rowcount == 0`.
3. **`rowcount == 0` disambiguation.** Zero matched rows means one of two things, distinguished
   by a follow-up `SELECT version`:
   - **Brand-new Execution** (`found is None and old == 0`): no row existed yet and we are at the
     initial version — so this is the first save. Do the `INSERT`.
   - **Stale write** (a row exists but moved past `old`, or it's missing with `old != 0`): another
     writer won. Roll back the in-memory bump (`exe.version = old`, since this transaction will
     not commit) and raise `StoreConflict(exe.id, expected=old, found=...)`.
4. **`_write` does not commit.** It only issues statements on the connection. That is the whole
   point: `commit` can run `_write` plus the outbox/dedupe/timer/spawn/trace statements and then
   issue **one** `self._conn.commit()`, so they are atomic together. `StoreConflict` is the
   single-writer-per-Execution backstop — the caller (a runner/worker) reloads and retries, or
   drops the stale work.

`save` is the standalone version used outside an event step: `_write` then `self._conn.commit()`,
rolling back (and restoring `exe.version`) on any exception, exactly like `commit` below.

## `commit` — the one atomic transaction

`commit` is the only write path on the hot loop. It issues a sequence of statements on the open
connection and commits them as **one transaction**, so the state advance and every deferred
effect of that event are a single durable unit:

```text
def commit(self, exe, emits, processed_event_id=None, timers=(), spawns=(), trace=None):
    old = exe.version
    try:
        self._write(exe)                                   # 1. CAS the Execution (no commit yet)

        seqs = []
        for target_id, event in emits:                     # 2. enqueue emitted events (outbox)
            cur = self._conn.execute(
                "INSERT INTO harel_outbox (target_id, event) VALUES (?, ?)",
                (target_id, event.model_dump_json()),
            )
            seqs.append(cur.lastrowid)                     #    each entry's seq, for the caller

        if processed_event_id is not None:                 # 3. record this event handled (dedupe)
            self._conn.execute(
                "INSERT OR IGNORE INTO harel_processed_events (execution_id, event_id) VALUES (?, ?)",
                (exe.id, processed_event_id),
            )

        for child_id, root_path, context in spawns:        # 4. enqueue orthogonal child creations
            self._conn.execute(
                "INSERT INTO harel_spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                (exe.id, child_id, root_path, json.dumps(context)),
            )

        for op in timers:                                  # 5. arm / disarm durable timers
            if op.action == "schedule":
                self._conn.execute(
                    "INSERT INTO harel_timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                    (exe.id, op.path, op.fire_at),
                )
            else:
                self._conn.execute(
                    "DELETE FROM harel_timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                )

        if trace is not None:                              # 6. append a trace step (opt-in)
            self._write_trace(exe.id, trace)

        self._conn.commit()                                # 7. ONE commit: all-or-nothing
        return seqs
    except BaseException:
        self._conn.rollback()                              # any failure: discard the whole batch
        exe.version = old
        raise
```

Statement by statement:

1. **`_write(exe)`** — the CAS `UPDATE` (or `INSERT` for a brand-new Execution). If it raises
   `StoreConflict`, no further statements run; the `except` rolls back and re-raises.
2. **Outbox INSERTs** — one row per emitted event. Deferred delivery, durable before the relay
   touches it.
3. **`INSERT OR IGNORE` into `harel_processed_events`** — mark the event that drove this step as
   handled. `OR IGNORE` makes a redelivery a no-op against the composite PK, so dedupe is
   idempotent.
4. **Spawn INSERTs** — one row per orthogonal child to create. Committed together with the
   parent's `children` join expectations (which live inside `exe`'s `data`), so the fork and its
   bookkeeping are atomic.
5. **Timer ops** — a `schedule` is an **upsert**: `ON CONFLICT(execution_id, path) DO UPDATE SET
   fire_at = excluded.fire_at`, so re-arming the same state replaces the prior fire time (the
   `(execution_id, path)` PK guarantees one live timer per state). A `cancel` is a `DELETE` by
   `(execution_id, path)`. Both are part of the same transaction as the transition that armed or
   disarmed them — a scheduled timer can never be lost relative to the state that scheduled it.
6. **`_write_trace`** — append the opt-in trace step (see next section). Only when `trace` is
   given.
7. **The single `self._conn.commit()`** — flushes the whole batch atomically.

The `except` branch covers **any** failure, not only a lost CAS: the entire batch (state, outbox,
dedupe, timers, spawns, trace) is discarded together and the in-memory `exe.version` is restored,
so a retry of the same Execution is not a false conflict. Rolling back on every error also
matters for the file as a whole: a transaction left open would keep holding SQLite's single
write lock, so every other writer on the same file would time out with `database is locked`,
and the next commit on this connection would persist the half-written batch.
Because all of this is **one transaction**, a crash at any point leaves the store either fully
at the prior step or fully at the new one — never half-applied (e.g. a state advanced but its
`Finished` lost, or a fork's children created but the parent's join expectations missing). This
is the durability contract; the relay and sweep below complete the at-least-once delivery
*after* commit.

## The trace ring (`_write_trace`)

`_write_trace` appends one timeline step **without committing** (so it batches into `commit`'s
transaction). It is two statements:

```text
INSERT INTO harel_trace (execution_id, idx, entry)
SELECT ?, COALESCE((SELECT MAX(idx) FROM harel_trace WHERE execution_id = ?), -1) + 1, ?
```

then, if `trace_max` is set, the ring cap:

```text
DELETE FROM harel_trace WHERE execution_id = ? AND idx <=
(SELECT MAX(idx) FROM harel_trace WHERE execution_id = ?) - trace_max
```

Why it is shaped this way:

- **`idx` is computed inline** with `COALESCE(MAX(idx), -1) + 1` rather than read first and
  passed in. There is **no pre-read round-trip**, and the value is **monotonic per Execution**.
  Crucially it is computed off `MAX(idx)`, *not* off a row count — so it keeps climbing even
  after the ring `DELETE` removes old rows (a count-based index would collide with a surviving
  row after eviction). The first step gets `idx = 0` (`COALESCE(NULL, -1) + 1`).
- **The cap** deletes everything whose `idx` is at least `trace_max` behind the current max,
  keeping only the last `trace_max` steps. With `DEFAULT_TRACE_MAX = 200` the timeline is a
  200-step ring per Execution.
- **`read_trace` re-stamps the index from the `idx` column**, so the stored JSON `entry` need not
  carry its own index:

  ```text
  SELECT idx, entry FROM harel_trace WHERE execution_id = ? ORDER BY idx
  -> [{**json.loads(entry), "index": idx} for idx, entry in rows]
  ```

`append_trace` is the **demo/test seam** — it writes a step and commits it on its own (used to
seed a timeline outside a real event step):

```text
def append_trace(self, execution_id, entry):
    self._write_trace(execution_id, entry)
    self._conn.commit()
```

## Reads and sweeps

### `load`

```text
SELECT data FROM harel_executions WHERE id = ?
```

Returns `Execution.model_validate_json(row[0])` or `None` if no row. The full snapshot, rehydrated.

### `load_for_event` (async only)

The sync `SqliteStore` has no `load_for_event`; the worker uses `load` + `is_processed`
separately. The **async twin** folds both into one round-trip (see Async twin below). The
`ExecutionStore` Protocol lists `load_for_event` as optional sugar.

### `list_executions`

The monitor/list view. It **never selects the heavy `data` blob**; it projects only the scalar
summary fields, reaching inside the JSON with `json_extract` for the ones that aren't broken-out
columns:

```text
SELECT id, definition_id, version, json_extract(data,'$.status'),
       json_extract(data,'$.outcome'), json_extract(data,'$.active_path'),
       json_extract(data,'$.parent_id'), json_extract(data,'$.finished_at') FROM harel_executions
WHERE <conditions> ORDER BY id LIMIT ? OFFSET ?
```

- **WHERE building.** Conditions start at `["1=1"]` and accumulate: `definition_id = ?` (exact
  match, a real column); `json_extract(data,'$.status') IN (?, ?, ...)` for a status set (OR);
  `json_extract(data,'$.parent_id') IS NULL` when `roots_only`. Because `status`/`outcome`/
  `parent_id` live inside the blob, `json_extract` is how the filters reach them — no separate
  columns needed.
- **Pagination.** The cursor is an opaque base64 of an integer offset (`_decode_offset` /
  `_encode_offset`). It fetches `limit + 1` rows: if more than `limit` come back there is a next
  page, so it returns the first `limit` as `ExecutionSummary` items and a `next_cursor`
  encoding `off + limit`; otherwise `next_cursor` is `None`. Ordering is stable by `id`.

### `is_processed`

```text
SELECT 1 FROM harel_processed_events WHERE execution_id = ? AND event_id = ?
```

Returns `True` if the marker exists — the dedupe lookup under at-least-once delivery.

### `pending_outbox` / `ack_outbox`

```text
SELECT seq, target_id, event FROM harel_outbox ORDER BY seq          -- oldest first
DELETE FROM harel_outbox WHERE seq = ?                               -- then commit
```

`pending_outbox` returns the undelivered `OutboxEntry` list (oldest first), deserializing each
`event`. `ack_outbox(seq)` removes a delivered entry **and commits on its own** (the relay's ack
is its own small transaction, independent of the event step that enqueued it).

### `pending_spawns` / `ack_spawn`

```text
SELECT seq, parent_id, child_id, root_path, context FROM harel_spawns ORDER BY seq
DELETE FROM harel_spawns WHERE seq = ?                               -- then commit
```

`pending_spawns` returns `SpawnEntry` rows (oldest first), `json.loads`-ing the context.
`ack_spawn(seq)` removes a created child's intent and commits. The relay creates each child
idempotently before acking, so a crash between create and ack just retries a harmless create.

### `due_timers` / `delete_timer`

```text
SELECT execution_id, path, fire_at FROM harel_timers WHERE fire_at <= ? ORDER BY fire_at
```

`due_timers(now)` returns the timers whose `fire_at <= now` as `(execution_id, path, fire_at)`,
soonest first — the sweep publishes a `Timeout` for each.

```text
DELETE FROM harel_timers WHERE execution_id = ? AND path = ? AND fire_at = ?
```

`delete_timer` is **guarded on `fire_at`**: it deletes the row only if it still holds the exact
fire time the sweep saw. This is the subtle, important bit — if the model **re-scheduled** the
same `(execution_id, path)` to a *new* time between the sweep reading it and acting on it, the
`fire_at` no longer matches, the `DELETE` removes nothing, and the freshly-armed timer survives
the stale sweep instead of being silently dropped. It commits on its own.

### `close`

`self._conn.close()` — releases the connection.

## `purge`

`purge(execution_id, expected_version)` permanently deletes one Execution and every row keyed by
it, in **one transaction** — the same all-or-nothing unit as `commit`:

```text
DELETE FROM harel_executions WHERE id = ? AND version = ?        -- the CAS
-- 0 rows and the id still exists -> it moved on: roll back, return False
DELETE FROM harel_processed_events WHERE execution_id = ?
DELETE FROM harel_trace            WHERE execution_id = ?
DELETE FROM harel_timers           WHERE execution_id = ?
DELETE FROM harel_outbox           WHERE target_id    = ?
DELETE FROM harel_spawns           WHERE parent_id    = ?
COMMIT
```

Any error rolls the whole transaction back. It returns True if the Execution was deleted. If no row exists under the id at all, the companion deletes still run (a no-op, or the leftovers of an earlier interrupted purge) and it returns False. The control plane's [purge](../control-plane.md#purge) calls it once per member of a finished tree.
Afterwards a commit of a stale copy of the Execution finds no row at its version and raises
`StoreConflict` (`_write` inserts only when `old == 0`), so a purged Execution is never recreated.
The async twin runs the same transaction under its connection lock.

`ids_with_prefix(prefix)` — `SELECT id FROM harel_executions WHERE id LIKE ? ESCAPE '\'`, with
`%`, `_` and `\` in the prefix escaped — is how the control plane finds a tree's descendants.
## Async twin (`aio_store/sqlite.py`)

`AsyncSqliteStore` is a faithful mirror of `SqliteStore` over **`aiosqlite`** — same schema,
same SQL, same CAS, same one-transaction `commit`. The differences are mechanical:

- Every DB call is `await`ed (`await self._conn.execute(...)`, `await cur.fetchone()`,
  `await self._conn.commit()`).
- **One transaction at a time per connection.** A SQLite transaction belongs to the
  *connection*, not to the coroutine, and `aiosqlite` only runs one *statement* at a time: the
  `await`s between the statements of a `commit` are points where another coroutine can run. The
  async worker drives many coroutines over a single store (concurrent events, concurrent child
  spawns), so without further care their statements would interleave inside one another's
  transactions — one coroutine's `commit()` persisting another's half-written batch, or two
  coroutines both creating the same child Execution. The store therefore holds an
  **`asyncio.Lock` for every method**: each transaction (and each read, which must not observe
  another coroutine's uncommitted writes) has the connection to itself, and `commit` stays a
  single atomic transaction exactly like the sync version.
- **Construction is async:** the connection must be awaited open, so you build it with
  `await AsyncSqliteStore.create(path)` (a classmethod that opens the connection, sets the
  PRAGMAs, creates the tables, commits, and returns the instance). `__init__` just stores the
  connection and sets `self.trace_max = DEFAULT_TRACE_MAX` (the sync version sets it after the
  `CREATE TABLE`s; the async version sets it in `__init__`).
- It additionally provides **`load_for_event`** — load + dedupe-check in **one round-trip** (the
  worker's per-event pair), a single query that selects the data and an `EXISTS` over
  `harel_processed_events` together:

  ```text
  SELECT (SELECT data FROM harel_executions WHERE id = ?),
         EXISTS(SELECT 1 FROM harel_processed_events WHERE execution_id = ? AND event_id = ?)
  ```

  It returns `(Execution | None, already_processed: bool)`; if the data sub-select is `NULL`
  (no such Execution) it returns `(None, False)`.

`:memory:` works in both as a non-persistent variant for tests.

## When to pick it / tradeoffs

Pick `SqliteStore` when:

- You run on **one host** (or several processes over a **shared volume**) and want durability
  with **zero infrastructure** — no server, no driver to install.
- A **single writer per Execution** is acceptable: WAL gives concurrent readers, but SQLite has
  one writer at a time, and the version CAS is the single-writer-per-Execution backstop. This is
  exactly the model the engine assumes.
- You want the simplest thing that survives a restart, or `:memory:` for fast tests.

**Concurrency, measured.** One file has one writer at a time, so:

- **Put the store and the [SQLite transport](../transports/sqlite) in separate files.** In one
  file the store's commits and the queue's claims and acks wait for the same lock; on the
  benchmark host the two were level with one event in flight, and two files drained 35–55%
  faster with more.
- **More worker processes don't add throughput.** The write lock serializes them, and it isn't
  fair: a process that finds it taken backs off with growing sleeps while the holder takes it
  again at once, so one or two workers end up doing almost all the work. One worker process,
  with in-flight concurrency, is the shape that suits SQLite.

The numbers are in [`bench/RESULTS.md`](https://github.com/acasadom/harel/blob/main/bench/RESULTS.md).

Reach for a **networked backend** instead when you need **many hosts** writing concurrently
(horizontal scale, no shared filesystem): `PostgresStore` (same SQL shape, server-side CAS via
`UPDATE ... WHERE version`) or `RqliteStore` (distributed SQLite over Raft). Those are the same
design as this page, just over a network engine — `SqliteStore` is the reference to read first.

See the [stores hub](../stores) for the full backend matrix and the
[`HAREL_STORE_BACKEND`](../distribution) selector, and [durability](../durability) for the
crash-safety guarantees this backend implements.
