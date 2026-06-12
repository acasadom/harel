# LibsqlStore — libSQL / Turso (experimental)

`LibsqlStore` is a durable [`ExecutionStore`](../stores) backend over **libSQL** — Turso's
open-source fork of SQLite — reached through the `libsql` Python package. The `libsql` driver is
**DB-API compatible** (a `sqlite3`-style driver), so for this backend the SQL, the version-CAS
write and the one-transaction `commit` are **byte-for-byte identical to
[`SqliteStore`](../stores#sqlitestore-one-machine-zero-infrastructure)**. The same six tables,
the same CAS, the same trace ring. What `LibsqlStore` adds over plain SQLite is purely *where the
database lives*: it can be a local file (an embed, like SQLite) **or** an embedded replica of a
remote Turso/`sqld` primary — selected entirely by constructor arguments. One backend is
therefore both a single-file embed and a distributed store.

## Experimental status

This backend is **EXPERIMENTAL**, and the boundary is precise:

- **Local-file path — tested.** Driving `LibsqlStore` against a local file (or `:memory:`) is
  covered in-process by the test suite. This is the SQLite-equivalent path and behaves like
  `SqliteStore`.
- **Turso / `sqld` embedded-replica path — wired but unvalidated.** Passing `sync_url=` (and
  `auth_token=`) builds an embedded replica against a Turso/`sqld` primary. The code path is in
  place but **has not been validated against a real Turso account**.
- **Replication is eventually consistent.** A Turso primary-follower deployment replicates
  asynchronously: a follower (the embedded replica) may lag the primary. The version-CAS reads
  the local replica, so a CAS decided against a stale follower can be wrong. In practice that
  means: **read from the primary for the CAS, or expect extra `StoreConflict` retries** while the
  follower catches up.

The docstring states the same constraints verbatim:

```text
EXPERIMENTAL: the local-file path is covered in-process by the test suite; the Turso/
`sqld` embedded-replica path (sync_url) is wired but not yet validated against a real
Turso account, and its primary-follower replication is eventually consistent (read from the
primary for CAS, or expect extra StoreConflict retries).
```

## Connection modes

The constructor adapts to one of two modes by argument. There is no separate class for "local"
vs "replica" — the kwargs decide:

```text
def __init__(
    self,
    database: Union[str, Path] = ":memory:",
    *,
    auth_token: str = "",
    sync_url: Optional[str] = None,
    sync_interval: Optional[float] = None,
) -> None:
    import libsql

    kwargs: dict[str, Any] = {"_check_same_thread": False}
    if sync_url is not None:  # embedded replica against a Turso/sqld primary
        kwargs["sync_url"] = sync_url
        kwargs["auth_token"] = auth_token
        if sync_interval is not None:
            kwargs["sync_interval"] = sync_interval
    self._conn = libsql.connect(str(database), **kwargs)
```

`_check_same_thread=False` is always passed (so the one connection can be used from a worker
thread — the async twin off-loads each call to a thread; see below). The two modes:

1. **Plain local file** — `LibsqlStore("state.db")` (or `LibsqlStore(":memory:")`, the test
   variant). `sync_url` is `None`, so `libsql.connect("state.db", _check_same_thread=False)` opens
   a local SQLite-compatible database file. Reads and writes are all local. This is the
   SQLite-equivalent embed.

2. **Embedded replica** — `LibsqlStore("local.db", sync_url="libsql://…", auth_token="…")`. With
   `sync_url` set, the kwargs grow `sync_url` + `auth_token` (and `sync_interval` if given), so the
   connection becomes `libsql.connect("local.db", _check_same_thread=False, sync_url=…,
   auth_token=…)`. Operationally:
   - **reads** come from the **local replica file** (`database`) — fast, local, possibly stale;
   - **writes** are **routed to the Turso/`sqld` primary** and **synced back** into the local
     replica;
   - `sync_interval` (seconds) controls how often the replica pulls the primary's latest state in
     the background.

   This is the path that makes the single backend a distributed store: the same SQL runs, but the
   authoritative copy lives on a remote primary and is replicated to every embedded replica.

After connecting, `__init__` creates the six tables (`CREATE TABLE IF NOT EXISTS …`), sets
`self.trace_max = DEFAULT_TRACE_MAX` (200), and `self._conn.commit()`s the DDL.

## Schema — the six tables

The schema is created verbatim in `__init__` (identical shape to `SqliteStore`):

```text
CREATE TABLE IF NOT EXISTS executions
  (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)

CREATE TABLE IF NOT EXISTS outbox
  (seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)

CREATE TABLE IF NOT EXISTS processed_events
  (execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))

CREATE TABLE IF NOT EXISTS timers
  (execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL,
   PRIMARY KEY (execution_id, path))

CREATE TABLE IF NOT EXISTS spawns
  (seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
   root_path TEXT NOT NULL, context TEXT NOT NULL)

CREATE TABLE IF NOT EXISTS trace
  (execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL,
   PRIMARY KEY (execution_id, idx))
```

What each holds:

- **`executions`** — one row per Execution. `data` is the full `Execution` serialized as JSON
  (`exe.model_dump_json()`); `version` is the optimistic-concurrency counter; `definition_id` lets
  `list_executions` filter without parsing the blob. PK is `id`.
- **`outbox`** — the transactional outbox of emitted events awaiting delivery. `seq` is a
  monotonic auto-increment used to order and ack entries; `target_id` is the Execution the event
  is delivered to (nullable = no target); `event` is the event JSON. The relay drains it after
  the commit, so a crash never loses a `Finished`.
- **`processed_events`** — the dedupe ledger: `(execution_id, event_id)` for every event already
  handled, PK on both. Makes at-least-once delivery effect-once.
- **`timers`** — durable timers, one per `(execution_id, path)` (PK), firing at `fire_at` (a
  REAL epoch second). Re-arming the same path replaces the row (upsert).
- **`spawns`** — pending orthogonal child-Execution creation intents, persisted in the same
  transaction as the parent's advance + join expectations. `seq` orders/acks; `parent_id`,
  `child_id`, `root_path` and the JSON `context` describe the child the relay must create
  idempotently.
- **`trace`** — the opt-in execution timeline ring. `(execution_id, idx)` PK; `entry` is the JSON
  step (event/transition/actions/`context_out`). Off by default.

## The CAS write + `commit`

### `_write` — the version-CAS

`_write` persists the Execution **without committing the transaction**, so it can be batched
atomically with the outbox/dedupe/timer/spawn/trace writes inside `commit`:

```text
def _write(self, exe: Execution) -> None:
    old = exe.version
    exe.version = old + 1
    data = exe.model_dump_json()
    cur = self._conn.execute(
        "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
        (data, exe.version, exe.id, old),
    )
    if cur.rowcount == 0:
        found = self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,)).fetchone()
        if found is None and old == 0:
            self._conn.execute(
                "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                (exe.id, exe.definition_id, data, exe.version),
            )
        else:
            exe.version = old
            raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)
```

The mechanism: bump `exe.version` to `old + 1`, then `UPDATE … WHERE id=? AND version=old`. If a
row matched, exactly one writer won and the version advanced. If `rowcount == 0`, no row was at
`old` — disambiguate by existence:

- **brand-new Execution** (`found is None and old == 0`): there is no row yet, so `INSERT` it at
  version 1.
- **stale write** (a row exists but its version moved past `old`, or `old != 0` with no row):
  another writer won. Roll back the in-memory `exe.version = old` (the commit did not happen) and
  raise `StoreConflict`.

This is the single-writer-per-execution backstop. On the Turso replica path the `UPDATE` is
evaluated against the local follower, which is exactly why an eventually-consistent follower can
produce spurious conflicts — see [Experimental status](#experimental-status).

`save` is the standalone version (one Execution, no side-writes):

```text
def save(self, exe: Execution) -> None:
    try:
        self._write(exe)
        self._conn.commit()
    except StoreConflict:
        self._conn.rollback()
        raise
```

### `commit` — one atomic write per event

`commit` is the **one atomic write per event boundary**: it persists the Execution advance, the
emitted events, the dedupe marker, the spawn intents, the timer mutations and the (optional) trace
step **all in a single SQLite transaction**, then commits once. Either every piece lands or none
does — which is the property that makes a crash safe: a fork's children and the parent's join
expectations commit together; a `Finished` is durable before delivery.

```text
def commit(
    self,
    exe: Execution,
    emits: list[tuple[Optional[str], Event]],
    processed_event_id: Optional[str] = None,
    timers: tuple[TimerOp, ...] = (),
    spawns: tuple[tuple[str, str, dict], ...] = (),
    trace: Optional[dict] = None,
) -> None:
    try:
        self._write(exe)
        for target_id, event in emits:
            self._conn.execute(
                "INSERT INTO outbox (target_id, event) VALUES (?, ?)",
                (target_id, event.model_dump_json()),
            )
        if processed_event_id is not None:
            self._conn.execute(
                "INSERT OR IGNORE INTO processed_events (execution_id, event_id) VALUES (?, ?)",
                (exe.id, processed_event_id),
            )
        for child_id, root_path, context in spawns:
            self._conn.execute(
                "INSERT INTO spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                (exe.id, child_id, root_path, json.dumps(context)),
            )
        for op in timers:
            if op.action == "schedule":
                self._conn.execute(
                    "INSERT INTO timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                    (exe.id, op.path, op.fire_at),
                )
            else:
                self._conn.execute(
                    "DELETE FROM timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                )
        if trace is not None:
            self._write_trace(exe.id, trace)
        self._conn.commit()
    except StoreConflict:
        self._conn.rollback()
        raise
```

Statement by statement:

1. **`self._write(exe)`** — the version-CAS UPDATE/INSERT above (uncommitted). If it raises
   `StoreConflict`, the whole transaction is rolled back and the error re-raised — nothing else in
   the batch is applied.
2. **Outbox** — for each `(target_id, event)`, `INSERT INTO outbox (target_id, event)`. The
   `seq` auto-increments. These are the deferred events the relay delivers post-commit.
3. **Dedupe** — if `processed_event_id` is given, `INSERT OR IGNORE INTO processed_events`. The
   `OR IGNORE` makes recording the handled event idempotent against the PK (a re-delivery is a
   no-op).
4. **Spawns** — for each `(child_id, root_path, context)`, `INSERT INTO spawns`, with `context`
   JSON-encoded. Persisted alongside the parent's advance so the orthogonal fork is atomic.
5. **Timers** — for each `TimerOp`: `schedule` does an upsert (`ON CONFLICT(execution_id, path) DO
   UPDATE SET fire_at = excluded.fire_at`, so re-arming the same path replaces its `fire_at`);
   `cancel` does `DELETE FROM timers WHERE execution_id=? AND path=?`. Arming/cancelling happens in
   the same transaction as the transition that caused it — no dual-write, a scheduled timer cannot
   be lost.
6. **Trace** — if a `trace` step is given, `_write_trace` appends it (still inside this txn; see
   below).
7. **`self._conn.commit()`** — the single atomic commit. Up to here nothing is durable.
8. **`except StoreConflict: rollback; raise`** — any CAS loss rolls the whole transaction back and
   propagates; the caller reloads and retries (or drops the stale work).

## The trace ring

`_write_trace` appends one timeline step **without committing** (so it batches into `commit`'s
transaction), in two statements: the index is computed inline (no pre-read), then the ring is
trimmed.

```text
def _write_trace(self, execution_id: str, entry: dict) -> None:
    self._conn.execute(
        "INSERT INTO trace (execution_id, idx, entry) "
        "SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ?",
        (execution_id, execution_id, json.dumps(entry)),
    )
    if self.trace_max:
        self._conn.execute(
            "DELETE FROM trace WHERE execution_id = ? AND idx <= "
            "(SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ?",
            (execution_id, execution_id, self.trace_max),
        )
```

- **Inline `idx`** — the `INSERT … SELECT MAX(idx)+1` computes the next index in SQL
  (`COALESCE(MAX(idx), -1) + 1`, so the first step is `0`). No round-trip to read the current max
  first; the `idx` is monotonic and survives the ring delete.
- **Ring trim** — when `trace_max` is set, `DELETE … WHERE idx <= MAX(idx) - trace_max` keeps only
  the last `trace_max` steps per execution. The index is **not** reset, so `read_trace` always
  returns ascending, contiguous-from-the-tail indices.

`append_trace` is the standalone seam (demo/test): write one step and commit it on its own.
`read_trace` returns the steps in order, stamping each with its stored `idx` as `index` (so the
JSON `entry` need not carry it):

```text
def append_trace(self, execution_id: str, entry: dict) -> None:
    self._write_trace(execution_id, entry)
    self._conn.commit()

def read_trace(self, execution_id: str) -> list[dict]:
    rows = self._conn.execute(
        "SELECT idx, entry FROM trace WHERE execution_id = ? ORDER BY idx", (execution_id,)
    ).fetchall()
    return [{**json.loads(entry), "index": idx} for idx, entry in rows]
```

## Reads & sweeps

### `load` and `load_for_event`

`load` rehydrates an Execution from its JSON blob. `load_for_event` folds the dedupe check into
the **same query** (the worker's per-event pair): it returns `(execution, already_processed)` in
one round-trip via a correlated subquery + `EXISTS`.

```text
def load(self, execution_id: str) -> Optional[Execution]:
    row = self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,)).fetchone()
    return Execution.model_validate_json(row[0]) if row is not None else None

def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
    row = self._conn.execute(
        "SELECT (SELECT data FROM executions WHERE id = ?), "
        "EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?)",
        (execution_id, execution_id, event_id),
    ).fetchone()
    if row is None or row[0] is None:
        return None, False
    return Execution.model_validate_json(row[0]), bool(row[1])
```

### `list_executions`

A page of lightweight `ExecutionSummary` for the monitor, projecting only the scalar fields out
of the JSON blob via `json_extract` (never pulling the full `data`). Filters: `definition_id`
(exact), `status` (any-of, `json_extract(data,'$.status') IN (…)`), `roots_only`
(`json_extract(data,'$.parent_id') IS NULL`). Pagination is offset-based with an opaque cursor,
fetching `limit + 1` rows to know whether a next page exists:

```text
rows = self._conn.execute(
    "SELECT id, definition_id, version, json_extract(data,'$.status'), "
    "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
    "json_extract(data,'$.parent_id') FROM executions "
    f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
    (*params, limit + 1, off),
).fetchall()
```

### `is_processed`

The dedupe lookup, a simple existence check on `processed_events`:

```text
def is_processed(self, execution_id: str, event_id: str) -> bool:
    row = self._conn.execute(
        "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
        (execution_id, event_id),
    ).fetchone()
    return row is not None
```

### Outbox relay

`pending_outbox` returns undelivered entries oldest-first (by `seq`); `ack_outbox` removes one by
`seq` after delivery:

```text
def pending_outbox(self) -> list[OutboxEntry]:
    rows = self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq").fetchall()
    return [
        OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
    ]

def ack_outbox(self, seq: int) -> None:
    self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
    self._conn.commit()
```

### Spawn relay

`pending_spawns` / `ack_spawn` mirror the outbox for orthogonal child-creation intents (the relay
creates each child idempotently, then acks):

```text
def pending_spawns(self) -> list[SpawnEntry]:
    rows = self._conn.execute(
        "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
    ).fetchall()
    return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

def ack_spawn(self, seq: int) -> None:
    self._conn.execute("DELETE FROM spawns WHERE seq = ?", (seq,))
    self._conn.commit()
```

### Timer sweep

`due_timers` returns every timer whose `fire_at <= now`, soonest-first; `delete_timer` removes one
**only if it still holds the same `fire_at`**, so a concurrent re-schedule to a new time survives
a stale sweep:

```text
def due_timers(self, now: float) -> list[tuple[str, str, float]]:
    rows = self._conn.execute(
        "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
    ).fetchall()
    return [(eid, path, fa) for eid, path, fa in rows]

def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
    self._conn.execute(
        "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
        (execution_id, path, fire_at),
    )
    self._conn.commit()
```

`close()` closes the underlying connection.

## Async twin — `AsyncLibsqlStore`

`harel/engine/aio_store/libsql.py` is the async-facing store the async worker talks to — but it is
**not natively async**. The `libsql` package is a *synchronous* sqlite3-style driver with no
awaitable API, so `AsyncLibsqlStore` is a **thread off-load wrapper** around the sync
`LibsqlStore`:

- it holds one sync `LibsqlStore` (`self._s`) and one `asyncio.Lock`;
- every method off-loads its sync call to a worker thread with `asyncio.to_thread(...)` (so the
  event loop is never blocked on libSQL IO);
- the lock **serializes** those off-loaded calls — one libSQL connection used **one operation at
  a time**, which suits this single-writer-class backend.

```text
def __init__(self, sync_store: Any) -> None:
    self._s = sync_store
    self._lock = asyncio.Lock()
```

A representative method shows the pattern (every method follows it): take the lock, then
`to_thread` the corresponding sync call:

```text
async def commit(
    self, exe, emits, processed_event_id=None, timers=(), spawns=(), trace=None
) -> None:
    async with self._lock:
        await asyncio.to_thread(
            self._s.commit, exe, emits,
            processed_event_id=processed_event_id, timers=timers, spawns=spawns, trace=trace,
        )
```

Construction is via the async factory `create()`, which builds the sync store **on a thread** too
(opening the connection and creating the tables is itself blocking IO):

```text
@classmethod
async def create(
    cls, database: str = ":memory:", *, auth_token="", sync_url=None, sync_interval=None
) -> "AsyncLibsqlStore":
    from harel.engine.store import LibsqlStore

    sync = await asyncio.to_thread(
        LibsqlStore, database, auth_token=auth_token, sync_url=sync_url, sync_interval=sync_interval
    )
    return cls(sync)
```

`trace_max` is a **property delegating to the sync store** (so configuring the ring on the async
wrapper configures the underlying sync store):

```text
@property
def trace_max(self) -> int:
    return self._s.trace_max

@trace_max.setter
def trace_max(self, value: int) -> None:
    self._s.trace_max = value
```

This is the one store whose async twin is not native — every other backend (aiosqlite,
redis.asyncio, psycopg async pool, motor, aioboto3, httpx for rqlite) issues real awaitable IO;
`AsyncLibsqlStore` is sync-on-a-thread because the `libsql` driver leaves no other option today.

## When to pick it / tradeoffs

- **Today: a single-file embed.** On the local-file path `LibsqlStore` is functionally
  `SqliteStore` reached through the libSQL driver — durable single-machine state, zero
  infrastructure, identical SQL and guarantees. If you only need that, `SqliteStore` is the
  battle-tested choice; `LibsqlStore` is interesting mainly as the *same code path* that can later
  point at Turso.
- **Tomorrow: managed distributed SQLite.** The `sync_url` / embedded-replica path turns the same
  backend into a client of a managed, replicated SQLite primary (Turso/`sqld`) — local reads,
  remote-primary writes, background sync — without changing any of the engine's persistence logic.
- **The caveats are the experimental ones.** The replica path is wired but unvalidated against a
  real account, and primary-follower replication is eventually consistent: a CAS decided against a
  lagging follower can spuriously conflict, so read from the primary for the CAS or budget for
  extra `StoreConflict` retries. The async wrapper is sync-on-a-thread, serialized to one
  in-flight operation — fine for a single-writer-class store, but not a true concurrent-IO
  backend.

See the [stores hub](../stores) for the full backend comparison and the
[`ExecutionStore`](../stores#the-contract-every-backend-implements) contract, and
[durability](../durability) for why the one-transaction `commit`, the outbox, the dedupe ledger
and the version-CAS together make a crash safe.
