# RqliteStore — distributed SQLite over Raft

`RqliteStore` is the durable `ExecutionStore` backend for the case where you want SQLite's
storage model — every `Execution` a JSON row, the whole step a single transaction — but with
**high availability and strong reads** instead of a single local file. It is the same shape as
`SqliteStore` and `PostgresStore` (the SQL family), but the database is
[**rqlite**](https://rqlite.io): a small distributed relational store that puts SQLite behind
the **Raft** consensus protocol. A cluster of rqlite nodes elects a leader, replicates every
write through the Raft log, and only applies a write once a quorum has durably accepted it. You
get SQLite semantics with no single point of failure and no Postgres server to operate.

`RqliteStore` does not link a database library. It talks to rqlite over its **HTTP API**
(`/db/execute`, `/db/query`) using `requests` (the async twin uses `httpx`). The store builds
SQL strings, ships them as JSON, and rqlite routes them through the leader.

`from_url` retries until rqlite is reachable **and has elected a leader** — a worker that boots
alongside rqlite in a compose stack waits for the cluster to come up rather than crashing:

```text
RqliteStore.from_url(url, connect_retries=30, retry_delay=1.0)
  -> for _ in range(connect_retries):
       try: return cls(url)              # __init__ issues the CREATE TABLEs
       except requests.exceptions.RequestException: time.sleep(retry_delay)
     raise last
```

`cls(url)` runs the schema-creation `_execute` in `__init__`; if rqlite is not up, or is up but
has no leader yet (the table create can't be applied), that POST raises, the loop sleeps, and it
tries again.

## The crucial constraint: no interactive transaction

rqlite has **no interactive, multi-round-trip transaction**. You cannot `BEGIN`, run a `SELECT`,
look at the result in Python, decide what to write, and then `COMMIT` — there is no open session
to hold a transaction across HTTP requests. A transaction in rqlite is **one HTTP request**
carrying a list of statements that the leader applies atomically (`transaction=true`).

That single constraint shapes the whole backend. Because `commit` cannot read the current
version, branch in Python, and conditionally write, **every write in `commit` is made
conditional inside the SQL itself**, all guarded on the same compare-and-swap (CAS) succeeding.
The Execution upsert applies only `WHERE version = old`; every side-write (outbox, dedupe,
timers, spawns, trace) runs only `WHERE EXISTS (… the executions row now holds our exact data)`.
A version mismatch therefore makes the *entire* request a no-op, which the store detects by
looking at the upsert's `rows_affected` and raises `StoreConflict`. There is no second
round-trip, no lock to hold, no read-then-write race.

## HTTP API helpers

Two private helpers wrap the rqlite HTTP API. Every other method is built on them.

### `_execute` — writes (POST /db/execute)

```text
def _execute(self, statements: list, transaction: bool = False) -> list:
    params = {"transaction": ""} if transaction else {}
    resp = self._session.post(
        f"{self._base}/db/execute", params=params, json=statements, timeout=self._timeout
    )
    resp.raise_for_status()
    results = resp.json()["results"]
    for res in results:
        if "error" in res:
            raise RuntimeError(f"rqlite execute error: {res['error']}")
    return results
```

- `statements` is a JSON array of statements (the **wire format** is described below).
- `transaction=True` adds the `transaction` query param, telling rqlite to wrap the whole
  statement list in one atomic transaction — all of them apply, or none do. This is how `commit`
  gets atomicity without an interactive session.
- rqlite returns HTTP 200 even for per-statement SQL errors; those surface as an `"error"` key on
  the individual result object. `_execute` walks every result and raises `RuntimeError` if any
  one carries an error, so a failed statement is never silently swallowed.

### `_query` — reads (POST /db/query, level=strong)

```text
def _query(self, sql: str, params: tuple) -> list:
    resp = self._session.post(
        f"{self._base}/db/query", params={"level": "strong"}, json=[[sql, *params]], timeout=self._timeout
    )
    resp.raise_for_status()
    result = resp.json()["results"][0]
    if "error" in result:
        raise RuntimeError(f"rqlite query error: {result['error']}")
    return result.get("values") or []
```

Every read is sent with `level=strong`. This is **linearizable**: rqlite forces the read
through the Raft leader and has the leader confirm it is still the leader (a leadership check
through the log) before answering. A weaker level (`none`/`weak`) could be served from a stale
follower or a deposed leader and return data that predates a committed write. Because the engine
relies on read-your-writes — load the Execution, decide the next step, CAS it back — a stale read
would let a worker act on an out-of-date snapshot, lose the CAS, and churn. `strong` buys
correctness at the cost of a leader round-trip per read; it is the right default for a state
engine. The method returns `result["values"]` (the list of rows; each row is a list of column
values) or `[]` when there are none.

### Statement wire format `[sql, *params]`

rqlite's HTTP API takes each statement as a JSON array whose **first element is the SQL string**
and whose **remaining elements are the positional parameters** that fill the `?` placeholders, in
order. So a parametrized statement is built as `[sql, p1, p2, …]`. `_query` constructs exactly
this — `json=[[sql, *params]]` — a list containing one such statement. `_execute` receives a list
of these arrays directly (each `statements` entry is itself a `[sql, *params]` list, except the
plain DDL strings at init which take no params). This positional binding keeps values out of the
SQL text — no string interpolation of user data into the query.

## Schema

`__init__` issues six `CREATE TABLE IF NOT EXISTS` statements in a single `_execute` call (not a
transaction — DDL that is idempotent by `IF NOT EXISTS`):

```text
CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL,
  data TEXT NOT NULL, version INTEGER NOT NULL)

CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT,
  event TEXT NOT NULL)

CREATE TABLE IF NOT EXISTS processed_events (execution_id TEXT NOT NULL, event_id TEXT NOT NULL,
  PRIMARY KEY (execution_id, event_id))

CREATE TABLE IF NOT EXISTS timers (execution_id TEXT NOT NULL, path TEXT NOT NULL,
  fire_at REAL NOT NULL, PRIMARY KEY (execution_id, path))

CREATE TABLE IF NOT EXISTS spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_id TEXT NOT NULL, child_id TEXT NOT NULL, root_path TEXT NOT NULL, context TEXT NOT NULL)

CREATE TABLE IF NOT EXISTS trace (execution_id TEXT NOT NULL, idx INTEGER NOT NULL,
  entry TEXT NOT NULL, PRIMARY KEY (execution_id, idx))
```

| Table | Holds |
| --- | --- |
| `executions` | One row per Execution. `id` PK; `definition_id` broken out so `list_executions` can filter without parsing JSON; `data` is the **full** Execution as `exe.model_dump_json()` (status, outcome, error, active_path, history, context, children join counter, parent/child id, invoke_seq, definition_fqn, version) — opaque except for `json_extract` in the summary projection; `version` is the broken-out optimistic-concurrency token used by the CAS (a cheap indexed scalar, not parsed from JSON). |
| `outbox` | The transactional event outbox: deferred events awaiting delivery. `seq` is a monotonic autoincrement ack token; `target_id` is the Execution to deliver to (NULL = no target); `event` is the serialized `Event`. Drained by `pending_outbox`/`ack_outbox`. |
| `processed_events` | The dedupe ledger: `(execution_id, event_id)` PK marks an event already handled, so at-least-once delivery takes effect exactly once. |
| `timers` | Durable timers, keyed by `(execution_id, path)` PK so re-entry replaces. `fire_at` is the absolute due time (epoch seconds). Swept by `due_timers`. |
| `spawns` | Pending orthogonal child-Execution creations, committed in the same transaction as the parent's advance + join expectations so a fork is atomic. `seq` ack token; `parent_id`/`child_id`/`root_path`/`context` describe the child to create. Drained by `pending_spawns`/`ack_spawn`. |
| `trace` | The opt-in execution trace ring: `(execution_id, idx)` PK, `entry` a JSON step. Append-only with a per-execution cap (`trace_max`, default `DEFAULT_TRACE_MAX = 200`). |

`data` is the source of truth; `definition_id` and `version` are denormalized out of it purely so
the hot paths (CAS, listing) never parse JSON.

## The guarded-upsert `commit` — the crux

`commit` is where the no-interactive-transaction constraint is paid off. It builds one
`statements` list and ships it as a single transactional request. It cannot read the current
version first, so it writes the new state **conditionally** and infers success from what changed.

It bumps the version in memory before serializing, so the stored JSON already carries the new
version:

```text
old = exe.version
exe.version = old + 1   # bump BEFORE dumping so the stored JSON carries the new version
new = exe.version
data = exe.model_dump_json()
```

### (a) The Execution upsert — the CAS

The first statement is the compare-and-swap. It inserts the row if absent, or updates it only if
the stored version still matches `old`:

```text
INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version
WHERE executions.version = ?
```

with params `(exe.id, exe.definition_id, data, new, old)` — the four insert values plus `old` for
the `WHERE`. On a first save the row is absent and the plain `INSERT` succeeds. On an update,
`ON CONFLICT(id)` fires and the `DO UPDATE … WHERE executions.version = old` clause only writes if
nobody else has advanced the row past `old`. If another writer already moved the version, the
`WHERE` matches nothing, the update is skipped, and **`rows_affected` is 0** — the CAS lost.

### (b) Each side-write — guarded on the row holding *our exact data*

Every other write in the same request is a guarded `INSERT … SELECT … WHERE EXISTS (…)`. The
outbox is the template:

```text
INSERT INTO outbox (target_id, event) SELECT ?, ?
WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)
```

with params `(target_id, event_json, exe.id, data)`. The crucial subtlety is **what the EXISTS
guards on**: not `version = new`, but `data = <our exact serialized data>`. Here is why that
matters.

If we guarded on `version = new`, consider two concurrent writers both loaded at `old`. Writer A
wins the CAS and stores its state at version `new`. Writer B's upsert loses (its `WHERE
version = old` no longer matches, A already moved it). But B's outbox `INSERT … WHERE EXISTS(…
version = new)` would *still find a row at version `new`* — A's row — and B's outbox event would
**leak**, even though B's state change never landed. Guarding on `data = <B's exact data>` closes
this: that EXISTS is true **iff B's own upsert won the CAS and wrote B's bytes**. A concurrent
writer reaching the same version with *different* state can't satisfy it, so B's side-writes are
correctly suppressed.

And in the benign double-delivery case where two writers genuinely produce **byte-identical**
state and events, the writes are idempotent — the `processed_events` PK / outbox content dedupes
at the target, so re-delivering the same thing twice is harmless.

The same `WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)` guard wraps:

- the dedupe marker, `INSERT OR IGNORE INTO processed_events (execution_id, event_id) SELECT ?, ?`;
- timer ops — a `DELETE FROM timers WHERE execution_id = ? AND path = ?` per op, plus, for a
  `schedule`, a following `INSERT INTO timers (execution_id, path, fire_at) SELECT ?, ?, ?` (so a
  schedule is a delete-then-insert = upsert; re-entry replaces `fire_at`, a cancel is just the
  delete);
- each spawn, `INSERT INTO spawns (parent_id, child_id, root_path, context) SELECT ?, ?, ?, ?`.

All of them carry the trailing `… WHERE EXISTS (… id = ? AND data = ?)` with `(exe.id, data)`.

### Firing the request and detecting the conflict

```text
results = self._execute(statements, transaction=True)
if results[0].get("rows_affected", 0) == 0:
    exe.version = old   # CAS missed: undo the in-memory bump (nothing was written)
    found = self._query("SELECT version FROM executions WHERE id = ?", (exe.id,))
    raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)
```

The whole list goes in `transaction=True`, so it is atomic — and because every statement is
guarded on the same condition, a CAS miss makes *all* of them no-ops, not just the upsert. The
store then inspects `results[0]` (the upsert): if its `rows_affected` is 0 the CAS lost, so it
rolls the in-memory version back to `old` (nothing was persisted) and raises `StoreConflict`,
querying the stored version for the error's `found`. On success `exe.version` is already `new`.

`save(exe)` is just `commit(exe, [])` — a CAS with no side-writes.

## The trace ring

When `commit` is given a `trace` step, it appends two more statements to the **same**
transactional request. The insert computes the next index inline (no pre-read — there is no
interactive transaction to read in), guarded on the CAS:

```text
INSERT INTO trace (execution_id, idx, entry)
SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ?
WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)
```

`COALESCE(MAX(idx), -1) + 1` gives `0` for the first step and `MAX+1` thereafter. Then, if
`trace_max` is set, a guarded cap delete trims the ring:

```text
DELETE FROM trace WHERE execution_id = ? AND idx <=
  (SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ?
AND EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)
```

with `?` for `trace_max`. Both statements are appended *after* the rest of the `commit` list, so
within the single atomic transaction the **INSERT runs before the DELETE** — the cap's
`MAX(idx)` therefore already sees the row we just inserted, and the delete keeps the last
`trace_max` steps including the new one. Both are guarded on the same `data` CAS, so a lost
commit appends and trims nothing.

`append_trace` is the unguarded standalone seam (used by the demo/monitor, not the engine's
commit path): the same insert and cap, but with no `WHERE EXISTS` guard and run as two separate
`_execute` calls:

```text
INSERT INTO trace (execution_id, idx, entry)
SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ?
-- then, if trace_max:
DELETE FROM trace WHERE execution_id = ? AND idx <=
  (SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ?
```

`read_trace` reads the ring back and re-derives each step's index from the `idx` column:

```text
SELECT idx, entry FROM trace WHERE execution_id = ? ORDER BY idx
-> [{**json.loads(entry), "index": idx} for idx, entry in rows]
```

so the merged `index` key always reflects the row's true position, even after the ring has been
trimmed.

## Reads & sweeps

All reads go through `_query` (and therefore `level=strong`).

**`load`** — rehydrate one Execution:

```text
SELECT data FROM executions WHERE id = ?
-> Execution.model_validate_json(rows[0][0]) if rows else None
```

**`load_for_event`** (on the async twin) — load and dedupe-check in **one** request, a single
`SELECT` of the data plus an `EXISTS` subquery over `processed_events`:

```text
SELECT data, EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?)
FROM executions WHERE id = ?
-> (Execution or None, bool)
```

This saves a round-trip on the hot worker path (otherwise load + `is_processed` would be two
strong reads, each a leader round-trip).

**`list_executions`** — the monitor/list projection, the same `json_extract` shape as
`SqliteStore` but over `_query`. It builds the `WHERE` from the optional filters
(`definition_id` exact; `status` as an `IN (…)` over `json_extract(data,'$.status')`; `roots_only`
as `json_extract(data,'$.parent_id') IS NULL`) and projects summary fields straight out of the
JSON blob:

```text
SELECT id, definition_id, version, json_extract(data,'$.status'),
  json_extract(data,'$.outcome'), json_extract(data,'$.active_path'),
  json_extract(data,'$.parent_id') FROM executions
WHERE <filters> ORDER BY id LIMIT ? OFFSET ?
```

It fetches `limit + 1` rows to decide whether there is a next page, returns the first `limit` as
`ExecutionSummary` items, and emits an opaque offset cursor (`_encode_offset(off + limit)`) when
more rows remain. Ordering is stable by `id`.

**`is_processed`** — dedupe check:

```text
SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?
-> bool(rows)
```

**Outbox relay** — `pending_outbox` drains oldest-first, `ack_outbox` removes a delivered entry:

```text
SELECT seq, target_id, event FROM outbox ORDER BY seq
-> [OutboxEntry(seq, target_id, Event.model_validate_json(event)), ...]

DELETE FROM outbox WHERE seq = ?
```

**Spawn relay** — `pending_spawns` drains the fork intents, `ack_spawn` removes a created one:

```text
SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq
-> [SpawnEntry(seq, parent_id, child_id, root_path, json.loads(context)), ...]

DELETE FROM spawns WHERE seq = ?
```

**Timer sweep** — `due_timers` returns everything due at `now`, `delete_timer` removes one but
only if it still holds the same `fire_at` (so a concurrent re-schedule to a new time survives a
stale sweep):

```text
SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at
-> [(execution_id, path, float(fire_at)), ...]

DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?
```

`ack_outbox`, `ack_spawn`, and `delete_timer` are unguarded standalone `_execute` deletes (they
are idempotent removes, not CAS writes). `close` closes the `requests.Session`.

## Async twin

`harel/engine/aio_store/rqlite.py` is `AsyncRqliteStore`, the exact mirror over
`httpx.AsyncClient`. The SQL, the schema, the guarded-upsert CAS, the `data`-guarded side-writes,
the trace ring, and the `level=strong` reads are **identical** — only the transport differs:
every `_execute`/`_query` is an `async def` that `await`s the `httpx` POST, and every store method
is `async`.

It is constructed with `await AsyncRqliteStore.from_url(url)`, which retries with
`anyio.sleep(retry_delay)` (instead of `time.sleep`), creating a fresh `httpx.AsyncClient` per
attempt, running the same six `CREATE TABLE IF NOT EXISTS` statements, and `aclose()`-ing the
client on a failed attempt before retrying. `trace_max` is set to `DEFAULT_TRACE_MAX` in
`__init__`, as in the sync store. `close` awaits `self._client.aclose()`.

## When to pick it / tradeoffs

Pick `RqliteStore` when you want **highly available, strongly-consistent durable state without
operating a Postgres server**. A small rqlite cluster (3 or 5 nodes) gives you:

- **The strongest durability of any harel backend.** Every write goes through Raft: it is
  replicated to a quorum and fsynced on each accepting node *before* it is applied. A node — even
  the leader — can die and no committed Execution step is lost; a new leader is elected from a
  replica that already has the write.
- **Linearizable reads** (`level=strong`), so a worker always loads the latest committed
  snapshot, with no stale-follower hazard.
- **SQLite semantics and operational simplicity** — a single self-contained binary per node, no
  separate database server to provision, tune, and back up.

The cost is that it is correspondingly the **slowest** store. Each write pays consensus latency (a
network round-trip to a quorum) **plus** an HTTP round-trip from the store **plus** an fsync per
node; each read pays a leader round-trip for its linearizability check. And because the
no-interactive-transaction constraint forces the guarded-upsert pattern, every `commit` is a
single (if larger) request — which is fine, but it means all the CAS cleverness lives in SQL
rather than in a held transaction.

Use it for distributed, HA deployments where losing a node must not lose state and where you'd
rather run rqlite than Postgres. For a single host, prefer [`SqliteStore`](./sqlite); for a
managed relational engine you already operate, prefer `PostgresStore`. The store and the
transport are independent seams — you can run all-rqlite (store + transport on the same cluster,
Raft serializing both) or mix freely.

See the [stores hub](../stores) for the full backend comparison and [durability](../durability)
for the CAS / outbox / dedupe / spawn-outbox model these backends implement.
