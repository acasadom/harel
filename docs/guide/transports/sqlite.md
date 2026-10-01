# SqliteTransport — durable, one host

A durable queue over stdlib `sqlite3` (WAL). The whole point: `claim` runs inside a **`BEGIN
IMMEDIATE`** transaction, so SQLite's global write-lock serializes claims across processes — the
per-group exclusivity selection is then race-free with plain SQL, no row or advisory locks. The
lease (`lock_expiry`) recovers a message a crashed worker was holding. The connection is opened
with `isolation_level=None` (autocommit) so the code drives `BEGIN IMMEDIATE`/`COMMIT` by hand in
`claim`; `PRAGMA busy_timeout=5000` makes a contended writer wait for the lock instead of erroring.

## Schema

Two tables:

```text
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- monotonic id: FIFO order + the Lease handle
  group_id     TEXT NOT NULL,                      -- the execution id (the exclusivity group)
  event        TEXT NOT NULL,                      -- the Event JSON (event.model_dump_json())
  locked_by    TEXT,                               -- worker id while leased / "__parked__" / NULL when free
  lock_expiry  REAL                                -- epoch when the lease (or park) ends; NULL when free
)

CREATE TABLE IF NOT EXISTS groups (
  group_id       TEXT PRIMARY KEY,    -- one row per group that has messages
  last_claimed_at REAL NOT NULL DEFAULT 0.0,  -- epoch of last claim (0 = never claimed)
  priority        INT  NOT NULL DEFAULT 0     -- set on first publish; 0–4
)
CREATE INDEX IF NOT EXISTS groups_by_last_claimed ON groups (last_claimed_at)  -- the claim's walk
CREATE INDEX IF NOT EXISTS messages_by_group ON messages (group_id, seq)       -- a group's head / in flight
```

`(locked_by, lock_expiry)` is the **lease**. The `groups` table drives **round-robin fairness**
and **priority filtering**: `claim` sorts by `last_claimed_at ASC` (oldest-claimed first) and
filters by `priority >= min_priority`.

## The claim — atomic select-then-lease with round-robin and priority

```text
BEGIN IMMEDIATE                                   -- take SQLite's write-lock: claims serialize
SELECT m.seq, m.group_id, m.event
  FROM groups g
  JOIN messages m ON m.seq = (SELECT MIN(seq) FROM messages WHERE group_id = g.group_id)  -- its head
  WHERE g.priority >= ?                                      -- priority floor (min_priority)
    AND NOT EXISTS (                                         -- group has nothing in flight
      SELECT 1 FROM messages x WHERE x.group_id = g.group_id
        AND x.locked_by IS NOT NULL AND x.lock_expiry >= ?)  -- (a lapsed lease or park doesn't count)
  ORDER BY g.last_claimed_at, g.rowid LIMIT 1               -- oldest-claimed group first, ties by arrival
-- if a row matched:
UPDATE groups SET last_claimed_at = ? WHERE group_id = ?    -- record claim time (round-robin)
UPDATE messages SET locked_by = ?, lock_expiry = now+visibility WHERE seq = ?
COMMIT                                              -- (ROLLBACK on any error)
```

Sorting by `g.last_claimed_at` makes groups that have never been claimed (`0`) always go
first, in the order they arrived; `last_claimed_at` is set to `now` **on claim** so a
just-serviced group yields to others. The query walks `groups` in that order on its index and
stops at the first group with nothing in flight, taking its head (oldest message) — so a claim
costs the same however long the backlog is. `AND g.priority >= ?` skips groups below the priority floor (pass `min_priority=0` for
normal operation). Returns `Lease(seq, group_id, event)` or `None` when nothing is deliverable.

## Operations

```text
publish(group_id, event, priority=0)
    # INSERT INTO messages (group_id, event) VALUES (?, ?)
    # INSERT OR IGNORE INTO groups (group_id, priority) VALUES (?, ?)  -- first publish sets priority
claim(worker_id, visibility, min_priority=0)  # the BEGIN IMMEDIATE select-then-lease above
ack(lease)
    # DELETE FROM messages WHERE seq = ?
    # DELETE FROM groups WHERE group_id = ? AND NOT EXISTS (SELECT 1 FROM messages WHERE group_id = ?)
nack(lease, delay=0)          # delay>0  -> UPDATE locked_by="__parked__", lock_expiry=now+delay  (park)
                              # delay==0 -> UPDATE locked_by=NULL, lock_expiry=NULL              (retry now)
close()                       # close the connection
```

`ack` removes the message and drops the group row when no messages remain. `nack(delay>0)` **parks**
the message (`_PARKED` sentinel keeps its group blocked) until the delay elapses — the portable
queue-park the [control plane](../control-plane) uses for a suspended group. `nack(0)` frees it
for immediate retry (e.g. after a `StoreConflict`).

## FIFO

Delivery within a group is `ORDER BY seq` (oldest first), and a group is advanced by one consumer
at a time, so per-group order is preserved.

## Async twin

`AsyncSqliteTransport` (`aio_transport/sqlite.py`) mirrors this over **aiosqlite** — every cursor
op awaited, the same hand-driven `BEGIN IMMEDIATE`/`COMMIT`. A SQLite transaction belongs to the
connection, not to the coroutine, and aiosqlite only runs one *statement* at a time — while the
async worker claims on its main loop, its in-flight tasks ack/nack/publish on the same
connection. Each method therefore holds an `asyncio.Lock` for its whole duration, so one
coroutine's `BEGIN IMMEDIATE` never lands inside (or is rolled back by) another's transaction,
and the multi-statement claim stays atomic.

## When to pick it

The global write-lock means claims **serialize** — perfect for one host or a shared volume, but
it won't let workers claim *different* groups in parallel. For that, use
[Postgres](postgres) (`SKIP LOCKED`) or [Redis](redis). See the [transports hub](../transports)
and [distribution](../distribution).

Keep the queue in a **different file from the [SQLite store](../stores/sqlite)**: in one file the
store's commits and the queue's claims and acks wait for the same lock. And run **one worker
process**: more don't add throughput, and SQLite's lock isn't fair between processes, so one or two
of them end up doing almost all the work. Numbers in
[`bench/RESULTS.md`](https://github.com/acasadom/harel/blob/main/bench/RESULTS.md).
