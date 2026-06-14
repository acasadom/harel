# SqliteTransport — durable, one host

A durable queue over stdlib `sqlite3` (WAL). The whole point: `claim` runs inside a **`BEGIN
IMMEDIATE`** transaction, so SQLite's global write-lock serializes claims across processes — the
per-group exclusivity selection is then race-free with plain SQL, no row or advisory locks. The
lease (`lock_expiry`) recovers a message a crashed worker was holding. The connection is opened
with `isolation_level=None` (autocommit) so the code drives `BEGIN IMMEDIATE`/`COMMIT` by hand in
`claim`; `PRAGMA busy_timeout=5000` makes a contended writer wait for the lock instead of erroring.

## Schema

One table:

```text
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- monotonic id: FIFO order + the Lease handle
  group_id     TEXT NOT NULL,                      -- the execution id (the exclusivity group)
  event        TEXT NOT NULL,                      -- the Event JSON (event.model_dump_json())
  locked_by    TEXT,                               -- worker id while leased / "__parked__" / NULL when free
  lock_expiry  REAL                                -- epoch when the lease (or park) ends; NULL when free
)
```

`(locked_by, lock_expiry)` is the **lease**: a message is *in flight* while `locked_by IS NOT NULL
AND lock_expiry >= now`. `seq` is both the FIFO key (ordered delivery within a group) and the
handle a `Lease` carries for `ack`/`nack`.

## The claim — atomic select-then-lease

```text
BEGIN IMMEDIATE                                   -- take SQLite's write-lock: claims serialize
SELECT seq, group_id, event FROM messages m
  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?)          -- this message is free (or its lease lapsed)
    AND m.group_id NOT IN (                                 -- and its GROUP has nothing in flight
      SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?)
  ORDER BY m.seq LIMIT 1                                     -- oldest deliverable message
-- if a row matched:
UPDATE messages SET locked_by = ?, lock_expiry = now+visibility WHERE seq = ?
COMMIT                                              -- (ROLLBACK on any error)
```

Two predicates do the work: the message itself must be free (`locked_by IS NULL OR lock_expiry <
now` — the second half is the **crash recovery**, an expired lease is treated as free), **and**
the `NOT IN (… in-flight groups …)` subquery enforces the single-active-consumer-per-group
invariant. Because the whole select-then-lease runs under `BEGIN IMMEDIATE`, no two workers can
lease the same group — SQLite's write-lock is the serialization primitive (the same role
Postgres's row lock or Redis's `SET NX` play elsewhere). Returns `Lease(seq, group_id, event)` or
`None` when nothing is deliverable.

## Operations

```text
publish(group_id, event)      # INSERT INTO messages (group_id, event) VALUES (?, ?)
claim(worker_id, visibility)  # the BEGIN IMMEDIATE select-then-lease above
ack(lease)                    # DELETE FROM messages WHERE seq = ?  (the group is then free)
nack(lease, delay=0)          # delay>0  -> UPDATE locked_by="__parked__", lock_expiry=now+delay  (park)
                              # delay==0 -> UPDATE locked_by=NULL, lock_expiry=NULL              (retry now)
close()                       # close the connection
```

`ack` removes the message; the group's next message becomes claimable on the next `claim`.
`nack(delay>0)` **parks** the message (`_PARKED` sentinel keeps its group blocked) until the delay
elapses — the portable queue-park the [control plane](../control-plane) uses for a suspended
group. `nack(0)` frees it for immediate retry (e.g. after a `StoreConflict`).

## FIFO

Delivery within a group is `ORDER BY seq` (oldest first), and a group is advanced by one consumer
at a time, so per-group order is preserved.

## Async twin

`AsyncSqliteTransport` (`aio_transport/sqlite.py`) mirrors this over **aiosqlite** — every cursor
op awaited, the same hand-driven `BEGIN IMMEDIATE`/`COMMIT`. aiosqlite serializes a connection's
operations on its own worker thread, so the multi-statement claim stays atomic.

## When to pick it

The global write-lock means claims **serialize** — perfect for one host or a shared volume, but
it won't let workers claim *different* groups in parallel. For that, use
[Postgres](postgres) (`SKIP LOCKED`) or [Redis](redis). See the [transports hub](../transports)
and [distribution](../distribution).
