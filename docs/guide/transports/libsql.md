# LibsqlTransport — libSQL / Turso (experimental)

A clone of [`SqliteTransport`](sqlite) over the `libsql` driver (Turso's SQLite fork): the same
`messages` table, the same **`BEGIN IMMEDIATE`** claim, the same lease. What changes is *where the
queue lives* — a local file, a `sqld` server, or an embedded Turso replica.

**Experimental:** the local-file path is covered in-process by the test suite; the Turso/`sqld`
embedded-replica path is wired but not yet validated against a real account.

## Connection modes

The constructor opens `libsql.connect(database, isolation_level=None, _check_same_thread=False,
…)`. With `sync_url=` (+ `auth_token=`, optional `sync_interval=`) it is an **embedded replica**
against a Turso/`sqld` primary; without it, a plain **local file**. `isolation_level=None` is
autocommit, so `claim` drives `BEGIN IMMEDIATE`/`COMMIT` by hand (exactly as SqliteTransport).

## Schema

```text
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order + the Lease handle
  group_id     TEXT NOT NULL,                      -- the execution id (exclusivity group)
  event        TEXT NOT NULL,                      -- the Event JSON
  locked_by    TEXT,                               -- worker id / "__parked__" / NULL
  lock_expiry  REAL                                -- lease/park deadline; NULL when free
)
```

## The claim — atomic select-then-lease

Identical to SqliteTransport: under `BEGIN IMMEDIATE` (libSQL serializes writers, so the per-group
selection is race-free), select the oldest deliverable message and lease it:

```text
BEGIN IMMEDIATE
SELECT seq, group_id, event FROM messages m
  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?)
    AND m.group_id NOT IN (
      SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?)
  ORDER BY m.seq LIMIT 1
UPDATE messages SET locked_by = ?, lock_expiry = now+visibility WHERE seq = ?
COMMIT                                  -- ROLLBACK on error
```

The message must be free (`locked_by IS NULL OR lock_expiry < now` — an expired lease is the crash
recovery), **and** its group must have nothing in flight (the `NOT IN` subquery). Returns
`Lease(seq, group_id, event)` or `None`.

## Operations

```text
publish(group_id, event)      # INSERT INTO messages (group_id, event) VALUES (?, ?)
claim(worker_id, visibility)  # the BEGIN IMMEDIATE select-then-lease
ack(lease)                    # DELETE FROM messages WHERE seq = ?
nack(lease, delay=0)          # delay>0 -> locked_by="__parked__", lock_expiry=now+delay (park)
                              # delay==0 -> locked_by=NULL, lock_expiry=NULL (retry now)
close()                       # close the connection
```

## Async twin

`AsyncLibsqlTransport` is **not** native async: the `libsql` package is synchronous, so it wraps
the sync `LibsqlTransport` and off-loads each call with `asyncio.to_thread`, serialized by an
`asyncio.Lock` (one connection, one op at a time — the `BEGIN IMMEDIATE` claim is single-writer
anyway). Build with `await AsyncLibsqlTransport.create(database, sync_url=…, auth_token=…)`.

## When to pick it

Like [SqliteTransport](sqlite) (claims serialize on the write-lock), but libSQL-hosted — a path
to a managed distributed SQLite (Turso) later. Experimental today. See the
[transports hub](../transports) and [distribution](../distribution).
