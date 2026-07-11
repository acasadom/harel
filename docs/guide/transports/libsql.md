# LibsqlTransport — libSQL / Turso (experimental)

A clone of [`SqliteTransport`](sqlite) over the `libsql` driver (Turso's SQLite fork): the same
`messages` + `groups` tables, the same **`BEGIN IMMEDIATE`** claim, the same lease, round-robin
fairness and priority filtering. What changes is *where the queue lives* — a local file, a `sqld`
server, or an embedded Turso replica.

**Experimental:** the local-file path is covered in-process by the test suite; the Turso/`sqld`
embedded-replica path is wired but not yet validated against a real account.

## Connection modes

The constructor opens `libsql.connect(database, isolation_level=None, _check_same_thread=False,
…)`. With `sync_url=` (+ `auth_token=`, optional `sync_interval=`) it is an **embedded replica**
against a Turso/`sqld` primary; without it, a plain **local file**. `isolation_level=None` is
autocommit, so `claim` drives `BEGIN IMMEDIATE`/`COMMIT` by hand (exactly as SqliteTransport).

## Schema

Two tables (identical to SqliteTransport):

```text
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order + the Lease handle
  group_id     TEXT NOT NULL,                      -- the execution id (exclusivity group)
  event        TEXT NOT NULL,                      -- the Event JSON
  locked_by    TEXT,                               -- worker id / "__parked__" / NULL
  lock_expiry  REAL                                -- lease/park deadline; NULL when free
)

CREATE TABLE IF NOT EXISTS groups (
  group_id        TEXT PRIMARY KEY,           -- one row per group that has messages
  last_claimed_at REAL NOT NULL DEFAULT 0.0,  -- epoch of last claim (0 = never claimed) — round-robin
  priority        INT  NOT NULL DEFAULT 0     -- set on first publish; 0–4
)
```

`(locked_by, lock_expiry)` is the **lease**. The `groups` table drives **round-robin fairness**
and **priority filtering**: `claim` sorts by `last_claimed_at ASC` (oldest-claimed first) and
filters by `priority >= min_priority`.

## The claim — atomic select-then-lease with round-robin and priority

Identical to SqliteTransport: under `BEGIN IMMEDIATE` (libSQL serializes writers, so the per-group
selection is race-free) select the oldest-claimed deliverable group and lease it:

```text
BEGIN IMMEDIATE
SELECT m.seq, m.group_id, m.event
  FROM messages m JOIN groups g ON g.group_id = m.group_id
  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?)          -- message free / lease lapsed (recovery)
    AND m.group_id NOT IN (                                  -- group has nothing in flight
      SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?)
    AND g.priority >= ?                                      -- priority floor (min_priority)
  ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1         -- oldest-claimed group first (round-robin)
-- if a row matched:
UPDATE groups SET last_claimed_at = ? WHERE group_id = ?    -- record claim time (round-robin)
UPDATE messages SET locked_by = ?, lock_expiry = now+visibility WHERE seq = ?
COMMIT                                              -- ROLLBACK on error
```

Sorting by `g.last_claimed_at ASC` makes never-claimed groups (`0`) go first; the group's
`last_claimed_at` is set to `now` **on claim** so a just-serviced group yields to others.
`AND g.priority >= ?` skips groups below the floor (`min_priority=0` for normal operation).
Returns `Lease(seq, group_id, event)` or `None`.

## Operations

```text
publish(group_id, event, priority=0)
    # INSERT INTO messages (group_id, event) VALUES (?, ?)
    # INSERT OR IGNORE INTO groups (group_id, priority) VALUES (?, ?)  -- first publish sets priority
claim(worker_id, visibility, min_priority=0)  # the BEGIN IMMEDIATE select-then-lease above
ack(lease)
    # DELETE FROM messages WHERE seq = ?
    # DELETE FROM groups WHERE group_id = ? AND NOT EXISTS (SELECT 1 FROM messages WHERE group_id = ?)
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
