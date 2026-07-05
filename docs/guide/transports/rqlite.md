# RqliteTransport — Raft-replicated queue

A multi-machine queue on **rqlite** (distributed SQLite over Raft), spoken over HTTP. rqlite
serializes every write through the Raft leader, so — like SQLite's write-lock — the per-group
exclusivity selection is race-free in a **single statement**: `claim` leases the oldest deliverable
message with a unique token in one `UPDATE`, then reads that row back by token. `from_url` retries
until rqlite is up and has elected a leader. `requests` is an optional extra.

## HTTP helpers

```text
_execute(statements)        -> POST /db/execute   (writes; raises on any per-result "error")
_query(sql, params)         -> POST /db/query?level=strong   (reads; linearizable, via the leader)
```

Statements are sent as `[sql, *params]`. `level=strong` makes reads go through the leader so a
claim's read-back sees its own write.

## Schema

```text
CREATE TABLE IF NOT EXISTS messages (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order + the Lease handle
  group_id     TEXT NOT NULL,                      -- the execution id (the exclusivity group)
  event        TEXT NOT NULL,                      -- the Event JSON
  locked_by    TEXT,                               -- lease token (worker_id:uuid) / "__parked__" / NULL
  lock_expiry  REAL)                               -- lease/park deadline; NULL/0 when free

CREATE TABLE IF NOT EXISTS groups (
  group_id        TEXT PRIMARY KEY,   -- one row per group that has messages
  last_claimed_at REAL NOT NULL DEFAULT 0.0,  -- epoch of last claim (0 = never claimed)
  priority        INT  NOT NULL DEFAULT 0)    -- set on first publish; 0–4
```

## claim — one serialized UPDATE, then read back

```text
token = "{worker_id}:{uuid}"
UPDATE messages SET locked_by = token, lock_expiry = now+visibility
  WHERE seq = (
    SELECT m.seq FROM messages m JOIN groups g ON g.group_id = m.group_id
    WHERE (m.locked_by IS NULL OR m.lock_expiry < ?)            -- free / lease lapsed (recovery)
      AND m.group_id NOT IN (                                   -- group has nothing in flight
        SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?)
      AND g.priority >= ?                                       -- priority floor (min_priority)
    ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1)         -- oldest-claimed group first (round-robin)
-- if rows_affected == 0: nothing claimable -> return None
SELECT seq, group_id, event FROM messages WHERE locked_by = token   -- read our leased row back
UPDATE groups SET last_claimed_at = ? WHERE group_id = ?            -- record claim time (round-robin)
return Lease(seq, group_id, event, token=token)
```

The single `UPDATE` does the select-then-lease atomically (Raft serializes it cluster-wide, so no
two workers lease the same group). Sorting by `g.last_claimed_at ASC` provides round-robin: groups
never yet claimed (0) come before recently-processed ones. `g.priority >= ?` is the priority floor
(pass 0 for normal operation). The `token` then identifies exactly the row we leased on the
read-back.

## Operations

```text
publish(group_id, event, priority=0)
    # INSERT INTO messages (group_id, event) VALUES (?, ?)
    # INSERT OR IGNORE INTO groups (group_id, priority) VALUES (?, ?)  -- first publish sets priority
claim(worker_id, visibility, min_priority=0)  # the serialized UPDATE + read-back above
ack(lease)    # DELETE FROM messages WHERE seq = ?
              # DELETE FROM groups WHERE group_id=? AND NOT EXISTS (SELECT 1 FROM messages WHERE group_id=?)
nack(lease, delay=0)          # delay>0 -> UPDATE locked_by="__parked__", lock_expiry=now+delay (park)
                              # delay==0 -> UPDATE locked_by=NULL, lock_expiry=0               (retry now)
close()                       # close the HTTP session
```

`ack` removes the message and drops the group row when no messages remain. `nack(delay>0)` parks
(the `_PARKED` sentinel keeps the group blocked until the delay passes); `nack(0)` frees it for
immediate retry.

## FIFO

`ORDER BY seq` within a group, one consumer at a time → order preserved.

## Async twin

`AsyncRqliteTransport` mirrors this over `httpx.AsyncClient` — the same single serialized `UPDATE`
+ read-back, every HTTP call awaited.

## When to pick it

HA queue without running Postgres — Raft replicates every message. The cost is consensus + HTTP +
fsync per write, so it's the slowest transport (mirror of the [RqliteStore](../stores/rqlite)
tradeoff). See the [transports hub](../transports) and [distribution](../distribution).
