# PostgresTransport — per-group row, `SKIP LOCKED`

A multi-machine queue on PostgreSQL with no Redis (the classic DB-as-queue). A FIFO message table
plus a **per-group row** carrying the lease; `claim` leases a claimable group with `SELECT … FOR
UPDATE SKIP LOCKED`, so Postgres's row lock makes the per-group selection race-free **and**
concurrent workers lease *different* groups in parallel — claims don't serialize on a global lock.
Both `claim` and `ack` now run as **server-side PL/pgSQL functions** (`harel_claim` /
`harel_ack`), each in ONE round-trip — Postgres was round-trip-bound, not lock-contended, so
folding each op's statements into one server call is the win (the lease was already race-free via
`FOR UPDATE SKIP LOCKED`). The connection is injected (`psycopg` optional); `from_dsn` retries so a
worker starting alongside Postgres in compose waits rather than crashing.

## Schema

```text
CREATE TABLE transport_messages (
  seq      BIGSERIAL PRIMARY KEY,   -- monotonic id: FIFO order + the Lease handle
  group_id TEXT NOT NULL,           -- the execution id (the exclusivity group)
  event    TEXT NOT NULL)           -- the Event JSON
CREATE INDEX transport_messages_group ON transport_messages (group_id, seq)   -- head-of-group lookup

CREATE TABLE transport_groups (
  group_id    TEXT PRIMARY KEY,     -- one row per group that has messages
  locked_by   TEXT,                 -- lease token (worker_id:uuid) while in flight, else NULL
  lock_expiry DOUBLE PRECISION,     -- epoch when the lease/park ends (NULL = free; >now = in-flight/parked)
  priority    INT NOT NULL DEFAULT 0)  -- set on first publish (0–4); used for priority filtering
CREATE INDEX transport_groups_claimable ON transport_groups (lock_expiry)
```

The messages and the lease are **split**: `transport_messages` is the queue; `transport_groups`
holds *exactly one row per active group* with its lease and priority. Claiming leases the **group
row**, not a message — that is what lets `SKIP LOCKED` pick a different group per worker.

`lock_expiry` also drives **round-robin fairness**: after `ack`, it is set to `now` (not `NULL`),
so the index naturally sorts recently-processed groups behind fresh ones — `ORDER BY group_id` in
the claim subquery becomes a lexicographic tiebreak only among groups with the same `lock_expiry`.

Schema setup also creates the `harel_claim` / `harel_ack` PL/pgSQL functions, idempotently and
under a `pg_advisory_xact_lock` so several workers opening connections at once don't collide on
`CREATE OR REPLACE FUNCTION` (which rewrites `pg_proc`).

## publish

```text
INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)
INSERT INTO transport_groups (group_id, locked_by, lock_expiry, priority) VALUES (%s, NULL, NULL, %s)
  ON CONFLICT (group_id) DO NOTHING   -- ready the group only if new; never reset a live lease or priority
```

`ON CONFLICT DO NOTHING` is the analogue of Redis's `ZADD … NX`: a publish into an in-flight or
parked group must not clear its lease or overwrite the priority set on first publish.

## claim — one server-side function: `harel_claim`

`claim` is one round-trip: `SELECT … FROM harel_claim(now, lease, token, min_priority)`. The
PL/pgSQL function does the lease (`FOR UPDATE SKIP LOCKED`), priority filtering, the head fetch,
and stale-empty-group cleanup server-side, returning `(group_id, seq, event)`:

```text
SELECT group_id, seq, event FROM harel_claim(now, now+visibility, token, min_priority)   -- ONE round-trip

-- harel_claim(p_now, p_lease, p_token, p_min_priority DEFAULT 0), server-side:
loop:
  UPDATE transport_groups SET locked_by = p_token, lock_expiry = p_lease
    WHERE group_id = (
      SELECT group_id FROM transport_groups
      WHERE (locked_by IS NULL OR lock_expiry < p_now)   -- free, or its lease lapsed (recovery)
        AND priority >= p_min_priority                    -- priority floor
      ORDER BY COALESCE(lock_expiry, 0) ASC, group_id    -- oldest-claimed first (round-robin)
        FOR UPDATE SKIP LOCKED LIMIT 1)                  -- lock THIS row; others skip it
    RETURNING group_id INTO g
  if g IS NULL:  return                                   -- nothing claimable at this priority
  RETURN QUERY SELECT group_id, seq, event FROM transport_messages
               WHERE group_id = g ORDER BY seq LIMIT 1    -- the head
  if FOUND:  return
  DELETE FROM transport_groups WHERE group_id = g AND locked_by = p_token  -- stale empty group, retry
```

`FOR UPDATE SKIP LOCKED` is the crux: Postgres row-locks the selected group row for the
transaction, and a concurrent claimer **skips** a locked row and takes a *different* group — so N
workers lease N groups in parallel, no global serialization. `ORDER BY COALESCE(lock_expiry, 0) ASC`
drives round-robin: groups with `lock_expiry IS NULL` (never claimed) sort as 0 and come first;
groups whose `lock_expiry = now` after a recent ack sort last. `p_min_priority DEFAULT 0` means
existing call sites without the argument are unaffected.

## ack — one server-side function; nack — fenced by the token

`ack` is also one round-trip: `SELECT harel_ack(group, seq, token, now)`. The function fences on
the token, deletes the head message, and sets `lock_expiry = now` (round-robin) rather than
`NULL`, so the group row stays in the index as a recently-claimed row and fresh groups sort ahead
of it. `nack` stays a single fenced `UPDATE`:

```text
ack(lease):   SELECT harel_ack(group_id, seq, token, now)   -- ONE round-trip, server-side:
                if EXISTS(transport_groups WHERE group_id=? AND locked_by=token):  # fence
                   DELETE FROM transport_messages WHERE seq = ?                     # remove head
                   if messages remain for group:
                     UPDATE transport_groups SET locked_by=NULL, lock_expiry=now    # free + round-robin
                       WHERE group_id=? AND locked_by=token
                   else:
                     DELETE FROM transport_groups WHERE group_id=? AND locked_by=token  # drain: drop row

nack(lease, delay):
   if delay>0:  UPDATE transport_groups SET lock_expiry = now+delay                # park (keep token)
                  WHERE group_id=? AND locked_by=token
   else:        UPDATE transport_groups SET locked_by=NULL, lock_expiry=NULL       # retry now
                  WHERE group_id=? AND locked_by=token
```

When messages remain, `lock_expiry = now` on ack is the round-robin signal: the claim subquery's
`ORDER BY COALESCE(lock_expiry, 0) ASC` sorts recently-processed groups (expiry = recent epoch)
after fresh ones (expiry = NULL → coalesced to 0). When the group is drained, its row is deleted
entirely so the priority resets on the next publish (no stale priority for recycled execution IDs).
Every write is fenced on `locked_by = token`, so an expired-lease worker can't disturb a group
another worker has taken.

## FIFO

`ORDER BY seq` within a group, one consumer per group at a time → per-group order preserved.

## Async twin

`AsyncPostgresTransport` mirrors this over an async `psycopg` connection — same per-group row +
`FOR UPDATE SKIP LOCKED`, the same server-side `harel_claim` / `harel_ack` functions, every
statement awaited, so concurrent workers issue real parallel claims.

## When to pick it

A no-Redis distributed queue that actually scales claims (parallel per-group leasing). Unify store
+ transport on one Postgres for an all-Postgres stack. See the [transports hub](../transports) and
[distribution](../distribution).
