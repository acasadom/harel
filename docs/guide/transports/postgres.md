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
  lock_expiry DOUBLE PRECISION)     -- epoch when the lease/park ends, else NULL
CREATE INDEX transport_groups_claimable ON transport_groups (lock_expiry)
```

The messages and the lease are **split**: `transport_messages` is the queue; `transport_groups`
holds *exactly one row per active group* with its lease. Claiming leases the **group row**, not a
message — that is what lets `SKIP LOCKED` pick a different group per worker.

Schema setup also creates the `harel_claim` / `harel_ack` PL/pgSQL functions, idempotently and
under a `pg_advisory_xact_lock` so several workers opening connections at once don't collide on
`CREATE OR REPLACE FUNCTION` (which rewrites `pg_proc`).

## publish

```text
INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)
INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL)
  ON CONFLICT (group_id) DO NOTHING       -- ready the group only if new; never reset a live lease
```

`ON CONFLICT DO NOTHING` is the analogue of Redis's `ZADD … NX`: a publish into an in-flight or
parked group must not clear its lease.

## claim — one server-side function: `harel_claim`

`claim` is one round-trip: a `SELECT … FROM harel_claim(now, lease, token)`. The PL/pgSQL function
does the lease (`FOR UPDATE SKIP LOCKED`), the head fetch, and the stale-empty-group cleanup
server-side, returning `(group_id, seq, event)`:

```text
SELECT group_id, seq, event FROM harel_claim(now, now+visibility, token)   -- ONE round-trip

-- harel_claim, server-side:
loop:
  UPDATE transport_groups SET locked_by = token, lock_expiry = lease
    WHERE group_id = (
      SELECT group_id FROM transport_groups
      WHERE locked_by IS NULL OR lock_expiry < now          -- free, or its lease lapsed (recovery)
      ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1)      -- lock THIS row; others skip it
    RETURNING group_id INTO g
  if g IS NULL:  return                                      -- nothing claimable
  RETURN QUERY SELECT group_id, seq, event FROM transport_messages
               WHERE group_id = g ORDER BY seq LIMIT 1       -- the head
  if FOUND:  return
  DELETE FROM transport_groups WHERE group_id = g AND locked_by = token   -- stale empty group, retry
```

`FOR UPDATE SKIP LOCKED` is the crux: Postgres row-locks the selected group row for the
transaction, and a concurrent claimer **skips** a locked row and takes a *different* group — so N
workers lease N groups in parallel, no global serialization. The lease was **already race-free**
this way; folding the lease + head fetch + stale cleanup into one server-side function is purely
about cutting round-trips (the old version did the UPDATE-lease then a separate `SELECT` for the
head, plus a loop for stale-empty groups — 2+ round-trips). (An even earlier design took a single
global `pg_advisory_xact_lock`, which made every claim wait on one lock — a bottleneck regardless
of worker count. This per-group + `SKIP LOCKED` design is the same one DBOS uses for its Postgres
queue.) The head message is returned but not removed.

## ack — one server-side function; nack — fenced by the token

`ack` is also one round-trip: `SELECT harel_ack(group, seq, token)`. The PL/pgSQL function fences
on the token, deletes the head message, and frees the lock server-side (the old version did a
`SELECT`-owns + `DELETE` + `UPDATE`, ~3 round-trips). `nack` stays a single fenced `UPDATE`:

```text
ack(lease):   SELECT harel_ack(group_id, seq, token)        -- ONE round-trip, server-side:
                if EXISTS(transport_groups WHERE group_id=? AND locked_by=token):    # fence
                   DELETE FROM transport_messages WHERE seq = ?                       # remove head
                   UPDATE transport_groups SET locked_by=NULL, lock_expiry=NULL       # free the group
                     WHERE group_id=? AND locked_by=token

nack(lease, delay):
   if delay>0:  UPDATE transport_groups SET lock_expiry = now+delay                 # park (keep token)
                  WHERE group_id=? AND locked_by=token
   else:        UPDATE transport_groups SET locked_by=NULL, lock_expiry=NULL        # retry now
                  WHERE group_id=? AND locked_by=token
```

Every write is fenced on `locked_by = token`, so a worker whose lease expired can't free or park a
group another worker has since leased. `nack(delay>0)` parks by pushing `lock_expiry` into the
future while keeping the token (the still-present head isn't re-claimed until then).

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
