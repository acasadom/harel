# PostgresTransport — per-group row, `SKIP LOCKED`

A multi-machine queue on PostgreSQL with no Redis (the classic DB-as-queue). A FIFO message table
plus a **per-group row** carrying the lease; `claim` leases a claimable group with `SELECT … FOR
UPDATE SKIP LOCKED`, so Postgres's row lock makes the per-group selection race-free **and**
concurrent workers lease *different* groups in parallel — claims don't serialize on a global lock.
The connection is injected (`psycopg` optional); `from_dsn` retries so a worker starting alongside
Postgres in compose waits rather than crashing.

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

## publish

```text
INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)
INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL)
  ON CONFLICT (group_id) DO NOTHING       -- ready the group only if new; never reset a live lease
```

`ON CONFLICT DO NOTHING` is the analogue of Redis's `ZADD … NX`: a publish into an in-flight or
parked group must not clear its lease.

## claim — lease a group with `FOR UPDATE SKIP LOCKED`

```text
loop:
  token = "{worker_id}:{uuid}"
  UPDATE transport_groups SET locked_by = token, lock_expiry = now+visibility
    WHERE group_id = (
      SELECT group_id FROM transport_groups
      WHERE locked_by IS NULL OR lock_expiry < now          -- free, or its lease lapsed (recovery)
      ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1)      -- lock THIS row; others skip it
    RETURNING group_id
  if no row:  return None                                    # nothing claimable
  SELECT seq, event FROM transport_messages WHERE group_id = ? ORDER BY seq LIMIT 1   # the head
  if no head:                                                # group drained (last msg already acked)
     DELETE FROM transport_groups WHERE group_id = ? AND locked_by = token;  continue
  return Lease(seq, group_id, event, token=token)
```

`FOR UPDATE SKIP LOCKED` is the crux: Postgres row-locks the selected group row for the
transaction, and a concurrent claimer **skips** a locked row and takes a *different* group — so N
workers lease N groups in parallel, no global serialization. (The earlier design took a single
global `pg_advisory_xact_lock`, which made every claim wait on one lock — a bottleneck regardless
of worker count. This per-group + `SKIP LOCKED` design is the same one DBOS uses for its Postgres
queue.) The head message is returned but not removed.

## ack / nack — fenced by the token

```text
ack(lease):   if EXISTS(transport_groups WHERE group_id=? AND locked_by=token):    # fence
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
`FOR UPDATE SKIP LOCKED`, every statement awaited, so concurrent workers issue real parallel
claims.

## When to pick it

A no-Redis distributed queue that actually scales claims (parallel per-group leasing). Unify store
+ transport on one Postgres for an all-Postgres stack. See the [transports hub](../transports) and
[distribution](../distribution).
