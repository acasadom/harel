# Transports — backend reference

A **transport** is the queue that lets *many* workers share the load while guaranteeing each
execution is advanced by only one worker at a time. This page is the detailed, per-backend
reference: how each one provides that guarantee and **how it lays the queue out**. For the
concepts (the seam, running workers, scaling) start with [distribution](distribution); the worker
picks a backend from [`STM_TRANSPORT_BACKEND`](distribution).

## The invariant: single active consumer per group

The transport is a FIFO queue partitioned into **groups**, where `group_id =
execution_id`. The one property the whole design rests on:

> at most one message **per group** is in flight at any moment.

That is what makes concurrency safe — events for one execution are processed **in order, by one
worker**, even with a pool of workers running — and it upholds the store's single-writer
invariant (the store CAS is the backstop if a lease expires). Different groups are claimed by
different workers in parallel, which is where the throughput comes from.

## The contract every backend implements

`Transport` is a small `Protocol` ([`transport/_base.py`](../../src/harel/engine/transport/_base.py)) —
each backend is a sibling module under `harel/engine/transport/`, with a native-async twin under
`harel/engine/aio_transport/`:

| Method | Purpose |
| --- | --- |
| `publish(group_id, event)` | enqueue `event` in `group_id`'s FIFO |
| `claim(worker_id, visibility)` | lease the oldest message of some group with **nothing in flight**, for `visibility` seconds; `None` if nothing is deliverable now |
| `ack(lease)` | the message was handled — remove it, freeing its group |
| `nack(lease, delay=0)` | return it to the queue: `delay=0` re-claimable now (retry); `delay>0` **parks** it (and blocks its group) until `delay` passes |
| `close()` | release the connection/client |

Two mechanisms recur:

- **The lease / visibility timeout.** `claim` marks a group in-flight until `now + visibility`.
  If the worker `ack`s, the message is gone; if the worker **dies**, the lease expires and the
  message becomes claimable again — crash recovery with no separate sweeper. (Like any
  lease-based queue, a lease that expires mid-`ack` can let two workers briefly touch one group;
  the store CAS is the backstop.) A `Lease` carries the `group_id`, the `event`, and a handle to
  identify it on ack/nack — a `seq` (row id) or a fencing `token`.
- **Parking (`nack` with delay).** The control plane parks a suspended group's message rather
  than spinning a worker on it — see [control plane](control-plane). The `_PARKED` sentinel marks
  a message that is non-claimable until its delay elapses.

Backends differ only in *how* they enforce per-group exclusivity: a database that has a cheap
serialization primitive (SQLite's write-lock, Postgres's row-lock, SQS's native `MessageGroupId`)
leans on it; the others (Redis, Mongo) build a per-group lock record by hand, indexed by when the
group next becomes claimable so `claim` reads only the few due groups instead of scanning.

---

## `InMemoryTransport` — in-process

A list of message dicts with a `locked_by`/`lock_expiry` lease, for tests and single-process
runs. `claim` skips groups with a live lock and leases the oldest message of a free group;
`ack` removes it; `nack` clears the lock (retry now) or sets `lock_expiry = now + delay` (park).
Same lease semantics as the durable backends, no IO.

## `SqliteTransport` — durable, one host

A `messages(seq, group_id, event, locked_by, lock_expiry)` table. `claim` runs inside **`BEGIN
IMMEDIATE`** (SQLite's global write-lock), so selecting and leasing the oldest message is atomic:

```text
SELECT the oldest message whose group has nothing in flight
  WHERE (locked_by IS NULL OR lock_expiry < now)
    AND group_id NOT IN (SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= now)
UPDATE that row  SET locked_by=worker_id, lock_expiry=now+visibility
```

`ack` deletes the row; `nack` parks it (`locked_by=_PARKED`, `lock_expiry=now+delay`) or frees it
(`NULL`). The expiry lease recovers a message a crashed worker held. The global write-lock means
claims serialize — fine for one host / a shared volume.

## `LibsqlTransport` — libSQL / Turso *(experimental)*

Identical to `SqliteTransport` (`BEGIN IMMEDIATE` + lease over a `messages` table), over the
`libsql` driver — so a local file, a `sqld` server, or a Turso replica. Same **experimental**
caveat as the libSQL store: local-file tested in-process, the Turso/`sqld` path unvalidated
against a real account.

## `RedisTransport` — pure-Redis, no global lock

Hand-built per-group exclusivity, so claims don't serialize on a global lock:

```text
q:{group}        -> list (RPUSH publish / LPOP claim), the per-group FIFO
lock:{group}     -> SET NX PX  — the group lock AND the fencing token; its TTL is the lease
ready            -> ZSET of group_id scored by "available_at" (when next claimable)
```

`claim` reads only the **few lowest-scored due groups** from the `ready` ZSET
(`zrangebyscore -inf now`, `O(log N + K)`, not a scan), then takes the group's lock with `SET
lock:{G} NX PX` (only one worker wins; the TTL auto-releases it if the worker dies). Leasing
bumps the group's `ready` score to `now + visibility`, so other claimers skip it **and** it
reappears on its own once the lease expires (recovery). `ack` `LPOP`s and releases the lock;
`nack` re-readies now or parks (`delay`). Pairs with `RedisStore` for a pure-Redis stack.

## `PostgresTransport` — per-group row, `SKIP LOCKED`

A `messages` queue table plus a **`transport_groups(group_id, locked_by, lock_expiry)`** row per
group carrying the lease. `claim` leases one claimable group with:

```text
UPDATE transport_groups SET locked_by=token, lock_expiry=now+visibility
WHERE group_id = (
  SELECT group_id FROM transport_groups
  WHERE locked_by IS NULL OR lock_expiry < now
  ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1)
```

Postgres's row lock makes the per-group selection race-free, and **`SKIP LOCKED`** lets
concurrent workers lease *different* groups in parallel — claims don't serialize (the earlier
design took a global `pg_advisory_xact_lock` and didn't scale; this is the same per-group +
`SKIP LOCKED` approach DBOS uses). `ack` removes the head message and frees the group (fenced by
the token); `nack` frees it now or parks. Unify store + transport on one Postgres for a no-Redis
stack.

## `RqliteTransport` — Raft-replicated queue

A `messages` table on rqlite. With no interactive transaction, `claim` is **one serialized
`UPDATE`** that leases the oldest deliverable message (same "group has nothing in flight"
subquery as SQLite) with a unique token, then reads that row back by token. Raft serializes the
claims across the cluster; `ack` deletes, `nack` parks/frees. HA at the cost of consensus
latency.

## `MongoTransport` — per-group ready-index document

A `messages` collection plus a **`locks` collection** that is the ready-index and lock in one:

```text
locks/{_id: group}: { available_at: epoch when next claimable (0 = now), token: current lease }
```

`claim` reads only the few lowest-`available_at` groups due now
(`find({available_at: {$lte: now}}).sort(available_at).limit(K)` — `O(log N + K)`, not a scan),
then **atomically leases** one with a `find_one_and_update` whose filter still requires
`available_at <= now` (so only one worker wins the race), bumping `available_at = now +
visibility`. A dead worker's group reappears when its lease expires (no separate sweep). `ack`
re-readies the group (`available_at=0`) or drops it if empty; `nack` re-readies now or parks.
Pairs with `MongoStore` for an all-Mongo stack.

## `SqsTransport` — AWS SQS FIFO (native fit)

SQS FIFO gives the invariant **natively**: a queue's `MessageGroupId` already guarantees only one
message per group is in flight, and the receive **visibility timeout *is* the lease**. So the
mapping is thin:

```text
publish  = send_message(MessageGroupId=group_id, MessageDeduplicationId=uuid)
claim    = receive_message(VisibilityTimeout=visibility) -> the ReceiptHandle is the lease token
ack      = delete_message(ReceiptHandle)
nack     = change_message_visibility(ReceiptHandle, VisibilityTimeout=delay)   # 0 = retry now
```

No lock table, no ready-index — AWS runs the exclusivity. Runs on LocalStack (no AWS account);
the all-AWS partner of `DynamoDBStore`. (Note: SQS can't purge/prioritise, which is why harel's
cooperative cancel drains the backlog as no-ops rather than relying on a queue jump — see
[control plane](control-plane).)

## Store and transport are independent

They are separate seams — mix freely (e.g. Postgres store + Redis transport) or **unify on one
backend** for a simpler stack: all-Postgres, all-rqlite, all-Mongo, or all-libSQL all need no
Redis. The async twins under `harel/engine/aio_transport/` are what the async worker uses; the
sync classes here back the embedded `DistributedRunner` façade and tests.
