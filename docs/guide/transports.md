# Transports

A **transport** is the queue that lets *many* workers share the load while guaranteeing each
execution is advanced by only one worker at a time. This is the hub for the per-backend
reference: the invariant they all uphold, the contract, the shared lease/parking mechanics — and
a page per backend with its **exact queue layout and every operation** (the table/keys/documents,
how `claim` leases a group, how `ack`/`nack` work, and why). For the concepts (the seam, running
workers, scaling) start with [distribution](distribution); to select one at the worker see
[`STM_TRANSPORT_BACKEND`](distribution).

## The invariant: single active consumer per group

The transport is a FIFO queue partitioned into **groups**, where `group_id = execution_id`. The
one property the whole design rests on:

> at most one message **per group** is in flight at any moment.

That is what makes concurrency safe — events for one execution are processed **in order, by one
worker**, even with a pool of workers running — and it upholds the store's single-writer
invariant (the store CAS is the backstop if a lease expires). Different groups are claimed by
different workers in parallel, which is where the throughput comes from.

## The contract every backend implements

`Transport` is a small `Protocol` ([`transport/_base.py`](https://github.com/acasadom/harel/blob/main/src/harel/engine/transport/_base.py)) —
each backend is a sibling module under `harel/engine/transport/`, with a twin under
`harel/engine/aio_transport/`:

| Method | Purpose |
| --- | --- |
| `publish(group_id, event)` | enqueue `event` in `group_id`'s FIFO |
| `claim(worker_id, visibility)` | lease the oldest message of some group with **nothing in flight**, for `visibility` seconds; `None` if nothing is deliverable now |
| `ack(lease)` | the message was handled — remove it, freeing its group |
| `nack(lease, delay=0)` | return it to the queue: `delay=0` re-claimable now (retry); `delay>0` **parks** it (and blocks its group) until `delay` passes |
| `close()` | release the connection/client |

Two mechanisms recur across the backends, which each per-backend page then spells out exactly:

- **The lease / visibility timeout.** `claim` marks a group in-flight until `now + visibility`. If
  the worker `ack`s, the message is gone; if the worker **dies**, the lease expires and the
  message becomes claimable again — crash recovery with no separate sweeper. (Like any lease-based
  queue, a lease that expires mid-`ack` can let two workers briefly touch one group; the store CAS
  is the backstop.) A `Lease` carries the `group_id`, the `event`, and a handle to identify it on
  ack/nack — a `seq` (row id) or a fencing `token`.
- **Parking (`nack` with delay).** The control plane parks a suspended group's message rather than
  spinning a worker on it — see [control plane](control-plane). The `_PARKED` sentinel marks a
  message that is non-claimable until its delay elapses.

Backends differ only in *how* they enforce per-group exclusivity: a database that has a cheap
serialization primitive (SQLite's write-lock, Postgres's row-lock, SQS's native `MessageGroupId`)
leans on it; the others (Redis, Mongo) build a per-group lock record by hand, indexed by when the
group next becomes claimable so `claim` reads only the few due groups instead of scanning.

## The backends

Each backend has its own page with the full queue layout and every operation broken down with the
real SQL/commands and the reasoning:

```{toctree}
:maxdepth: 1

transports/inmemory
transports/sqlite
transports/libsql
transports/redis
transports/postgres
transports/rqlite
transports/mongo
transports/sqs
```

| Backend | Per-group exclusivity mechanism |
| --- | --- |
| [InMemoryTransport](transports/inmemory) | in-process list + lock (tests, single process) |
| [SqliteTransport](transports/sqlite) | `BEGIN IMMEDIATE` (global write-lock) + lease |
| [LibsqlTransport](transports/libsql) | `BEGIN IMMEDIATE` + lease over libSQL *(experimental)* |
| [RedisTransport](transports/redis) | `SET NX PX` group-lock + a `ready` ZSET index |
| [PostgresTransport](transports/postgres) | per-group row claimed `FOR UPDATE SKIP LOCKED` |
| [RqliteTransport](transports/rqlite) | one serialized `UPDATE` (Raft orders it) |
| [MongoTransport](transports/mongo) | per-group ready-index/lock doc + atomic `find_one_and_update` |
| [SqsTransport](transports/sqs) | SQS FIFO `MessageGroupId` (native) + visibility-timeout lease |

## Store and transport are independent

They are separate seams — mix freely (e.g. Postgres store + Redis transport) or **unify on one
backend** for a simpler stack: all-Postgres, all-rqlite, all-Mongo, or all-libSQL all need no
Redis. The async twins under `harel/engine/aio_transport/` are what the async worker uses; the
sync classes back the embedded `DistributedRunner` façade and tests. Each per-backend page has an
*Async twin* section.
