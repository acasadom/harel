# Transports

A **transport** is the queue that lets *many* workers share the load while guaranteeing each
execution is advanced by only one worker at a time. This is the hub for the per-backend
reference: the invariant they all uphold, the contract, the shared lease/parking mechanics — and
a page per backend with its **exact queue layout and every operation** (the table/keys/documents,
how `claim` leases a group, how `ack`/`nack` work, and why). For the concepts (the seam, running
workers, scaling) start with [distribution](distribution); to select one at the worker see
[`HAREL_TRANSPORT_BACKEND`](distribution).

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
| `publish(group_id, event, priority=0, routing_key=None)` | enqueue `event` in `group_id`'s FIFO; the publish that creates the group sets its priority and its routing key (the machine its execution's tree started from) |
| `claim(worker_id, visibility, min_priority=0, routing_keys=None)` | lease the oldest message of some group with **nothing in flight**, for `visibility` seconds, considering only groups with `priority >= min_priority` and, given `routing_keys`, a routing key among them; `None` if nothing deliverable |
| `ack(lease)` | the message was handled — remove it, freeing its group |
| `nack(lease, delay=0)` | return it to the queue: `delay=0` re-claimable now (retry); `delay>0` **parks** it (and blocks its group) until `delay` passes |
| `close()` | release the connection/client |

Three mechanisms recur across the backends, which each per-backend page then spells out exactly:

- **The lease / visibility timeout.** `claim` marks a group in-flight until `now + visibility`. If
  the worker `ack`s, the message is gone; if the worker **dies**, the lease expires and the
  message becomes claimable again — crash recovery with no separate sweeper. (Like any lease-based
  queue, a lease that expires mid-`ack` can let two workers briefly touch one group; the store CAS
  is the backstop.) A `Lease` carries the `group_id`, the `event`, and a handle to identify it on
  ack/nack — a `seq` (row id) or a fencing `token`.
- **Parking (`nack` with delay).** The control plane parks a suspended group's message rather than
  spinning a worker on it — see [control plane](control-plane). The `_PARKED` sentinel marks a
  message that is non-claimable until its delay elapses.
- **Round-robin fairness.** When a group is serviced, its claimability timestamp is set to `now`
  (on **claim** for the SQL / in-memory backends, on **ack** for postgres / mongo / redis) — not to
  0/earliest — so it yields to groups that haven't been serviced recently. Groups that have never
  been claimed have timestamp 0 and always go first. This prevents a handful of high-traffic
  executions from monopolizing worker capacity — the hot-partition problem — while still processing
  every group fairly.
- **Priority (`min_priority`).** A group's priority (0-4, fixed at the execution's `create`) lets a
  worker configured with `high_ratio>0` prefer high-priority groups (see
  [distribution](distribution)). Backends that keep their own group index honour it exactly —
  including finding a high-priority group behind a large backlog of low-priority ones (Redis does
  this with **one ready set per priority level**). **SQS is the exception:** FIFO has no per-group
  priority, so `SqsTransport` **rejects** `priority>0` / `min_priority>0` (fail-fast) rather than
  silently ignoring them; its cross-`MessageGroupId` delivery still gives round-robin fairness.
- **Routing by machine (`routing_keys`).** Every publish carries the routing key of the execution
  it is for — its tree's root machine — and the publish that creates a group stores it with the
  group (a group is deleted once drained, so the next publish creates it again). A worker given
  `machines` claims only groups with one of them (see
  [dedicated workers](distribution.md#dedicating-workers-to-machines)). The in-memory, SQLite,
  libSQL, rqlite, Postgres and Mongo transports route; **Redis and SQS can't**, and raise
  `NotImplementedError` for a claim given `routing_keys` rather than claim every machine's
  messages for a worker meant to have only some. A claim without it works on all of them.

Backends differ only in *how* they enforce per-group exclusivity: a database that has a cheap
serialization primitive (SQLite's write-lock, Postgres's row-lock, SQS's native `MessageGroupId`)
leans on it; the others (Redis, Mongo) build a per-group lock record by hand, indexed by when the
group next becomes claimable so `claim` reads only the few due groups instead of scanning.

A transport outside harel builds on `harel.engine.transport.base` (`Transport`,
`AsyncTransport`, `Lease`, `PARKED`) and checks itself against the contract with
`harel.testing`:

```text
from harel.testing import assert_machine_routing_contract, assert_transport_contract

def test_my_transport():
    assert_transport_contract(lambda clock: MyTransport(clock=clock))
    assert_machine_routing_contract(lambda clock: MyTransport(clock=clock))
```

`make(clock)` returns a fresh, empty transport for each check. Pass `timing=False` if it keeps
its deadlines elsewhere than the injected clock (server-side TTLs, a queue service's visibility)
and `priorities=False` if it has none. A transport that can't route by machine checks its refusal
with `assert_refuses_machine_routing` instead; either way it accepts `routing_key` on `publish`
and `routing_keys` on `claim`, which the workers pass.

A transport whose writes go into the store's transaction — a table on the caller's own
connection, next to the store's — sets `shares_store_transaction = True`. `create()` then lets a
failed publish of the new execution's `Start` reach the caller, whose transaction can't commit.
Without it, `create()` tolerates the failure: the `Start` is already in the store's outbox, and a
later flush delivers it.

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
| [RedisTransport](transports/redis) | per-priority `ready:{prio}` ZSETs + per-group lock; `publish`/`claim`/`ack` are atomic Lua (one round-trip each) |
| [PostgresTransport](transports/postgres) | per-group row, `FOR UPDATE SKIP LOCKED`; `claim`/`ack` are `plpgsql` functions (one round-trip each) |
| [RqliteTransport](transports/rqlite) | one serialized `UPDATE` (Raft orders it) |
| [MongoTransport](transports/mongo) | per-group ready-index/lock doc; `claim` is one atomic sorted `find_one_and_update` |
| [SqsTransport](transports/sqs) | SQS FIFO `MessageGroupId` (native) + visibility-timeout lease |

### What each backend supports

Every backend implements the protocol, sync and async, and passes `assert_transport_contract` —
in CI for the in-process ones, in the stack against the real servers. Where a backend can't do
something, it refuses it with an error rather than ignore it:

| | In-memory | SQLite | libSQL | rqlite | Postgres | Mongo | Redis | SQS |
|---|---|---|---|---|---|---|---|---|
| durable | — | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| priority (`priority`, `min_priority`) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | refuses |
| routing by machine (`routing_keys`) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | refuses | refuses |
| deadlines by the injected clock | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | server TTLs | queue visibility |

- **SQS** can't select what it receives — no per-group priority, no filter: it refuses both
  (`ValueError` for a priority, `NotImplementedError` for `routing_keys`). A queue per priority or
  per machine would be the SQS way; not built.
- **Redis** keeps its ready sets per priority level, so a claim can't pick one machine's groups
  out of them without scanning: it refuses `routing_keys`. A ready set per machine and priority
  would do it; not built.
- **Deadlines:** Redis keeps its group locks with server-side TTLs and SQS its visibility
  timeouts, both on real time; the others measure leases and parks with the clock they are
  given, which tests inject.

## Naming and schema ownership

Like the stores, every persistent transport takes `prefix` (default `"harel"`), which names its
tables, keys or collections, and, where it has a schema, `create_schema` — see [naming and
schema ownership](stores.md#naming-and-schema-ownership). A store and a transport in the same
database share a prefix without colliding: the transport's names carry `transport`
(`harel_transport_messages`). The SQS transport is named by its queue (`queue_name`, default
`harel.fifo`).

## Store and transport are independent

They are separate seams — mix freely (e.g. Postgres store + Redis transport) or **unify on one
backend** for a simpler stack: all-Postgres, all-rqlite, all-Mongo, or all-libSQL all need no
Redis. The async twins under `harel/engine/aio_transport/` are what the async worker uses; the
sync classes back the embedded `DistributedRunner` façade and tests. Each per-backend page has an
*Async twin* section.
