# Distribution

Durability lets one process resume an execution. **Distribution** lets *many* workers share the
load while guaranteeing each execution is only ever advanced by one of them at a time.

## The transport seam

Alongside the store sits an independent **transport** — a queue with **single-active-consumer
per group**, where the group is the execution id. That property is what makes concurrency safe:
events for one execution are processed in order by one worker, even with a pool of workers
running. Backends:

| Transport | Exclusivity mechanism |
| --------- | --------------------- |
| `InMemoryTransport` | in-process (tests, single process) |
| `SqliteTransport` | `BEGIN IMMEDIATE` (global write-lock) + lease |
| `RedisTransport` | `SET lock NX PX` group-lock-as-lease + FIFO list; a `ready` ZSET scored by available-at indexes claimable groups so `claim` is O(log N + K), not a scan of every group |
| `PostgresTransport` | per-group row claimed with `SELECT … FOR UPDATE SKIP LOCKED` — concurrent workers lease *different* groups in parallel (no global lock) |
| `RqliteTransport` | queue table on Raft-replicated SQLite |
| `MongoTransport` | per-group ready-index/lock document (`available_at` + token; `find_one_and_update` lease) |
| `LibsqlTransport` | libSQL (Turso's SQLite fork): `BEGIN IMMEDIATE` + lease, like `SqliteTransport`; local file, `sqld`, or a Turso replica. **Experimental** (local-file tested; Turso/`sqld` path unvalidated against a real account) |
| `SqsTransport` | SQS FIFO `MessageGroupId` (runs on LocalStack) |

Backends without a cheap global serialization primitive (Redis, Mongo) build per-group
exclusivity by hand with a per-group lock record indexed by when it next becomes claimable, so
`claim` reads only the few due groups rather than scanning the whole queue; SQLite and Postgres
lean on the database instead (SQLite's write-lock; Postgres's row lock via `SKIP LOCKED`). For
**how each transport enforces the per-group invariant and lays its queue out** — the lease,
claim, FIFO and parking mechanics — see the [transports reference](transports).

Store and transport are **independent** — mix freely, or unify on one backend (all-Postgres,
all-rqlite, all-Mongo, all-libSQL: no Redis needed).

## Running with workers

`DistributedRunner(store, transport, definitions)` is the façade: `create` an execution, `send`
it events (published to the transport), and run one or more `worker`s that claim → load → dedupe
→ route → ack. A worker's `step()` processes exactly one available message and returns whether it
did — which lets us drive it deterministically here, without thread timing:

```python
from harel import definition_from_dsl, DictStore, Event
from harel.engine.distributed import DistributedRunner
from harel.engine.transport import InMemoryTransport

SOURCE = """
event PlaceOrder {}
event Deliver {}

machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  final Delivered success {}
  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Delivered on Deliver
}
"""

defn = definition_from_dsl(SOURCE, "order")
runner = DistributedRunner(DictStore(), InMemoryTransport(), {defn.id: defn})
worker = runner.worker()


def drain():
    while worker.step():
        pass


exe = runner.create(defn.id)
drain()

runner.send(exe.id, Event(kind="PlaceOrder"))
drain()
print("after PlaceOrder ->", runner.store.load(exe.id).active_path)

runner.send(exe.id, Event(kind="Deliver"))
drain()
final = runner.store.load(exe.id)
print("after Deliver    ->", final.active_path, "/", final.status.name, "/", final.outcome)
```

```text
after PlaceOrder -> AwaitingPayment
after Deliver    -> Delivered / DONE / success
```

In production you don't call `step()` in a loop — a worker runs `run(stop_event)`, looping
claim→process and, on its idle path, sweeping due timers (`fire_due_timers`) so timeouts fire
without a separate scheduler.

### Async-native workers

The worker is **async-native**. The bundled `python -m harel.worker` runs one `asyncio` event
loop (via `anyio.run`) that drives up to `STM_CONCURRENCY` events in flight at once
(`AsyncWorker.run`, default 256): while one execution's action awaits IO, the loop processes
others — so one worker process overlaps many events' IO on a single event loop, with no thread
per event.
The synchronous façades shown above (`DistributedRunner`, `worker.step()`) are thin wrappers
over that async core through an [anyio](https://anyio.readthedocs.io/) blocking portal, so the
deterministic `step()`-in-a-loop style still works for embedding and tests.

Scale out by running more worker **processes** (or machines) against the same store and
transport — the single-active-consumer-per-group property keeps each execution on one worker at
a time regardless of how many are running.

## Scaling & throughput

Two independent dials:

- **Concurrency within a worker** (`STM_CONCURRENCY`): how many events one event loop keeps in
  flight. Raising it lifts throughput until either the backend's per-event round-trips (load →
  commit → claim → ack) **or** the event loop's own per-event CPU becomes the limit — which of
  the two binds depends on the backend's per-op latency on your host (a slow-I/O backend is
  round-trip-bound; a fast one is loop-CPU-bound). Too high a value *degrades* throughput: more
  in-flight coroutines cost more to schedule than they save.
- **More workers / shards**: adding worker processes lifts the aggregate sublinearly until
  something shared saturates. That ceiling is **either the host** (CPU-bound worker processes
  oversubscribing the cores) **or the single backend instance** — which one binds depends on your
  hardware. On a busy single instance you scale **horizontally by sharding**: because executions
  are independent (single-consumer per group, no cross-execution coordination), you partition them
  across independent `(store, transport)` instances — each shard shares nothing with the others.
  This is the same model Temporal (hash the id to a shard, add shards) and DBOS (partition, "your
  ceiling is your Postgres") use.

Each backend's claim/commit is folded into the fewest round-trips it can be — an atomic Lua script
on Redis, one sorted `find_one_and_update` on Mongo, a `plpgsql` function (`FOR UPDATE SKIP LOCKED`
inside) on Postgres — because the per-worker limit is usually round-trips, not the server (a single
Postgres/Redis sat at single-digit % CPU even at the top rates we measured).

```{warning}
The numbers in [`bench/RESULTS.md`](https://github.com/acasadom/harel/blob/main/bench/RESULTS.md)
are **A/B comparisons on one laptop with the backends in Docker Desktop** — its VM/network proxy and
the shared cores cap the *absolute* throughput, so the multi-worker plateau there is the **laptop**,
not the backend. Read them as relative gains; a backend on native hardware / managed cloud, with
workers on a multi-core host, scales far higher on a *single* instance before sharding is needed.
Run `bench/bench_async.py`, `bench_workers.py`, `bench_shards.py` against your own backend for real
numbers.
```

### harel and durable-execution engines (illustrative)

harel is a *statechart* engine; [DBOS](https://www.dbos.dev/), Temporal and friends are
*durable-execution* / workflow engines. They overlap — both keep long-lived state alive across
crashes — but model the problem differently: declarative named states versus imperative durable code.

```{warning}
The figures below are **illustrative only — NOT a fair benchmark of DBOS**, and should not be cited
as one. They run the same *toy* FSM on the same laptop + Docker Postgres (so both carry the handicap
above), but the two tools do different work and the harness *favours harel*: its number is drain-only
while the DBOS one includes enqueuing each event. DBOS ran in its default single-instance config. The
point is the *shape* of the difference, not the ratio. Full caveats and the script
([`bench/bench_dbos.py`](https://github.com/acasadom/harel/blob/main/bench/bench_dbos.py)) are in
[`bench/RESULTS.md`](https://github.com/acasadom/harel/blob/main/bench/RESULTS.md).
```

On the toy FSM (`Idle → Working → Done`), single process, same Postgres + laptop:

| | events/s |
|---|---:|
| harel-on-Postgres (1 worker) | ~1200 |
| harel-on-Postgres (8 workers) | ~2050 |
| DBOS — durable workflow + `send`/`recv` | ~390 |
| DBOS — durable workflow per event | ~230 |

harel comes out ahead here, but **that's the paradigm, not a verdict on DBOS**: DBOS does full
durable-*workflow* bookkeeping per event — workflow-status rows, automatic recovery of arbitrary
imperative code, queues, `SERIALIZABLE` transactions — which is exactly what you want for "run these
steps, with retries, and survive a crash mid-way", and is overkill for the trivial state transition
this toy does. harel's per-event path is a lean claim → load → commit → ack. So pick by the shape of
the problem: a **statechart engine** when the domain *is* a machine of named states with hierarchy and
guards; a **durable-execution engine** when it's imperative code that must survive crashes.

## Orthogonal & fan-out, distributed

The same machinery carries [orthogonal regions](../tutorial/08-orthogonal) and
[fan-out](../tutorial/12-fanout): each region or addressed child is its own execution with its
own group, so they genuinely run on different workers, and their `Finished` events travel back
through the transport to the parent's join. The model you wrote in the tutorial distributes
without change.
