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
| `SqsTransport` | SQS FIFO `MessageGroupId` (runs on LocalStack) |

Backends without a cheap global serialization primitive (Redis, Mongo) build per-group
exclusivity by hand with a per-group lock record indexed by when it next becomes claimable, so
`claim` reads only the few due groups rather than scanning the whole queue; SQLite and Postgres
lean on the database instead (SQLite's write-lock; Postgres's row lock via `SKIP LOCKED`).

Store and transport are **independent** — mix freely, or unify on one backend (all-Postgres,
all-rqlite, all-Mongo: no Redis needed).

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
others — so a single worker process saturates an IO-bound backend without a thread per event.
The synchronous façades shown above (`DistributedRunner`, `worker.step()`) are thin wrappers
over that async core through an [anyio](https://anyio.readthedocs.io/) blocking portal, so the
deterministic `step()`-in-a-loop style still works for embedding and tests.

Scale out by running more worker **processes** (or machines) against the same store and
transport — the single-active-consumer-per-group property keeps each execution on one worker at
a time regardless of how many are running.

## Scaling & throughput

Two independent dials:

- **Concurrency within a worker** (`STM_CONCURRENCY`): how many events one event loop keeps in
  flight. Raising it lifts throughput until the backend's per-event round-trips (load → commit →
  claim → ack) become the limit.
- **More workers / shards**: a single backend instance is the ceiling — piling workers on one
  backend plateaus (the backend, not the worker, is the bottleneck). Throughput scales
  **horizontally by sharding**: because executions are independent (single-consumer per group,
  no cross-execution coordination), you partition them across independent `(store, transport)`
  instances — each shard shares nothing with the others. This is the same model Temporal (hash
  the id to a shard, add shards) and DBOS (partition, "your ceiling is your Postgres") use.

The Postgres transport's `SKIP LOCKED` claim lets workers on one Postgres lease different groups
concurrently rather than serializing on a global lock. Measured numbers, the worker-vs-backend
and sharding experiments, and the methodology live in
[`bench/RESULTS.md`](https://github.com/acasadom/harel/blob/main/bench/RESULTS.md) (run them
yourself with `bench/bench_async.py`, `bench_workers.py`, `bench_shards.py`).

## Orthogonal & fan-out, distributed

The same machinery carries [orthogonal regions](../tutorial/08-orthogonal) and
[fan-out](../tutorial/12-fanout): each region or addressed child is its own execution with its
own group, so they genuinely run on different workers, and their `Finished` events travel back
through the transport to the parent's join. The model you wrote in the tutorial distributes
without change.
