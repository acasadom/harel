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
| `SqliteTransport` | `BEGIN IMMEDIATE` + lease |
| `RedisTransport` | `SET lock NX PX` group-lock-as-lease + FIFO list |
| `PostgresTransport` | `pg_advisory_xact_lock` serializing claims |
| `RqliteTransport` | queue table on Raft-replicated SQLite |
| `MongoTransport` | per-group lock document (atomic-upsert lock-as-lease) |
| `SqsTransport` | SQS FIFO `MessageGroupId` (runs on LocalStack) |

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
without a separate scheduler. Today's workers are threads (`test/integration/` covers the
genuinely-concurrent, multi-process variant); the multi-process deployment is the same code with
a networked store and transport.

## Orthogonal & fan-out, distributed

The same machinery carries [orthogonal regions](../tutorial/08-orthogonal) and
[fan-out](../tutorial/12-fanout): each region or addressed child is its own execution with its
own group, so they genuinely run on different workers, and their `Finished` events travel back
through the transport to the parent's join. The model you wrote in the tutorial distributes
without change.
