# Execution models

A sync runner — `DurableRunner`, `DistributedRunner`, its `Worker` — takes an `execution=` that
picks *how* its calls run; *what* they do is the same either way (see
[semantics and execution models](architecture.md#semantics-and-execution-models)):

| `execution=` | where a call runs | the store and transport | the actions |
|---|---|---|---|
| `"background"` (default) | on one shared background event loop; the call blocks until it's done | sync or async | a sync action in a thread pool, a coroutine awaited |
| `"inline"` | in the caller's own thread, with no event loop | sync only | called right there; a coroutine action is refused |

For async code, use the async runners (`AsyncDurableRunner`, `AsyncDistributedRunner`,
`AsyncWorker`) directly.

## Inline: inside the caller's transaction

Frameworks that keep a database connection per thread — Django, SQLAlchemy sessions — need the
store to run on the caller's thread: on another one it would use another connection, outside the
caller's transaction (and Django refuses ORM calls from a thread running an event loop).
`execution="inline"` does every store call, every transport call and every action right there:

```text
runner = DurableRunner(store, definitions, execution="inline", on_action_error="raise")

with transaction.atomic():
    order = Order.objects.create(...)
    runner.process(order.execution_id, Event(kind="Paid"))   # same connection, same transaction
```

The distributed side works the same way: with the store and the transport in the caller's
database, `create()` writes the execution and queues its `Start` in the caller's transaction — so
they commit, or roll back, with the caller's own writes.

```text
runner = DistributedRunner(store, transport, definitions, execution="inline")
with transaction.atomic():
    exe = runner.create("order", context={"order_id": order.id})   # the Start is queued here
```

`runner.worker()` uses the runner's model: an inline worker handles one message per `step()` in
its own thread, and `run(stop)` loops over it.

## When an action raises

`DurableRunner(on_action_error=...)` decides what happens to an action error nothing in the model
handles (an `on error` transition takes precedence either way):

- `"fail"` (default) — the execution fails terminally (`FAILED`, with its `error`: the dead
  letter), and that step is committed; `redrive()` brings it back once the bug is fixed.
- `"raise"` — the exception reaches the caller and that step is not committed. Inside the
  caller's transaction, the caller's own writes and the machine's advance roll back together.
  Steps the same call committed before — an earlier region of a broadcast event — stay, unless
  the transaction rolls them back.
