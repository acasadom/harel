# Execution models

Two separate questions describe how a runner behaves, and they have independent answers:

- **Its semantics — does the caller wait for the result?** That's in the runner's name.
- **Its execution model — how does the call run?** In the caller's own thread, on a background
  event loop, or as a coroutine. That's `execution=`, or the `Async…` runner.

Running with coroutines does not mean the work is handed to someone else: `await
AsyncDurableRunner.process(...)` comes back with the event fully processed and committed. And
handing the work to a worker doesn't need coroutines: a `DistributedRunner` with
`execution="inline"` has no event loop at all.

## Semantics: wait for the result, or hand it over

**Synchronous** — `DurableRunner` (and `AsyncDurableRunner`): `process()` runs the machine for
the event (the actions, the commit, everything the event cascades into) and returns the
execution as it now stands.

```{mermaid}
sequenceDiagram
  actor C as Caller
  participant R as DurableRunner
  participant S as Store
  C->>R: process(exe_id, event)
  R->>S: load(exe_id)
  R->>R: run the engine and the actions
  R->>S: commit(exe v+1, …)
  R->>R: relay: deliver what the step emitted, create its children
  R-->>C: the Execution, advanced
```

**Asynchronous** — `DistributedRunner` (and `AsyncDistributedRunner`): `send()` puts the event on
the transport and returns. A worker — another thread, process or machine — claims it later and
runs the machine; the caller learns the result by reading the store (or from what the machine
itself does when it finishes).

```{mermaid}
sequenceDiagram
  actor C as Caller
  participant R as DistributedRunner
  participant T as Transport
  participant W as Worker (elsewhere)
  participant S as Store
  C->>R: send(exe_id, event)
  R->>T: publish(exe_id, event)
  R-->>C: returns — nothing has run yet
  Note over W: later, independently
  W->>T: claim
  T-->>W: lease(event)
  W->>S: load, run the engine and the actions, commit
  W->>T: ack
```

## Execution model: how a call runs

The logic is written once, as flows that ask for their IO (a store call, an action, independent
work) instead of doing it; the execution model is the interpreter that performs those requests
(see [semantics and execution models](architecture.md#semantics-and-execution-models)). Here is
the same `DurableRunner.process()` under each.

**`execution="inline"`** — everything in the caller's thread, with no event loop: each store call
and each action is a plain call. Independent work (a broadcast to several regions) runs in order.

```{mermaid}
sequenceDiagram
  participant CT as Caller's thread
  participant S as Store (sync)
  participant A as Action
  CT->>S: load(exe_id)
  CT->>A: on_enter(stm, event)
  A-->>CT: returns
  CT->>S: commit(exe v+1, …)
  Note over CT: process() returns — one thread, the caller's connection
```

**`execution="background"`** (the default for the sync runners) — the caller's thread blocks
while one shared background event loop (a thread of its own, one per process) runs the call. A
sync store is called on that loop's thread; a sync action goes to a thread pool, so a slow one
doesn't freeze the loop; a coroutine action is awaited on the loop.

```{mermaid}
sequenceDiagram
  participant CT as Caller's thread
  participant L as Background loop thread
  participant P as Thread pool
  participant S as Store
  CT->>L: process(exe_id, event)
  Note over CT: blocked until the call is done
  L->>S: load(exe_id)
  L->>P: on_enter(stm, event) — a sync action
  P-->>L: returns
  L->>S: commit(exe v+1, …)
  L-->>CT: the Execution
```

**Coroutines** — the async runners (`AsyncDurableRunner`, `AsyncDistributedRunner`,
`AsyncWorker`), awaited from your own event loop: no extra thread. Every store call is awaited,
and while one execution waits on its IO the loop runs the others — many calls in flight on one
thread. Independent work inside one call (creating a fork's regions, a broadcast) overlaps too.

```{mermaid}
sequenceDiagram
  participant L as Your event loop (one thread)
  participant S as Store (async)
  Note over L: task 1: await process(e1, …)
  L->>S: load(e1)
  Note over L: task 2: await process(e2, …) — runs while task 1 waits
  L->>S: load(e2)
  S-->>L: e1
  L->>S: commit(e1 v+1, …)
  S-->>L: e2
  L->>S: commit(e2 v+1, …)
  Note over L: both done — no thread per call
```

## Every combination

The two axes combine freely:

| | synchronous semantics | asynchronous semantics |
|---|---|---|
| **caller's thread** | `DurableRunner(..., execution="inline")` | `DistributedRunner(..., execution="inline")` and its worker |
| **background loop** | `DurableRunner(...)` | `DistributedRunner(...)` and its worker |
| **coroutines** | `AsyncDurableRunner` | `AsyncDistributedRunner` + `AsyncWorker` (`python -m harel.worker`) |

For the sync runners, `execution=` picks the model:

| `execution=` | where a call runs | the store and transport | the actions |
|---|---|---|---|
| `"background"` (default) | on one shared background event loop; the call blocks until it's done | sync or async | a sync action in a thread pool, a coroutine awaited |
| `"inline"` | in the caller's own thread, with no event loop | sync only | called right there; a coroutine action is refused |

It is taken by `DurableRunner`, `DistributedRunner`, its `Worker`, and the bare in-memory
`Driver`. For async code, use the async runners directly.

## Inline: inside the caller's transaction

A database connection is often bound to the thread that opened it, and a transaction to that
connection. A store that writes through the caller's connection must then run on the caller's
thread: on another one it would use another connection, outside the caller's transaction.
`execution="inline"` does every store call, every transport call and every action right there:

```text
runner = DurableRunner(store, definitions, execution="inline", on_action_error="raise")

with db.transaction():                       # the caller's own transaction
    order_id = db.insert_order(...)
    runner.process(execution_id, Event(kind="Paid"))   # same connection, same transaction
```

The distributed side works the same way: with the store and the transport in the caller's
database, `create()` writes the execution and queues its `Start` in the caller's transaction — so
they commit, or roll back, with the caller's own writes.

```text
runner = DistributedRunner(store, transport, definitions, execution="inline")
with db.transaction():
    exe = runner.create("order", context={"order_id": order_id})   # the Start is queued here
```

`runner.worker()` uses the runner's model: an inline worker handles one message per `step()` in
its own thread, and `run(stop)` loops over it.

The bare `Driver` takes it too: `Driver(defn, execution="inline")` drives its executions in the
caller's thread over a `DictStore` (or the sync store given), and propagates an action's error
as it does in the background.

## When an action raises

`DurableRunner(on_action_error=...)` decides what happens to an action error nothing in the model
handles (an `on error` transition takes precedence either way):

- `"fail"` (default) — the execution fails terminally (`FAILED`, with its `error`: the dead
  letter), and that step is committed; `redrive()` brings it back once the bug is fixed.
- `"raise"` — the exception reaches the caller and that step is not committed. Inside the
  caller's transaction, the caller's own writes and the machine's advance roll back together.
  Steps the same call committed before — an earlier region of a broadcast event — stay, unless
  the transaction rolls them back.
