# Observing executions

Every commit harel makes is described by a `Step`: what caused it, and how the execution moved.
Wrap a store in `ObservedStore`, and a function of yours hears about every step once it is
committed — to count, alert, audit or notify, without touching the machines or the runners.

```python
from harel import DictStore, DurableRunner, Event, ObservedStore, definition_from_dsl
from harel.engine.execution import Status

defn = definition_from_dsl(
    """
event Approve {}
machine approval {
  initial Review
  state Review {}
  final Approved success
  from Review to Approved on Approve
}
""",
    "approval",
)

finished = []


def on_step(step, exe):
    if step.to_status is Status.DONE and step.from_status is not Status.DONE:
        finished.append((exe.id, exe.outcome))


runner = DurableRunner(ObservedStore(DictStore(), on_step), {defn.id: defn})
exe = runner.create(defn.id)
runner.process(exe.id, Event(kind="Approve"))
assert finished == [(exe.id, "success")]
```

## What a step says

`Step` (from `harel`, or `harel.engine.store.base`) is a frozen record:

| field | what |
|---|---|
| `cause` | `"create"` — a new execution, its `Start` queued (`DistributedRunner.create`); `"start"` — an execution started without an event (`DurableRunner.create`, a child the relay spawns) or a deferred start queued (`DistributedRunner.start`); `"event"` — an event processed; `"control"` — a control-plane command |
| `event_kind`, `event_id` | the event, for `"event"` (and the `Start` a `"create"` queues) |
| `command` | for `"control"`: `"suspend"`, `"resume"`, `"cancel"`, `"terminate"` or `"redrive"` |
| `from_status`, `to_status` | the execution's status before and after the commit |
| `from_path`, `to_path` | its active state before and after |
| `actions` | the actions the step ran, by name — none for a step that failed the execution (its effects are dropped; `exe.error` says what failed) |

The status before is what tells a step *into* a status apart from one that stays there: a step
into `FAILED` carries no `finished_at` — a dead letter can still be redriven — so
`from_status`/`to_status` is how to see it happen. A region's steps come with the region's own
execution (its `parent_id` names the parent).

`on_step(step, exe)` gets a copy of the execution as committed. It runs in the writer's thread
or loop, right after the commit — keep it quick, or hand the work off. If it raises, the error
is logged and the step stands: it is already committed. `AsyncObservedStore` wraps an async
store, and takes a plain or a coroutine function.

## Transactional stores

`ObservedStore` calls `on_step` once the wrapped store's `commit` has returned — durable, for a
store that commits on its own. A store that writes inside a transaction it doesn't end (one on
the caller's connection) has committed nothing yet when `commit` returns, and the transaction
may still roll back. Such a store reads the `step` argument of its own `commit` — every writer
passes one — and defers what it does until that transaction commits.
