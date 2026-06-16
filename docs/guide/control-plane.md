# Control plane

Events drive a machine *forward*. The **control plane** is the out-of-band commands that manage
an execution's lifecycle — cancel, terminate, suspend, resume. They bypass the FIFO event queue
(they compare-and-set the execution record directly), so they take effect at the next event
boundary instead of waiting behind the backlog. Both `DurableRunner` and `DistributedRunner`
expose them.

## Cancel — cooperative or forceful

`cancel` adapts to your model. If the active state has its own `on Cancel` transition, the
machine **owns its cleanup**: the command moves it to `CANCELLING` and injects a `Cancel` event,
and the model's cleanup transition runs. If there is no `Cancel` handler, `cancel` is a forceful
`terminate`. A `reason` payload rides on the `Cancel` event for the model to read:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event Finish {}

machine job {
  initial Working
  state Working {}
  final Done success {}
  final Stopped cancelled {}

  from Working to Done on Finish
  from Working to Stopped on Cancel where reason == "user_request"
}
"""

defn = definition_from_dsl(SOURCE, "job")
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
exe = runner.cancel(exe.id, reason={"reason": "user_request"})
print("cooperative cancel ->", exe.active_path, "/", exe.status.name, "/", exe.outcome)
```

```text
cooperative cancel -> Stopped / DONE / cancelled
```

The machine ran its modelled cleanup (`Working → Stopped`) and ended with the verdict it chose
(`cancelled`) — the engine didn't just kill it. Cleanup logic lives in the model, like
everything else.

## Terminate, suspend, resume

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

defn = definition_from_dsl(SOURCE, "job")  # SOURCE from the block above
runner = DurableRunner(DictStore(), {defn.id: defn})

# suspend / resume — reversible; state, history and the queued backlog are preserved
exe = runner.create(defn.id)
print("suspended ->", runner.suspend(exe.id).status.name)
print("resumed   ->", runner.resume(exe.id).status.name)

# terminate — forceful: CANCELLED now, no hooks, backlog drains as no-ops
exe = runner.create(defn.id)
print("terminated->", runner.terminate(exe.id).status.name)
```

```text
suspended -> SUSPENDED
resumed   -> RUNNING
terminated-> CANCELLED
```

| Command | Effect |
| ------- | ------ |
| `cancel(reason=…)` | cooperative if the model has `on Cancel`, else forceful |
| `terminate()` | forceful `CANCELLED` now — no cleanup, no hooks |
| `suspend()` | `RUNNING → SUSPENDED`, reversible (backlog parked) |
| `resume()` | `SUSPENDED → RUNNING`, continues where it stopped |

In a distributed deployment the same commands work portably: a paused group parks its messages
(`nack` with delay) rather than spinning a worker, and a cooperative `cancel` makes the worker
drain the backlog as no-ops until the injected `Cancel` arrives — the queue-jump semantics
without needing transport-level priority or purge (which SQS FIFO, for one, can't do). Commands
also propagate from an orthogonal parent to its regions.
