# Control plane

Events drive a machine *forward*. The **control plane** is the out-of-band commands that manage
an execution's lifecycle — cancel, terminate, suspend, resume, redrive. They bypass the FIFO
event queue (they compare-and-set the execution record directly), so they take effect at the
next event boundary instead of waiting behind the backlog. Both `DurableRunner` and
`DistributedRunner` expose them.

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
| `redrive(target_path)` | `FAILED → RUNNING`, repositioned at `target_path` |

In a distributed deployment the same commands work portably: a paused group parks its messages
(`nack` with delay) rather than spinning a worker, and a cooperative `cancel` makes the worker
drain the backlog as no-ops until the injected `Cancel` arrives — the queue-jump semantics
without needing transport-level priority or purge (which SQS FIFO, for one, can't do). Cancel,
terminate, suspend and resume also propagate from an orthogonal parent to its regions — all
children at each level of the tree are updated concurrently via `asyncio.gather`. `redrive` does
not: see below.

## Redrive — repairing a dead letter

An unhandled action error (a bug, not a modelled failure) fails the execution terminally:
`status=FAILED`, dead-lettered, with `error` set. This is different from a *modelled* failure
routed by [`on error`](../tutorial/16-on-error) to a `final … failed {}` state — that's
`status=DONE` with a verdict, not a dead letter. `redrive` is how you bring a dead letter back
once you've fixed the bug that killed it: it repositions `active_path` at a leaf state **you
choose** and flips the status back to `RUNNING`, clearing `error`.

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

BUGGY = """
event Go {}
machine job {
  initial Waiting
  state Waiting {}
  state Working { on enter process }
  final Done success {}
  from Waiting to Working on Go
  from Working to Done
}
"""

calls = {"n": 0}


def process(stm, event, **inputs):
    calls["n"] += 1
    if calls["n"] == 1:
        raise RuntimeError("simulated bug")  # the first call always fails


defn = definition_from_dsl(BUGGY, "job", actions={"process": process})
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
exe = runner.process(exe.id, Event(kind="Go"))
print("dead-lettered:", exe.status.name, exe.active_path, "-", exe.error)

# ... the bug is fixed and deployed; `process` no longer raises on this path ...
exe = runner.redrive(exe.id, "Waiting")  # back to the last known-good leaf
print("redriven:     ", exe.status.name, exe.active_path)

exe = runner.process(exe.id, Event(kind="Go"))  # retry the same transition
print("done:         ", exe.status.name, exe.active_path)
```

```text
dead-lettered: FAILED Working - RuntimeError: simulated bug
redriven:      RUNNING Waiting
done:          DONE Working
```

Two things to note:

- **You choose the target, deliberately.** `redrive` never infers it from the failed
  `active_path` — an `on_exit` failure in particular can leave that pointing at a composite
  state mid-cascade (see the [DSL reference](dsl-reference)), which is never a valid resting
  leaf. Picking the target is a judgment call about what's safe to resume from, given whatever
  the action's side effects actually did before it raised.
- **Context is untouched, history is not.** `redrive` assumes the *code* had a bug, now fixed —
  not that the data needs correcting too. If the execution's context itself is what caused the
  failure, fix it directly in the store before redriving (or via your own repair tooling);
  `redrive` won't do it for you. History, on the other hand, is cleared: a state whose own
  `on_exit` raised never got to record its own history entry, even though a descendant that
  exited cleanly earlier in the same cascade did — that partial, inconsistent history is
  discarded rather than risking a later history re-entry landing on the pre-crash child instead
  of wherever you just redrove to.

`redrive` also refuses (`ValueError`):

- a target outside the execution's own branch — a region spawned by an orthogonal fork is its
  own `Execution`, redriven independently;
- a target while any spawned child (region/invoke) is still unfinished, so it never orphans a
  live region;
- a target nested inside an orthogonal state — a single leaf can't represent a fork's parallel
  regions (the engine always parks `active_path` *at* the fork itself and spawns one child
  Execution per region). Target a leaf before the fork instead and let the model's own
  transition re-fork it normally.
