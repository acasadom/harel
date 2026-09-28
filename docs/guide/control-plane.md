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

### `on Cancel` must resolve directly to a terminal

`Cancel` is the control plane's own teardown signal, not a business event — `harel validate`
rejects an `on Cancel` transition whose target is not itself a terminal state (`Stopped` above is
a `final`, so it qualifies). `cancel()` also checks this itself at the moment it decides
cooperative-vs-forceful, so an unvalidated Definition gets the same guarantee, not just a
recommendation: a non-terminal target is treated as no handler at all (forceful terminate)
instead of being taken. Either way, the cooperative path always finishes in the very same step
the injected `Cancel` is processed: the execution never sits parked mid-cleanup, waiting on some
further event, indistinguishable from an ordinary `RUNNING` execution — which is exactly the
state a second `cancel()` call could otherwise misinterpret.

A model whose own cancellation needs more than a bounded, immediate cleanup — release a lock now,
but wait for an external refund confirmation before actually finishing — is modelling **business**
cancellation, not an execution one. Give it its own event name instead of the reserved `Cancel`:

```text
event CancelOrder {}
event Refunded {}

machine job {
  initial Working
  state Working {}
  state Releasing {}
  final Cancelled cancelled {}
  final Done success {}

  from Working to Done on Finish
  from Working to Releasing on CancelOrder
  from Releasing to Cancelled on Refunded
}
```

`CancelOrder` is handled with an ordinary `send()`, like any other domain event — `cancel()` is
never involved, `CANCELLING` never appears, and the machine can take as many steps as its own
business logic needs. If this same execution also needs to be operationally abortable (a stuck or
unwanted workflow, independent of what the business decided), that's a *separate* concern: give
`Working` its own bounded `on Cancel` straight to a terminal, and call `cancel()` for that — the
two mechanisms coexist without conflict, because they answer different questions ("did the
business cancel the order?" vs. "should this running workflow be stopped?").

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
| `cancel(reason=…)` | cooperative if the model has `on Cancel`, else forceful; always forceful on a `FAILED` execution |
| `terminate()` | forceful `CANCELLED` now — no cleanup, no hooks; on a `FAILED` execution, abandons the dead letter |
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

A dead letter you will *not* redrive is closed with `terminate()`: `FAILED → CANCELLED`, with
`error` kept as the record of why it died (and any live regions terminated along with it).
`cancel()` on a `FAILED` execution does the same — it never takes the cooperative path, even if
the state it died in models `on Cancel`, because the dead-lettered position is not a trustworthy
resting state to run the model's cleanup from.

`redrive` also refuses (`ValueError`):

- a target outside the execution's own branch — a region spawned by an orthogonal fork is its
  own `Execution`, redriven independently;
- a target while any spawned child (region/invoke) is still unfinished, so it never orphans a
  live region;
- a target nested inside an orthogonal state — a single leaf can't represent a fork's parallel
  regions (the engine always parks `active_path` *at* the fork itself and spawns one child
  Execution per region). Target a leaf before the fork instead and let the model's own
  transition re-fork it normally.

## Purge

Nothing above ever removes an execution: a finished one stays in the store — with its dedupe
records, trace and any leftover timers — for good. `purge` permanently deletes a finished
execution **tree**: the root, every region/`invoke` descendant, and everything the store keeps
for them. Pass an `archive` to keep a copy somewhere else first:

```python
import os
import tempfile

from harel import JsonlArchive, definition_from_dsl, DurableRunner, DictStore, Event

JOB = """
event Finish {}
machine job {
  initial Working
  state Working {}
  final Done success {}
  from Working to Done on Finish
}
"""

defn = definition_from_dsl(JOB, "job")
store = DictStore()
runner = DurableRunner(store, {defn.id: defn})

exe = runner.create(defn.id)
runner.process(exe.id, Event(kind="Finish"))

path = os.path.join(tempfile.mkdtemp(), "archive.jsonl")
print("purged:  ", runner.purge(exe.id, archive=JsonlArchive(path)))
print("in store:", store.load(exe.id))
print("archived:", sum(1 for _ in open(path)), "tree")
print("again:   ", runner.purge(exe.id))
```

```text
purged:   True
in store: None
archived: 1 tree
again:    False
```

- **Only finished trees.** Every member must be `DONE` or `CANCELLED`, else `ValueError`. A
  `FAILED` dead letter is waiting for a `redrive`, so abandoning it is a deliberate step:
  `terminate()` it first (its `error` is kept, and archived with it).
- **Only roots.** A region or `invoke` child belongs to its parent's join; purging the root
  removes the whole tree, so a child id is refused.
- **The archiver runs first.** It receives `{"root_id", "executions", "traces"}` — the tree root
  first, plus each member's trace — before anything is deleted, so an archiver that raises
  aborts the purge. `JsonlArchive(path)` appends one line per tree and fsyncs it; any callable
  (or, on an async runner, coroutine function) taking that dict works the same way.
- **Safe against a concurrent change.** Descendants are deleted first and the root last, each
  only if unchanged since it was checked. A member that moved on meanwhile (a `Reset` revived
  the tree) raises `StoreConflict` and stops the purge with the root still in place; running
  `purge` again resumes it. A retry may hand the archiver the same `root_id` twice.
- A stale copy of a purged execution (a worker that loaded it before the purge) can't recreate
  it: its next commit is a `StoreConflict`, and any queued event for it is dropped.

### Purging by age

Every commit stamps three wall-clock fields on the `Execution`: `created_at` (the first commit),
`updated_at` (the latest) and `finished_at` (when it became `DONE`/`CANCELLED` — cleared again if
a `Reset` revives it). Runners use their injectable `clock`; control-plane commands use the wall
clock.

`purge_finished` purges every root tree that finished more than `older_than` seconds ago — the
same `purge` per tree, so the same rules apply — and reports what it did:

```text
from harel.engine.control import purge_finished

report = purge_finished(store, older_than=30 * 86400, archive=JsonlArchive("archive.jsonl"))
report.purged           # root ids purged
report.skipped_undated  # finished roots with no finished_at (see include_undated)
report.refused          # root id -> why purge refused it (a member still live, a concurrent change)
```

`statuses` narrows the candidates to `DONE` or `CANCELLED`, `limit` caps one run, and `dry_run`
reports without deleting. Candidates are collected before anything is deleted; a refused tree is
reported and skipped, while an archiver error stops the run. Executions stored before
`finished_at` existed have none, and are skipped unless `include_undated=True`. It reads
candidates with `list_executions`, so it takes a sync store — the `harel purge` command
([CLI](cli.md)) wraps it over the store configured by the `STM_STORE_*` environment.

