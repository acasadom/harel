# Architecture — how harel works

This is the contributor-level deep dive: the layering, what is pure vs. stateful, and a
step-by-step walk through the real lifecycle of an execution — how it's created, where and
when it's persisted, how an event is loaded, processed into **effects**, those effects run,
and how an in-memory or distributed worker actually executes your functions.

If you only want to *author and run* machines, the [tutorial](../tutorial/01-getting-started)
and the [CLI](cli) / [durability](durability) guides are enough. This page is for changing the
engine.

The *formalism* — hierarchy, orthogonal (concurrent) regions, broadcast communication — is
**David Harel's**, from his 1987 paper *Statecharts: A Visual Formalism for Complex Systems*
([PDF](https://dubroy.com/refs/Statecharts_a_visual_formalism_for_complex_systems.pdf); he later
recounted how it came to be in [*Statecharts in the Making*](https://www.weizmann.ac.il/math/harel/sites/math.harel/files/users/user50/Statecharts.History.pdf), 2007).
This engine is named in his honour and makes that formalism durable and distributed; what
follows is how it does so.

## The shape of the design

harel splits a running statechart into three layers with a hard rule between them: **the
engine is a pure function of data, and never does IO**. Everything that touches the world —
calling your action functions, reading and writing the store, publishing to a queue — lives in
a *runner* outside the engine.

```{mermaid}
flowchart TB
  subgraph pure["PURE — no IO, fully deterministic"]
    direction LR
    Defn["Definition<br/><i>immutable program</i><br/>node tree, refs"]
    Engine["Engine (core.py)<br/><i>generators that describe effects</i><br/>start / process / set_state"]
  end
  subgraph stateful["STATEFUL — does the IO"]
    direction LR
    Runner["Runner (driver flows + interpreter)<br/><i>consumes effects, runs actions</i>"]
    Store["ExecutionStore<br/><i>durable checkpoint</i>"]
    Transport["Transport + Worker<br/><i>distributed delivery</i>"]
  end
  Exe["Execution<br/><i>serializable state</i>"]

  Engine -- "reads" --> Defn
  Engine -- "mutates" --> Exe
  Runner -- "drives" --> Engine
  Runner -- "runs your action fns" --> User["user (stm, event, **inputs) fns"]
  Runner -- "commit / load" --> Store
  Transport -- "claim / ack" --> Store
  Transport --> Runner
```

Why this split: keeping the engine a pure function of data — no IO, no user code — is what buys
the three properties harel is after.

- **Determinism & testability** — a run is just `Definition + Execution → effects`, reproducible
  and unit-testable without a store, a worker, or your action code.
- **Crash-safety** — because the engine only *describes* effects, the runner can apply them and
  advance the state in **one atomic checkpoint**, so a crash never leaves a half-applied step.
- **Portability** — the state logic navigates by **node references** and touches no backend, so
  the same engine runs in-memory or over sqlite / redis / postgres / … without changing a line.

The IO — your action functions, the store, the queue — lives entirely in the runner.

### 1. Definition — the immutable program (`harel/definition/`)

A `Definition` ([model.py](https://github.com/acasadom/harel/blob/main/src/harel/definition/model.py)) is the compiled machine: a tree
of `Node`s with **real `parent`/`children` references** and an `index: dict[full_path, Node]`.
`full_path` is only a *stable address* (for serialization), **not** the runtime navigation
mechanism — the engine walks references (`chain`, `lca`, `ancestors`). It's built once from the
DSL and never mutated. Actions/guards are referenced (`ActionRef.function` is a dotted string
**or** a callable); the Definition holds no execution state.

### 2. Execution — the serializable state (`engine/execution.py`)

An `Execution` is the running instance: `status`, `active_path` (the active leaf's `full_path`),
`history` (composite → last active child), `context` (your data), `outcome`, `version` (the
optimistic-concurrency token), `children` (the orthogonal join counter), `parent_id`/`child_id`,
`deferred` (FIFO of domain events held by `defer` while the machine is in a state that has no
matching transition — persisted with the Execution and re-delivered on the next matching state).
It is **pure data** — round-trips to/from JSON, holds no references into the Definition. The
engine reads a Definition + an Execution and mutates the Execution in place.

### 3. Engine — pure, effects-based (`engine/core.py`)

`start`, `process`, `set_state` are **generators**. They mutate the Execution and `yield`
*descriptions of effects* — they never call your code or touch a store. The type is:

```python
# docs-test: skip
Step = Generator[Effect, Union[ActionResult, float, None], None]   # core.py
```

## The effect protocol — how pure meets stateful

The engine yields an effect and the runner sends a result back. Effects come in two kinds:

| Effect | Kind | Runner does | Resume |
|---|---|---|---|
| `RunAction(node, hook, action, event)` | **blocking** | call `action(stm, event, **inputs)` | `gen.send(ActionResult(value=ret))` |
| `RunSelector(node, selector, event)` | **blocking** | call the selector fn, map result → target | `gen.send(ActionResult(value=ret))` |
| `Emit(event, to)` | deferred | enqueue the event (outbox) | `gen.send(None)` |
| `SpawnChildren(specs)` | deferred | queue child-Execution creations | `gen.send(None)` |
| `ScheduleTimer(path, delay, context_key)` | deferred | arm a durable timer | `gen.send(fire_at)` — the absolute time it scheduled |
| `CancelTimer(path)` | deferred | disarm the timer | `gen.send(None)` |
| `Assigned(values)` | deferred | record what a `set` wrote, for the trace | `gen.send(None)` |

**Blocking** effects pause the generator until the runner sends back an `ActionResult` (this is
how a slow action or a remote FaaS call blocks the worker). **Deferred** effects are
fire-and-forget: the runner records them and continues immediately; they are persisted and acted
on *after* the commit (see the relay below). `ScheduleTimer` is the one whose resume carries a
value — the absolute fire time, which only the runner can compute (the engine has no clock); the
engine keeps it where a later `Timeout` must be matched against the arming it belongs to (a
machine's `ttl`). The engine raises only one thing of its own: `ExpressionError`, when a model expression (a
`set`) can't be evaluated — the runner routes it like an action error, to `on error` or the
dead-letter.

```{mermaid}
sequenceDiagram
  participant R as Driver (_drive_flow)
  participant E as Engine (process generator)
  R->>E: next(gen)
  E-->>R: RunAction(on_exit)
  R->>R: ret = on_exit(stm, event)
  R->>E: gen.send(ActionResult(ret))
  E-->>R: RunAction(on_enter)
  R->>R: ret = on_enter(stm, event)
  R->>E: gen.send(ActionResult(ret))
  E-->>R: ScheduleTimer(path, delay)
  R->>R: collect timer op (fire_at = clock() + delay)
  R->>E: gen.send(fire_at)
  E-->>R: Emit(Finished, to=parent)
  R->>R: collect emit
  R->>E: gen.send(None)
  E--xR: StopIteration (quiescent)
```

Your action functions run **only here**, in the driver ([driving.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/driving.py)) — never inside `core.py`. Its
`_drive_flow` doesn't call them itself: it yields a request to call the action, and the
interpreter of the execution model calls it (see
[semantics and execution models](#semantics-and-execution-models)). The proxy passed as `stm`
exposes `execution_ctx` (the Execution's `context`) and a stable `idempotency_key`
(`{id}:{version}:{action_index}`) so a side effect can be made effect-once across an
at-least-once redelivery (see [durability](durability)).

### What `process` does inside (the pure part)

For one event, `process` ([core.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/core.py)) resolves a transition by
**scope** (innermost composite wins; outer scopes are a fallback for *event* transitions only —
automatic lookups don't bubble), evaluating the `EventFilter` (kind with `A|B` alternation +
flat `field__op` predicates AND-ed with a composable `all`/`any`/`not` tree). Then `_take`
applies UML hook semantics over the LCA chain: **exit innermost-first, enter outermost-first,
each level runs its own `on_exit`/`on_enter`** (no override by depth), a self/local transition
fires nothing. `_drain` then runs automatic transitions to quiescence; a leaf with no outgoing
transition is a **sink** that bubbles to its composite, and a global sink sets `status=DONE` and
(if this Execution is an orthogonal region) emits `Finished` to its parent.

## The runner and the single atomic checkpoint

The driver's `_run_flow` ([driving.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/driving.py)) is the checkpoint boundary. It drives the
generator to quiescence, collecting the deferred effects, then writes **everything in one
transaction**:

```python
# docs-test: skip
def _run_flow(self, exe, gen, event_id=None, event=None):
    emits, timer_ops, spawns, actions, assigned = yield from self._drive_flow(exe, gen)
    yield from store("commit", exe, emits, processed_event_id=event_id,
                     timers=tuple(timer_ops), spawns=tuple(spawns))   # ONE atomic commit
```

(`yield from store(...)` is a flow's store call: the interpreter performs it, awaited or
plain.)

`store.commit` ([store/_base.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/store/_base.py)) persists, in a single transaction:

1. **the Execution** — a CAS write: `UPDATE … SET version=old+1 WHERE id=? AND version=old`
   (or an `INSERT` if brand-new). If the row moved on, it raises `StoreConflict` — the
   single-writer-per-Execution backstop.
2. **the outbox** — the emitted events (`Emit`), to be delivered after commit.
3. **the dedupe record** — `processed_event_id`, so an at-least-once redelivery is a no-op.
4. **the timers** — schedule/cancel, so a scheduled timer can never be lost (no dual-write).
5. **the spawns** — the orthogonal child-creation intents.
6. **the trace step** *(opt-in)* — one timeline entry when tracing is on (`HAREL_TRACE`), so the
   monitor timeline is recorded in the same atomic write; off by default (see [stores](stores)).

Either all of it commits or none does. This is the property that makes a crash safe: the state
advance, the `Finished` it must emit, the timer it armed, and the children it must spawn are one
unit.

After the commit, the **relay** (`_flush_flow`) delivers the deferred work from the durable store —
creating each pending child (idempotently: skip if it already exists) and delivering each outbox
event — looping until quiescent. Because it reads the durable store, a crash-and-restart re-runs
it idempotently.

## Walkthrough — creating and running a machine (in-memory)

`DurableRunner` ([durable.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/durable.py)) is the headless host over a
store (its logic written once and run by the model `execution=` picks — see
[semantics and execution models](#semantics-and-execution-models) below).
**Create** starts the engine and checkpoints; **process** loads, runs the
engine, checkpoints, and flushes.

```{mermaid}
sequenceDiagram
  autonumber
  actor C as Caller
  participant DR as DurableRunner
  participant D as Driver
  participant E as Engine (core)
  participant S as Store

  C->>DR: create(definition_id, context)
  DR->>D: start(exe)
  D->>E: start(defn, exe)  (generator)
  E-->>D: RunAction(on_enter) … (effects)
  D->>D: run action fns
  D->>S: commit(exe v1, emits, timers, spawns)
  D->>D: _flush_flow() — create initial regions, deliver outbox
  DR->>S: load(exe.id)
  DR-->>C: Execution (committed)

  C->>DR: process(exe.id, event)
  DR->>S: load(exe.id)            %% rehydrate from the store
  DR->>D: inject(exe, event)
  D->>E: process(defn, exe, event)  (generator)
  loop until quiescent
    E-->>D: RunAction / RunSelector (blocking)
    D->>D: run your fn, gen.send(ActionResult)
    E-->>D: Emit / ScheduleTimer / SpawnChildren (deferred)
    D->>D: collect
  end
  D->>S: commit(exe v+1, emits, processed=event.id, timers, spawns)
  D->>D: _flush_flow() — deliver outbox / create children
  DR->>S: load(exe.id)
  DR-->>C: Execution (committed)
```

The key points: **state is rehydrated from the store on every event** (the runner is stateless —
a fresh driver per call, a pure function of Definition + store), and **persisted exactly once
per event boundary**, in the atomic `commit`. Your functions run during `_drive_flow`; if an action raises, the driver first checks whether the
current configuration has an `on error` transition — if so it synthesises an `error` event and
routes to the handler state; if not (or the guard doesn't match), the production driver fails
the Execution terminally (`status=FAILED`, the dead-letter) rather than crashing the worker.

## Orthogonal fork — crash-safe spawn via the outbox

Entering an AND-state doesn't create the regions inline. The engine yields `SpawnChildren`; the
runner records the intents, and they commit **atomically with the parent's advance and its join
expectations** (`children`). The relay (`_flush_flow`) then creates all pending children as
independent work (a `Parallel` request): with coroutines their `on enter` actions (LLM calls,
HTTP requests, …) overlap on the event loop rather than running one after another; in the
caller's thread (`execution="inline"`) they run in order. Each spawn targets a distinct
`child_id` so their store commits are independent rows; no CAS conflict is possible between
siblings. The idempotency guard (skip if the child already exists) makes a crash-and-restart
safe: a re-run of `_flush_flow` skips any region that was already created before the crash.

```{mermaid}
sequenceDiagram
  autonumber
  participant E as Engine
  participant D as Driver
  participant S as Store
  E-->>D: SpawnChildren([region A, region B])
  D->>S: commit(parent, spawns=[A,B], children={A,B})   %% atomic: advance + join + spawns
  Note over D,S: parent is parked on the AND-state, waiting for the join
  D->>D: _flush_flow()
  par in parallel — all spawns (concurrent with coroutines)
    D->>S: load(child_id)  (skip if exists — idempotent)
    D->>E: start(defn, child)   %% region runs the same Definition, different root_path
    D->>S: commit(child v1)     %% independent rows: no CAS conflict between siblings
    D->>S: ack_spawn(seq)
  end
  Note over E: each region, on its global sink, Emits Finished → parent_id
  D->>S: (later, sequential) deliver Finished → process(parent) → join when all children finished
```

Regions share the parent's event stream (a domain event is broadcast to all live regions — UML
semantics). Engine events are not: lifecycle events, a region's `Finished`, and a `Timeout` —
addressed to the Execution whose state armed the timer, so a fork's own `timeout` fires on the
fork — go to the parent itself. In the headless host (`inject`) the delivery to each live region is independent work
too, so with coroutines a broadcast that triggers async actions in multiple regions (LLM calls,
HTTP requests…) overlaps them on the event loop rather than serialising them. Data-parallel work (N independent workers) is **not** an orthogonal state but a fan-out
`invoke`, which reuses the same child-Execution machinery.

## Durable timers

A state with `timeout: T` yields `ScheduleTimer` on enter and `CancelTimer` on exit; those ride
the **same commit** as the transition, so a scheduled timer can't be lost. When due, a sweep
delivers a `Timeout` event (with a stable id, so a double sweep dedupes) carrying the timed
state's `path`. `process` only fires the model's `on Timeout` transition **if that path is still
active** (a staleness guard), bubbling from the timed-out node up through its ancestors. Timers
are statechart-native: the engine schedules, the **model decides** what the timeout does — which
is why retry/backoff is modelled as a composite, not an engine feature.

## In-memory vs. distributed

The same engine and the same `commit` run in two hosts:

- **In-memory** (`Driver` / `DurableRunner`): the relay delivers emitted events *inline* (it
  calls `process` on the target itself). One process. Used for embedding and tests.
- **Distributed** (the transport driver, `TransportDriverLogic`, + `Worker` + `Transport`, [distributed.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/distributed.py)): the relay **publishes** emitted events to a
  `Transport` (a queue) instead of delivering inline; workers claim and process them. Same code,
  many processes.

The table below shows the key behavioural differences:

| | Headless (`DurableRunner`) | Distributed (`DistributedRunner`) |
|---|---|---|
| **Outbox delivery** | `_flush_flow` calls `_run_flow` on the target inline | `_flush_flow` publishes to transport; worker claims |
| **Broadcast to regions** | `inject` calls `_run_flow` on each region (in parallel) | `route` publishes to each region's group; workers claim independently |
| **Timers** | `_deliver_timeout_flow` calls `_run_flow` inline | publishes `Timeout` to transport; worker claims it like any event |
| **`process()` returns** | after all cascades settle (fully quiescent) | after publishing the event; worker runs asynchronously |
| **Who calls `_run_flow`** | the driver, inline | always the worker, after claim |
| **Processes** | one | N workers in parallel |

## Walkthrough — distributed flow

`DistributedRunner` ([distributed.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/distributed.py)) splits the lifecycle across
two roles: the **sender** (your application code calling `create`/`send`) and the **worker**
(a long-running loop that claims and processes). Unlike the headless runner, `send` returns as
soon as the event is on the queue — the machine advances asynchronously.

```{mermaid}
sequenceDiagram
  autonumber
  actor C as Caller
  participant DR as DistributedRunner
  participant T as Transport
  participant S as Store
  participant W as Worker
  participant TD as Transport driver
  participant E as Engine (core)

  C->>DR: create(definition_id, context)
  DR->>S: commit(exe PENDING, emits=[Start])   %% the Start is in the outbox
  DR->>T: publish(exe.id, Start, priority)
  DR->>S: ack_outbox(seq)   %% delivered: off the outbox
  DR-->>C: Execution committed, a worker runs its Start

  C->>DR: send(exe.id, event)
  DR->>S: load(exe.id)   %% read priority
  DR->>T: publish(exe.id, event, priority)
  DR-->>C: returns immediately

  Note over W,T: worker loops independently

  W->>T: claim(worker_id, visibility)
  T-->>W: Lease(group_id=exe.id, event)
  W->>S: load(exe.id)
  W->>TD: route(exe, event)
  alt domain event with live regions
    TD->>T: publish event to each region group
    TD->>S: commit(exe, processed=event.id)
  else control event or no regions
    TD->>E: process(defn, exe, event)
    E-->>TD: effects
    TD->>S: commit(exe v+1, emits, processed, timers, spawns)
    TD->>TD: _flush_flow() publish outbox, create children
  end
  W->>T: ack(lease)
```

The critical difference from the headless path: **`send` does not wait for the machine to
advance**. The event is durable on the transport the moment `send` returns; if every worker
dies at that instant, the event is redelivered when one restarts. The worker's `ack` only fires
*after* `commit` succeeds — so the at-least-once guarantee is upheld end-to-end.

The `StoreConflict` path (not shown above) is the backstop: if two workers somehow claim the
same group concurrently (lease expired, clock skew), the CAS in `commit` rejects the slower
one, which `nack`s and lets the transport redeliver to a fresh worker with the correct version.

Any other failure while handling a message — the store or the transport down, a bug — is logged by
the worker's `run` loop with its traceback, and the message is `nack`ed to come back after
`suspend_recheck` seconds (5 by default): soon enough for a passing outage, without spinning on one
that persists. If that `nack` fails too, the message returns when its lease expires. (An action that
raises is not this case: the driver routes it to `on error` or dead-letters the execution.)

### Semantics and execution models

Two separate questions describe how a runner behaves:

- **Its semantics — does the caller wait for the result?** `DurableRunner.process` returns the
  execution once the event has been processed: *synchronous*. `DistributedRunner.send` queues
  the event and returns; a worker processes it later: *asynchronous*. That is in the runner's
  name.
- **Its execution model — how does the call run?** In the caller's own thread; on a background
  event loop the call blocks on; or as a coroutine the caller awaits.

The engine in `core.py` is a generator that does no IO — it only yields effects — and the code
that drives it follows the same approach one level up. The driver and the runners are written
once as **flows** (`harel/engine/flow.py`): generators that yield IO requests — a call on the
store or the transport, a user action, independent work in parallel — and receive their results.
The logic lives in `harel/engine/driving.py` (run the engine for an event, call the actions and
route their errors, commit, relay the outbox), `harel/engine/hosting.py` (the durable host,
the distributed sender, the worker) and `harel/engine/control.py` (the control plane: cancel,
suspend, purge, …). An **interpreter** serves those requests, and that is the
execution model:

| | interpreter | used by |
|---|---|---|
| coroutines | `flow.run_async` — awaits each request; a sync action runs in a thread pool; parallel work overlaps | `AsyncDurableRunner`, `AsyncDistributedRunner`, `AsyncWorker` (`python -m harel.worker`) |
| background loop | the coroutine interpreter on one shared background loop the call blocks on (an [anyio](https://anyio.readthedocs.io/) portal) | the sync runners and `Driver`, `execution="background"` (the default) |
| caller's thread | `flow.run_inline` — each request is a plain call, parallel work in order, no event loop | the sync runners and `Driver`, `execution="inline"` |

So a synchronous snippet in this guide, a web view running `execution="inline"` inside its
transaction, and the async production worker run the same logic and the same commit. See
[execution models](execution) for a sequence diagram of each semantics and each model, every
combination of the two, and when to pick each.

A `Transport` is a queue with **single-active-consumer per group**, where `group_id =
execution_id` — so at most one message per Execution is in flight, which is what upholds the
single-writer invariant (the store CAS is the backstop if a lease expires). A `Worker` loops
**claim → load → dedupe → route → ack**:

```{mermaid}
sequenceDiagram
  autonumber
  participant W as Worker.step()
  participant T as Transport
  participant S as Store
  participant TD as Transport driver
  participant E as Engine

  W->>T: claim(worker_id, visibility)
  T-->>W: Lease(group_id=exe.id, event)
  W->>S: load(group_id)
  alt unknown or already processed
    W->>T: ack (no-op)
  else SUSPENDED
    W->>T: nack(delay)   %% park the group, don't spin
  else CANCELLED / (CANCELLING & not Cancel)
    W->>T: ack   %% drain the backlog as no-ops
  else live
    W->>TD: route(exe, event)
    alt domain event & live regions
      TD->>T: publish(event) to each region's group   %% fan out
      TD->>S: commit(exe, processed=event.id)
    else control event or no regions
      TD->>E: process(defn, exe, event)
      E-->>TD: effects (run actions, collect)
      TD->>S: commit(exe v+1, emits, processed, timers, spawns)
    end
    TD->>TD: _flush_flow() → publish outbox to transport, create children
    alt StoreConflict (another writer won)
      W->>T: nack   %% redeliver, retry against fresh state
    else
      W->>T: ack
    end
  end
```

The worker honours the lifecycle status after load: `CANCELLED` → ack-drain; `SUSPENDED` →
`nack(delay)` (park so a paused group doesn't spin a worker); `CANCELLING` + non-`Cancel` →
ack-drain until the cooperative `Cancel` arrives. This is the **control plane**: lifecycle
commands ([control.py](https://github.com/acasadom/harel/blob/main/src/harel/engine/control.py)) CAS the Execution record directly, so
they land at the next event boundary instead of behind the FIFO backlog — portably, with no
transport priority/purge.

Timers in the distributed host: `Worker.fire_due_timers` runs on the idle path of the loop and
*publishes* the `Timeout` to the transport (vs. the synchronous host delivering it inline). All
due timers are fired as independent work — with coroutines, a batch of expired timers is swept
in one round instead of sequentially.

## Where state is persisted (checkpoint points)

| When | Call | What is written |
|---|---|---|
| create | `start()` → `commit` | Execution v1, initial region spawns, entry-hook timers |
| each event | `_run_flow` → `commit` | Execution v+1 (CAS), outbox emits, dedupe id, timer ops, spawns, trace step (if `HAREL_TRACE`) |
| relay | `_create_spawn_flow` → `commit` | each child Execution v1 (idempotent) |
| control plane | `control.*` → `commit` | status change (+ a cooperative `Cancel` emit), via CAS |
| timer fire | sweep → `process` → `commit` | the `Timeout` processed like any event; timer row deleted |

Two invariants hold everything together: **one atomic commit per event** (no dual-write window),
and **at-least-once delivery + per-event dedupe** (`processed_events` + `Event.id`), so a redelivery
takes effect exactly once. Side effects in *your* actions are made effect-once with the
`idempotency_key` against an external backend — harel records nothing extra there, because a
harel-side record would roll back with a failed commit.

## Recap

- The **engine is pure**: it reads a `Definition`, mutates an `Execution`, and yields **effects**.
  It never calls your code or does IO.
- The **runner** consumes those effects — running your action functions (blocking effects) and
  collecting the deferred ones — then writes one **atomic checkpoint** to the store.
- **Persistence is one transaction per event**: the state, the outbox, the dedupe, the timers and
  the spawns commit together; a relay delivers the deferred work afterwards from the durable store.
- **In-memory and distributed** are the same engine + the same commit, differing only in whether
  the relay delivers inline or publishes to a transport that workers claim — single-active-consumer
  per execution, with the store CAS as the backstop.
