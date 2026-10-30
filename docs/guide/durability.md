# Durability

Everything in the tutorial used an in-memory `DictStore`, so executions vanished with the
process. The point of harel is that they don't have to: an `Execution` is **serializable
data**, and the runner checkpoints it through a **store** at every event boundary. Swap the
store and the same machine survives crashes and restarts.

## The store seam

`DurableRunner(store, definitions)` takes any `ExecutionStore`. The bundled backends:

| Store | For |
| ----- | --- |
| `DictStore` | tests, examples (in-memory, not durable) |
| `SqliteStore` | a single machine — durable, zero infrastructure |
| `RedisStore` | distributed (atomic Lua CAS on the state-only fast path; WATCH/MULTI for complex commits) |
| `PostgresStore` | distributed (UPDATE-WHERE-version CAS) |
| `RqliteStore` | distributed SQLite over Raft (HTTP) |
| `MongoStore` | distributed document store (single-document atomic `update_one` CAS) |
| `LibsqlStore` | libSQL (Turso's SQLite fork): a local file, a `sqld` server, or an embedded Turso replica — same CAS as SQLite. **Experimental**: the local-file path is tested in-process; the Turso/`sqld` path is wired but not yet validated against a real account |
| `DynamoDBStore` | AWS serverless (conditional-write CAS, atomic `TransactWriteItems`; pairs with `SqsTransport`) |

The networked ones are optional extras (`harel[redis]`, `[postgres]`, `[rqlite]`, `[mongo]`, `[libsql]`, `[dynamodb]`) and take an
injected client. Selecting one is the *only* change — the machine and the driving loop are
identical. For **how each backend lays its data out** — tables/keys/collections, the CAS, the
outbox/dedupe/timer/trace storage — see the [stores reference](stores).

## Surviving a restart

Point a `SqliteStore` at a file, and an execution created by one runner is picked up by another
**using only its id** — the state lives in the store, the id is the handle that crosses the
process boundary. A stand-in for "the process died and came back":

```python
import tempfile
from pathlib import Path

from harel import definition_from_dsl, DurableRunner, SqliteStore, Event

SOURCE = """
event Finish {}

machine order {
  initial Cart
  state Cart {}
  final Done success {}
  from Cart to Done on Finish
}
"""

defn = definition_from_dsl(SOURCE, "order")
db = str(Path(tempfile.mkdtemp()) / "stm.db")

# process #1: a runner creates the execution (persisting it) and then "crashes".
# create() returns the Execution; its `.id` is the ONLY handle that outlives the
# process — you keep it (in a URL, a queue message, another table) to refer back.
execution_id = DurableRunner(SqliteStore(db), {defn.id: defn}).create(defn.id).id

# process #2: a brand-new runner — shares nothing with the first but the file —
# reloads that execution by id from the store and drives it to completion.
exe = DurableRunner(SqliteStore(db), {defn.id: defn}).process(execution_id, Event(kind="Finish"))
print("survived restart ->", exe.active_path, "/", exe.outcome)
```

```text
survived restart -> Done / success
```

## What "durable" guarantees

The store is hardened beyond just persisting state:

- **Optimistic concurrency** — every record carries a `version`; a compare-and-set on write
  raises `StoreConflict` if another writer got there first (the single-writer backstop).
- **Transactional outbox** — a commit saves the execution *and* its emitted events in one
  transaction; a relay delivers them after commit, so a crash can never lose a `Finished`.
- **Dedupe** — processed event ids are recorded, so at-least-once delivery takes effect once.
- **Crash-safe orthogonal fork** — region children are created from a durable spawn-outbox,
  idempotently, so a crash mid-fork neither double-spawns nor loses a region's result.
- **Durable timers** — a `timeout` is persisted on enter and cancelled on exit in the *same*
  commit as the transition, so a scheduled timer can't be lost (see [step 7](../tutorial/07-timers)).
- **Execution trace (opt-in)** — with `HAREL_TRACE=1` (or `DurableRunner(..., trace=True)`) the
  commit also appends one timeline step (event, transition, actions, `context_out`) in the same
  transaction, so the [monitor](monitor) timeline is real. Off by default (zero hot-path cost);
  ring-capped to the last `HAREL_TRACE_MAX` steps. Per-backend storage in the [stores reference](stores).

## At-least-once actions & idempotency

Delivery is **at least once**. The dedupe above stops a *redelivered* event from re-running once
its prior attempt **committed** — but when an attempt runs its actions and then doesn't commit,
the event is redelivered and the actions **run again**. Dedupe is per *event*, not per *action*.
That happens when a worker crashes between an action and the commit — and, with nothing
crashing, when the commit loses to another writer of the same execution (a `StoreConflict`): a
control-plane command (`suspend`, `cancel`, …) or a `process()` landing while a worker runs the
step, or a lease that expired under an action slower than `visibility`.

For a side-effecting action (charge a card, send an email — local or a remote FaaS function) the
driver exposes a stable key, `stm.idempotency_key = {execution_id}:{step}:{index}:{action}`:
`step` is the id of the event being processed (`start` when the execution starts), `index` the
action's position in that step, `action` its name. Every attempt at the same event hands each
action the *same* key — even when another writer moved the execution on in between, since the
event's id, unlike the version, doesn't change. So give a retried submission the same
`Event(id=...)` (a webhook's own event id, a request id) and the key — and the dedupe — follow
it.

The dedupe must live in an **external** backend you own (Redis `SET NX`, a DynamoDB conditional
put, a service's native idempotency key) — **not** in harel's store or context. The gap is a crash
*before* the commit, so anything harel recorded would roll back with that failed commit; only a
record the callee wrote outside harel's transaction survives. `harel.idempotency` ships the opt-in
helper:

```python
# docs-test: skip
from harel import idempotent, DictIdempotency  # DictIdempotency is in-memory (tests)

backend = my_redis_backed_idempotency()         # an IdempotencyBackend you supply
actions = {"charge": idempotent(backend)(charge)}  # bind the wrapped action
```

`idempotent` runs the body at most once per `stm.idempotency_key` (caching the result, so a
selector still routes the same way). Residual window: true exactly-once needs the effect and the
claim to be atomic (an idempotency-key-native service); the helper narrows the window, it doesn't
abolish it.

## Changing a machine with executions in flight

A machine declares a version, and a change to it is a new version:

```text
machine order version 2 {
  …
}
```

A machine without `version` is version 1. Each execution records the version it was created with
and the *digest* of that version's source, and runs on it to the end; so do its regions, its
fan-out instances and the inline machines it `invoke`s. A new execution gets the highest version
the runner has loaded, or the one `create(..., version=1)` asks for.

### Published versions

The store keeps every version of every machine, so an execution finds the one it runs on
whichever process picks it up — whatever files that process has. A version is published from its
`.stm` file, with every file it imports, and is never changed once stored:

- a **worker publishes** the machines of its definitions directory when it starts. A version
  already published is left as it is; one published **with other content** — a machine changed
  without a new version — stops the worker before it takes any work, so the deploy fails, not the
  executions;
- **`harel publish DIR`** does the same from a deploy pipeline, against the store the
  `HAREL_STORE_*` settings name (`publish_definitions(store, read_definitions(dir))` in code);
- a runner that meets an execution of a version it hasn't loaded **reads it from the store**.
  So `order.stm` simply moves on to version 2: the workers keep running version 1's executions
  from the store, and no copy of the old file is kept anywhere.

To change a machine, bump its version and deploy. That's all.

A version read from the store is checked before it runs: it may call only the action modules the
runner's own machines call, plus those it is told to allow — `action_modules=` on a runner,
`HAREL_ACTION_MODULES` (comma-separated) for a worker; a module allows the modules under it. A
record written to the store by someone else can't make a worker run code its own machines don't.
The store keeps the machines, not the code of their actions: an action a published version
names has to stay importable, with the same meaning, for as long as executions run on it. What
the code binds — `definition_from_dsl(..., actions=, guards=)` rather than a `bind` in the DSL —
isn't in the store either: give the runner the same `actions=` and `guards=`, and it binds them
in the versions it reads from there (a handler it can't bind is reported like a version it
hasn't got).

### What harel checks

- **A version edited without a bump.** The digest covers the machine's block and what its file
  shares with it — events, guards, `bind`s, fragments, and each imported file whole, its machines
  included (by alias and content, not by where the file lives) — but not comments, layout or its
  own file's other machines, nor the code of the actions. Editing a machine in an imported file
  therefore needs a bump of every machine that imports it. Publishing such a version is refused (above). A runner whose own copy differs from the
  one an execution started on runs it on the published one, and logs a warning; if no published
  one matches either, the execution fails (`FAILED`) with a `DefinitionChanged` error —
  `process()` raises it instead under `on_action_error="raise"`, and the event that found it is
  consumed with the dead letter. `harel hash FILE` prints each machine's version and digest.
- **A version neither this runner nor the store has.** A worker logs a `DefinitionNotLoaded`
  error and puts the message back to be retried, for a worker that has it; a durable runner raises
  it to the caller and leaves that execution's timers armed. The execution is not touched. A
  published version this runner can't use is reported the same way: one that calls a module it
  doesn't allow, or an action its code can't import — an older worker meeting a version a newer
  deploy published, which a newer worker then runs.
- **The same version twice with different content** among the files a runner loads is refused:
  `load_definitions` and `definition_registry` raise. A file declares each machine once.
- **A state an execution is parked in, renamed or removed** — which a bump prevents, but an
  execution from before versions, or an edit without one, can still meet — fails it with a
  `DefinitionChanged` that names the missing state.

`redrive` brings back an execution failed this way: it is
[repositioned](control-plane.md#redrive--repairing-a-dead-letter) on a state of the machine
loaded for its version, and carries on with that source from then on. `cancel` and `terminate`
work on it as ever. A finished or suspended execution is not checked: only one the engine would
drive (`RUNNING`, or `CANCELLING` on its way to a `Cancel`).

An execution written before versions existed runs as version 1, and takes the digest of the
version 1 loaded the first time it is processed.

## Retry & backoff is a composite, not a feature

Because timers are durable and the model decides what a `Timeout` does, retry-with-backoff is
assembled, not configured:

- a `Waiting` state whose delay is read from context — `timeout context backoff` — so an
  `on enter` can compute the next wait;
- a `select` that branches *succeeded / retry again*;
- the composite's own `timeout` as the overall budget, whose `Timeout` exits to a failed
  terminal.

harel ships composable backoff actions in `harel.lib` — `exponential_backoff`,
`linear_backoff`, `reset_backoff` — ordinary `inputs`-parameterised actions that compute the next
delay. Policy stays in the model; the engine just keeps time. The repository's `retry.stm` /
`charge_retry.stm` are the complete pattern.
