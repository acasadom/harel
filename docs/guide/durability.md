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
| `RedisStore` | distributed (WATCH/MULTI CAS) |
| `PostgresStore` | distributed (UPDATE-WHERE-version CAS) |
| `RqliteStore` | distributed SQLite over Raft (HTTP) |
| `MongoStore` | distributed document store (single-document atomic `update_one` CAS) |
| `SurrealStore` | distributed multi-model, ACID (server-side `BEGIN…COMMIT`, THROW-gated CAS) |
| `DynamoDBStore` | AWS serverless (conditional-write CAS, atomic `TransactWriteItems`; pairs with `SqsTransport`) |

The networked ones are optional extras (`harel[redis]`, `[postgres]`, `[rqlite]`, `[mongo]`, `[surrealdb]`, `[dynamodb]`) and take an
injected client. Selecting one is the *only* change — the machine and the driving loop are
identical.

## Surviving a restart

Point a `SqliteStore` at a file, and an execution created by one runner is picked up by another
**using only its id** — the state lives in the store, the id is the handle that crosses the
process boundary. A stand-in for "the process died and came back":

```python
import tempfile
from pathlib import Path

from harel import definition_from_dsl, DurableRunner, SqliteStore, Event

SOURCE = """
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

## At-least-once actions & idempotency

Delivery is **at least once**. The dedupe above stops a *redelivered* event from re-running once
its prior attempt **committed** — but if a worker crashes *after* an action ran and *before* the
commit, the event is redelivered and the action **runs again**. Dedupe is per *event*, not per
*action*.

For a side-effecting action (charge a card, send an email — local or a remote FaaS function) the
driver exposes a stable key, `stm.idempotency_key = {execution_id}:{version}:{index}`. It is
deterministic — the pure engine reproduces the same action sequence and the version is the
pre-commit value — so a redelivery hands each action the *same* key.

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
