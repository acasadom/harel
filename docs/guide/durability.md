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

The networked ones are optional extras (`harel[redis]`, `[postgres]`, `[rqlite]`, `[mongo]`, `[surrealdb]`) and take an
injected client. Selecting one is the *only* change — the machine and the driving loop are
identical.

## Surviving a restart

Point a `SqliteStore` at a file, and an execution created by one runner is picked up by another
— a stand-in for "the process died and came back":

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

# process #1: create the execution, then "crash"
exe = DurableRunner(SqliteStore(db), {defn.id: defn}).create(defn.id)

# process #2: a brand-new runner over the same file picks it up and finishes it
exe = DurableRunner(SqliteStore(db), {defn.id: defn}).process(exe.id, Event(kind="Finish"))
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
