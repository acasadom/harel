# Changelog

## Unreleased

### Breaking

- **A store's `commit` takes a `step`** (`ExecutionStore` and `AsyncExecutionStore`): every
  writer — the driver, the control plane, `DistributedRunner.create` — now passes a `Step`
  describing the commit. A store written outside harel must accept it (it may ignore it): one
  whose `commit` doesn't raises `TypeError`, and the store contracts in `harel.testing` pass a
  `step`, so its tests say so.

### Added

- **`Step`**, the description of a commit: what caused it (`create`, `start`, `event`,
  `control`), the event or the control-plane command, the execution's status and state before
  and after, the actions it ran. It is not persisted — the opt-in trace is the persisted
  timeline — and costs a small record per commit.
- **`ObservedStore(store, on_step)`** and `AsyncObservedStore`: wrap a store, and `on_step(step,
  execution)` hears about every commit once it has returned — for metrics, alerts, audit,
  notifications, without touching the machines or the runners. The status before tells a step
  into `FAILED` apart, which `finished_at` doesn't. A store whose commits join the caller's
  transaction reads `step` in its own `commit` instead, and defers to that transaction's commit.
  See the new [observing executions](docs/guide/observing.md) guide.

### Fixed

- **An action's idempotency key is the same on every attempt at an event**, also when another
  writer moved the execution on in between. The key was `{execution_id}:{version}:{index}`: a
  step whose commit lost to another writer — a control-plane command or a `process()` landing
  while a worker ran it, with nothing crashing — was redelivered, ran its actions on the new
  version, and handed them new keys, so a side effect deduped on the key happened twice. It is
  now `{execution_id}:{step}:{index}:{action}`, `step` being the event's id (`start` when the
  execution starts). The format changes: a dedupe record written under an old key won't match a
  retry made after the upgrade.
- The durability and distribution guides say when a step's actions run twice without a crash.

## 0.8.0 — 2026-10-04

### Fixed

- **The Postgres transport's `claim` no longer sorts every group.** Its query reads the groups in
  `(COALESCE(lock_expiry, 0), group_id)` order, which the index on `lock_expiry` couldn't serve,
  so each claim sorted the whole table — on disk past a few tens of thousands of groups (154 ms
  at 100,000 groups). An index on that order lets it stop at the first claimable group (0.03 ms).
  The old index is dropped. `SCHEMA_VERSION` is 3: a backend built with `create_schema=True`
  updates its schema when it starts; where a migration tool owns it, apply
  `sql_schema("postgres", prefix)` again.

### Changed

- **A declared event is checked when it comes in**: `process()` and `send()` (sync and async)
  raise `EventError`, a `ValueError` exported from `harel`, for an event whose data lacks a
  required field or has a value of the wrong type — in the caller's stack, before anything runs,
  commits or queues. Until now only `harel validate` checked events, statically: an incomplete
  event was accepted. Undeclared fields and events, and the engine's own (`Timeout`, `Cancel`,
  …), pass as before.

## 0.7.1 — 2026-10-04

### Fixed

- **The Postgres and Mongo transports could lose a message published while its group was being
  acked.** An `ack` that found the group drained deleted it, while a concurrent `publish` into
  the same group had inserted its message but found the group still there: the message was left
  without a group, and no `claim` would reach it. Postgres now locks the group's row in both
  `publish` and `ack` — one waits for the other. Mongo, which has no row lock, re-checks after
  the delete and readies the group again; `claim`'s cleanup of an empty group does the same.
- `SCHEMA_VERSION` is 2: the Postgres transport's `ack` function changed. A backend built with
  `create_schema=True` replaces it when it starts; where a migration tool owns the schema,
  apply `sql_schema("postgres", prefix)` again (it is idempotent).

## 0.7.0 — 2026-10-04

### Breaking

- **Every backend names what it creates with a `prefix`, default `"harel"`**: the SQL tables are
  `harel_executions`, `harel_outbox`, …, `harel_transport_messages`, `harel_transport_groups`
  (they had no prefix; the SQLite-family transport's were `messages` and `groups`), Redis keys
  `harel:…` (were `stm:…`), Mongo collections `harel_executions`, `harel_counters`,
  `harel_transport_*` (were `executions`, `counters`, `stm_*`), and the SQS transport's default
  queue is `harel.fifo` (was `stm.fifo`). DynamoDB's tables are unchanged. A 0.7 backend on a
  database from an earlier version starts empty beside the old data: [upgrading to
  0.7](docs/guide/upgrading.md) renames it in place, backend by backend.
- **The environment variables are `HAREL_*`** (were `STM_*`): `HAREL_STORE_BACKEND`,
  `HAREL_POSTGRES_DSN`, `HAREL_CONCURRENCY`, … — the same names with the new prefix. A variable
  still set under its old name is not read; each is logged with the name to use.
- The backends that took `prefix` positionally take it by keyword (`RedisStore`,
  `RedisTransport`, `PostgresTransport`, `MongoTransport`, `DynamoDBStore`, their async twins
  and constructors); `PostgresTransport`'s `prefix`, accepted and ignored before, now names its
  tables.

### Fixed

- A sync runner (`DurableRunner`, `DistributedRunner`, `Worker`, `Driver`) called from inside
  a running event loop raises in the background model too, on every call, as documented: once
  the shared background loop was up, the call ran instead — blocking the caller's loop for its
  whole duration.
- `harel render --mermaid` (`harel.viz.mermaid`) writes a `:` in a state description or an edge
  label (`on enter: …`, `outcome: …`, a guard's string) as Mermaid's entity `#58;`, still drawn
  as a colon: Mermaid 11.12 rejected the whole diagram.
- The NiceGUI example uses the async API (`AsyncDurableRunner` over an `AsyncSqliteStore`,
  `async` handlers): NiceGUI calls its handlers on its event loop, which every click blocked.

### Added

- **`prefix` on every persistent backend** — store and transport, sync and async — so several
  deployments, or harel and other applications, share one database. `harel.engine.schema.Names`
  gives the names under a prefix.
- **`create_schema`** (default `True`) on every backend with a schema: with `False` it creates
  no table, index or function, and expects them to exist — for a schema owned by a migration
  tool or infrastructure-as-code.
- **The SQL schema as data**: `harel.engine.schema.sql_schema(dialect, prefix)` (and
  `store_schema` / `transport_schema`) returns the statements the SQL backends run, for a
  migration tool to apply; `SCHEMA_VERSION` changes whenever the schema does.
- The worker reads `HAREL_PREFIX` and `HAREL_CREATE_SCHEMA`.
- **An examples section in the docs**: each example's machine, how it is built, its sequence
  diagrams, and the semantics and execution model it runs with, and why. The diagrams of the
  machines are checked against their `.stm` in CI.
- **The `inline_transaction` example**: a shop whose orders commit in its own transaction —
  `DurableRunner(execution="inline", on_action_error="raise")` over `ConnectionStore`, a store
  on the caller's connection written on `harel.engine.store.base`.
- **`shares_store_transaction`**, an optional attribute of a transport whose writes go into the
  store's transaction (the caller's connection): `create()` then lets a failed publish of the
  new execution's `Start` reach the caller instead of leaving it queued in the outbox.

## 0.6.1 — 2026-10-04

### Changed

- The control plane is written once, as flows in `harel.engine.control`: the sync functions
  there and their async counterparts in `harel.engine.aio.control` run the same flows, and so
  do the runners' `cancel`, `terminate`, `suspend`, `resume`, `purge` and `redrive`. Their
  signatures and behaviour are unchanged.
- The execution models guide separates a runner's semantics (wait for the result, or hand it to
  a worker) from its execution model (caller's thread, background loop, coroutines), with a
  sequence diagram for each and a table of every combination. The architecture and distribution
  guides describe the driver as flows.

### Fixed

- The sync `purge` / `purge_finished` (and a runner's `purge` with `execution="inline"`) refuse a
  coroutine-function `archive` with a `TypeError` before deleting anything. It was called and
  never awaited, so the tree was deleted without being archived. The async control plane, and
  the sync runners in the background model, await it as before.
- The distribution guide said the headless runner fires the timers due in one sweep
  concurrently; it delivers them one at a time (two timers of one execution must not race on its
  version) — only a worker publishes them concurrently.

## 0.6.0 — 2026-10-03

### Added

- **Execution models for the sync runners**: `DurableRunner`, `DistributedRunner`, `Worker`
  and the bare `Driver` take `execution="background"` (the default: the shared background
  event loop, as before) or `execution="inline"` — the caller's own thread, with no event loop,
  over a sync store and transport. Inline, every store call and action runs on the caller's
  connection and inside its transaction; a coroutine action is refused.
- **`harel.testing`**: the contracts harel runs on its own backends, for a backend written
  outside harel to run on itself — listing, purge and outbox for a store, and a new one for a
  transport (FIFO per group, one in flight per group, ack/nack, parking, lease expiry,
  round-robin, priorities), sync and async. The transport contract now also runs on every
  built-in transport.
- **`harel.engine.store.base` and `harel.engine.transport.base`**: the stable API for a backend
  author — the protocols, records, errors and the helpers the built-in backends share
  (`encode_offset`, `decode_offset`, `matches`, `listing_page`, `like_prefix`, `PARKED`).
- **`ContextError`, `ExpressionError`, `StoreConflict` and `PurgeRefused`** are exported from
  `harel`.
- **`on_action_error`** on `DurableRunner` and `AsyncDurableRunner`: `"fail"` (default:
  dead-letter the execution) or `"raise"` (the exception reaches the caller and the step isn't
  committed, so it rolls back with the caller's transaction).

### Changed

- The driver and the runners are written once, as flows that yield IO requests
  (`harel.engine.flow`, `driving`, `hosting`), run by an interpreter per execution model. The
  async runners keep their API and behaviour.

## 0.5.0 — 2026-10-03

Since 0.4.1.

### Added

- **Defaults for context fields**: `context { retries: int = 0  max_retries: int = 2 }`. An
  execution created without a defaulted field starts with its default (a list default is
  copied per execution), an invoked machine's child gets its own machine's defaults, and a
  `Reset` starts over with them (it used to leave the context empty, so a `set` reading a
  required field then failed); an orthogonal region still starts with only what its fork
  passes down. The default must be of
  the field's type (`harel validate`: `default_type_mismatch`), and a field with one can't also
  be optional (`?`). Event fields take no default.
- **A `set` on a `choose` branch**: `when ... to Refining set context.retries = context.retries + 1`
  (and on the `else`), applied only when that branch is taken, after the transition's own `set`
  and evaluated with it, against the context as the transition starts. Validated like a
  transition's `set`, and drawn on its branch.

### Fixed

- With a `context { ... }` schema, `harel validate` reported the `for` variable of a fan-out
  (`invoke X for item in coll with { k: item }`) as an unknown context field; it is the
  collection's entry. The collection itself is now checked against the schema.
- An empty list literal `[]` parsed as `[None]`, so `where x in []` held for a null `x`.

## 0.4.1 — 2026-10-03

Since 0.4.0.

### Added

- **A guard can compare two references**: the right side of a comparison may be `context.x` or
  `event.x`, not only a literal — `where context.retries >= context.max_retries`. Either side
  missing, or values that can't be compared, make it not hold. `harel validate` checks the
  right side like the left (declared context field, existing event field, no event on an
  automatic transition) and warns when the two declared types differ
  (`compare_type_mismatch`).

### Fixed

- `harel run` reports a context its machine's `context { ... }` refuses (pass it with
  `--seed`), and malformed JSON in `--seed` or an event's data, as `error: ...` — checked
  before anything runs — instead of a traceback.

### Documentation

- The examples are up to date with 0.4.0 (#93): typed contexts, `set`, `choose`, guards
  over references, and a carrier selector in `place_order`. `webhook_payment` keeps its
  store and queue in two SQLite files, and its timeout terminal is `Abandoned`. The monitor
  demo's seed runs the machines, as `python -m examples.monitor_demo.seed`. A test runs
  every example.
- The timers tutorial uses the DSL's syntax for a delay read from the context,
  `timeout context backoff`.

## 0.4.0 — 2026-10-02

Since 0.3.1. The changes under [Behavior changes](#behavior-changes) can change what an existing
machine or caller sees; read them before upgrading. Custom `ExecutionStore` implementations need
the changes under [Store protocol](#store-protocol).

### Added

- **Context in the model** (#85, #86). Guards read the execution context (`context.x`, next to
  the event's `x` / `event.x`); a machine can declare `context { field: type ... }`, checked on
  create (`ContextError`), by `validate()` and after each `set`; transitions can update it with
  `set context.f = <expr>` (a reference, a literal or one arithmetic operation; a failing
  expression raises `ExpressionError`, routed like an action error); and `choose` routes on
  guards tried in order, with an optional `else`. An orthogonal node passes context to its
  regions with `with { child: parent }`, as `invoke` does. See tutorial 17.
- **Retention** (#76, #77, #79, #82):
  - `purge(execution_id, archive=...)` on `control`, `aio.control` and every runner deletes a
    finished execution tree (root and every stored descendant); `JsonlArchive` keeps a copy
    first. A tree with a member that isn't `DONE`/`CANCELLED` is refused (`PurgeRefused`).
  - `control.purge_finished(older_than=...)` and the `harel purge --older-than AGE` command purge
    finished trees by age (`--archive`, `--status`, `--limit`, `--dry-run`,
    `--include-undated`); `aio.control.purge_finished` is the async one (#90).
  - Executions record `created_at`, `updated_at` and `finished_at`; listings include
    `finished_at`.
  - `ttl <seconds>` at machine level expires a root execution after that long without a domain
    event: the reserved `Expired` event is delivered, and an `on Expired` into a terminal ends
    it; without one it ends `CANCELLED` with outcome `expired`.
- **`redrive(execution_id, target_path)`** (#67): brings a dead-lettered (`FAILED`) execution
  back to `RUNNING` at a chosen leaf once its bug is fixed.
- **`start(execution_id, data=...)`** on `DistributedRunner` (#68, #71), and
  `create(..., start_on_create=False)` to create without starting.
- **`clock` keyword** on the control-plane commands (`cancel`, `terminate`, `suspend`, `resume`,
  `redrive`), which the runners pass their own clock to (#77).
- **Validator rules**: `cancel_target_not_terminal`, `expired_target_not_terminal`,
  `ttl_not_positive`, `unknown_context_field`, `assign_type_mismatch`,
  `event_ref_without_event` and `choose_can_hang` (errors); `expired_without_ttl` and
  `with_without_children` (warnings).
- **Diagrams** draw composite (`and`/`or`/`not`) and named guards, not only flat comparisons
  (#84).

### Behavior changes

- **`DistributedRunner.create()` no longer runs the model on the caller** (#68). It persists
  the execution and publishes its `Start`; the first transition runs on a worker like every
  later one. A domain event that reaches an execution still `PENDING` is discarded (and
  logged), not queued.
- **`send()` refuses a `Start` event** (`ValueError`, #71): `create()` and `start()` are the
  only ways to start an execution. A `Start` is honored only while the execution is `PENDING`.
- **`on Cancel` must land on a terminal that ends the execution** (#69, #73, #80), and so must
  `on Expired`. `validate()` reports it, and `cancel()` treats any other target as no handler
  and terminates forcefully. Cancellation that needs more than one step belongs on a domain
  event of its own (`CancelOrder`).
- **`cancel()` of a `FAILED` execution is always forceful** (#74): it becomes `CANCELLED` and
  keeps its `error`. `cancel()` and `terminate()` do nothing on a `DONE` or `CANCELLED`
  execution (#69).
- **An exception in `on_exit` is never routed through `on error`** (#66): it goes straight to
  the runner's default policy (dead letter).
- **`Reset` and `SetState` are guarded** (#70): `Reset` is refused while `CANCELLING` and
  cancels live children; `SetState` needs a `RUNNING` execution and a leaf target in its own
  branch, outside any unforked orthogonal state.
- **A `Timeout` reaches the execution that armed it**, never its orthogonal regions (#78). On
  the worker path a fork's own `timeout` used to go to its regions and never fire.
- **A guard over values that can't be compared doesn't hold** (#83) — `"abc" < 3`,
  `None < 3`, `x in 5` — instead of raising out of the engine.
- **A message the worker fails to handle comes back after `suspend_recheck` seconds** (5 by
  default) instead of when its lease expires (`visibility`, 30 by default), and the failure is
  logged with its traceback (#90). It covers store or transport outages and engine bugs, not
  action errors, which the driver routes as before.

### Store protocol

For custom `ExecutionStore` / `AsyncExecutionStore` implementations:

- **`commit(...)` returns `list[int]`**: the `seq` of each outbox entry it enqueued, in `emits`
  order (`[]` when nothing was emitted) (#87).
- **`purge(execution_id, expected_version) -> bool`**: delete the execution and every row keyed
  by it, iff the version still matches (#76).
- **`ids_with_prefix(prefix) -> list[str]`**: the stored ids starting with `prefix`, matched
  literally (#82).
- **`list_executions(...)`** is now part of the async protocol too (`AsyncExecutionStore`), as
  `aio.control.purge_finished` needs it (#90).
- A stale copy's `commit` after a purge must raise `StoreConflict`, not recreate the
  execution (#76).

### Fixed

- `create()`/`start()` left their `Start` in the outbox, so a later flush published it again
  (#87).
- Async `redrive` now clears history, as the sync one does (#72).
- SQLite: async store and transport serialize transactions per connection, and `save`/`commit`
  roll back on any error, not only a conflict (#75).
- SQLite and libSQL transports: the claim walks the groups on an index and stops at the first
  one it can lease, instead of reading the whole backlog — its cost no longer grows with the
  queue (a worker drained a 4000-execution backlog about 3× faster) (#90).
- `LibsqlStore`: `save`/`commit` roll back on any error, not only a conflict (#90).
- `DistributedRunner(trace=True)` records its worker's steps (the sync `Worker` takes `trace`),
  each step carries the event that drove it, and the in-memory stores keep each step's context
  as it was (#90).
- `purge_finished(dry_run=True)` applies the same whole-tree check as a real run (#81).
- The benchmarks in `bench/` run the machine they describe again, and gained end-to-end modes
  with several producer processes (#88).
