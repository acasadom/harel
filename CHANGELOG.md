# Changelog

## Unreleased

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
