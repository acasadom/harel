# DictStore — in-memory (default)

`DictStore` is the in-memory `ExecutionStore`: a handful of plain Python dicts, lists and
sets living in the process. It is the **default** backend for embedded, non-durable runs —
tests, scenarios, notebooks, single-process hosts. Its defining property is that `load`
returns the **same `Execution` object** that was handed to `save`/`commit`: there is no
serialization round-trip, no JSON blob, no copy. A caller that holds a reference to an
Execution sees every mutation the engine makes, and `load` hands back that identical object
— the identity contract the in-place test harness relies on. Nothing survives the process
exiting.

## Data model

`DictStore.__init__` allocates exactly these structures (all live on `self`):

```text
self._by_id    : dict[str, Execution]            # execution_id -> the live Execution object
self._outbox   : list[OutboxEntry]               # FIFO of deferred events awaiting delivery
self._processed: set[tuple[str, str]]            # {(execution_id, event_id)} already-handled marks
self._timers   : dict[tuple[str, str], float]    # (execution_id, path) -> fire_at (epoch seconds)
self._spawns   : list[SpawnEntry]                # FIFO of pending child-Execution creations
self._trace    : dict[str, list[dict]]           # execution_id -> ordered trace steps (capped ring)
self._trace_idx: dict[str, int]                  # execution_id -> next monotonic step index
self.trace_max : int = DEFAULT_TRACE_MAX         # ring size (200); 0/None disables trimming
self._seq      : int = 0                          # monotonic counter for outbox seq numbers
self._spawn_seq: int = 0                          # monotonic counter for spawn seq numbers
```

What each one holds and why:

- **`_by_id`** — the entire authoritative state of the store. Maps an Execution id to the
  *live* `Execution` instance (see `execution.py`: `id`, `definition_id`, `status`, `outcome`,
  `active_path`, `history`, `context`, `version`, the orthogonal `parent_id`/`child_id`/
  `children`, etc.). Because the value is the object itself, no deserialization is needed and
  identity is preserved across `save`/`load`.

- **`_outbox`** — the **transactional outbox**: a FIFO list of `OutboxEntry(seq, target_id,
  event)`. `commit` appends an entry for every emitted event so delivery happens *after* the
  Execution is durably saved (here: after the dict is mutated), never inline. A relay drains
  it via `pending_outbox`/`ack_outbox`. `target_id` is the Execution to deliver to (or `None`
  for an untargeted event); `seq` is a monotonic ack token from `_seq`.

- **`_processed`** — the **dedupe set** for at-least-once delivery. Holds
  `(execution_id, event_id)` pairs. If a redelivered event's id is already in the set, the
  worker skips it. Populated by `commit`'s `processed_event_id`, queried by `is_processed`.

- **`_timers`** — durable timers, keyed by `(execution_id, path)` mapping to the absolute
  `fire_at` (epoch seconds). The composite key means **re-entry replaces**: re-arming the
  timer for the same state path overwrites the prior `fire_at` rather than stacking timers.
  A `TimerOp(action="schedule")` upserts; `action="cancel"` removes.

- **`_spawns`** — the **spawn outbox**: a FIFO of `SpawnEntry(seq, parent_id, child_id,
  root_path, context)`, the orthogonal-fork sibling of `_outbox`. When a parent enters an
  AND-state, `commit` records the parent's advance + its join expectations (the `children`
  dict on the Execution) *and* the intents to create each region child, all in one shot. A
  relay later creates the children idempotently via `pending_spawns`/`ack_spawn`. `seq` comes
  from `_spawn_seq`.

- **`_trace`** — the opt-in execution timeline: `execution_id -> list[dict]`, one dict per
  recorded step (event/transition/actions/context_out plus a stamped `index`). Capped to the
  last `trace_max` steps per id (a ring). This is a side-channel for monitoring; `load` never
  reads it and is unaffected by it.

- **`_trace_idx`** — `execution_id -> next monotonic step index`. Survives ring trimming, so
  the stamped `index` keeps climbing (0, 1, 2, …) even after old steps are dropped — readers
  see a stable, gap-free-from-the-store's-view ordering of recent steps.

- **`trace_max`** — the ring size, defaulting to `DEFAULT_TRACE_MAX = 200` (from `_base.py`).
  A truthy value trims; `0`/`None` disables trimming (unbounded).

- **`_seq` / `_spawn_seq`** — independent monotonic counters incremented *before* each append,
  so the first outbox/spawn entry gets `seq == 1`. They give every entry a stable ack token.

## Operations

### `load(execution_id) -> Optional[Execution]`

```text
return self._by_id.get(execution_id)
```

Returns the live object (or `None`). **No copy, no deserialization** — the caller gets the
exact instance under that id. This is what makes the store's identity contract hold.

### `save(exe) -> None` — the CAS check

```text
prev = self._by_id.get(exe.id)
if prev is not None and prev is not exe and prev.version != exe.version:
    raise StoreConflict(exe.id, expected=exe.version, found=prev.version)
exe.version += 1
self._by_id[exe.id] = exe
```

This is the optimistic-concurrency (CAS) gate, the single-writer backstop. The condition
`prev is not None and prev is not exe and prev.version != exe.version` bites **only** when a
*different* object is already stored under the same id at a *different* version — i.e. a
genuine concurrent writer that loaded the row, advanced it, and committed while this caller
held a stale copy. It raises `StoreConflict(execution_id, expected, found)` so the caller can
reload and retry (or drop the stale work).

Three deliberate exemptions:

- **Same object** (`prev is exe`): the common embedded case where the engine mutates the very
  object the store holds. `prev is not exe` is false, so the check is skipped and the write
  always wins — there is no other writer.
- **First write** (`prev is None`): nothing to conflict with.
- **Different object, same version**: not a conflict — it's a legitimate replacement at the
  expected version.

On success `exe.version` is bumped in place to the committed value and the dict entry is set.

### `commit(exe, emits, processed_event_id=None, timers=(), spawns=(), trace=None)`

The atomic event-boundary write. Because everything is in-process and synchronous, "atomic"
here means it all runs without interleaving. Step by step:

1. **CAS first** — `self.save(exe)`. This raises `StoreConflict` *before* any emit is
   enqueued, so a losing writer never pollutes the outbox.
2. **Outbox append** — for each `(target_id, event)` in `emits`: bump `_seq`, append
   `OutboxEntry(self._seq, target_id, event)`. Deferred delivery, post-save.
3. **Dedupe** — if `processed_event_id is not None`, add `(exe.id, processed_event_id)` to
   `_processed`, marking the just-handled event so a redelivery is skipped.
4. **Timer ops** — for each `TimerOp` in `timers`: `schedule` sets
   `self._timers[(exe.id, op.path)] = op.fire_at` (upsert/replace); anything else (`cancel`)
   does `self._timers.pop((exe.id, op.path), None)`.
5. **Spawn append** — for each `(child_id, root_path, context)` in `spawns`: bump `_spawn_seq`,
   append `SpawnEntry(self._spawn_seq, exe.id, child_id, root_path, dict(context))`. Note
   `dict(context)` — the context is **copied** here so the spawn intent is snapshotted
   independently of later parent mutations (one of the few copies the store makes).
6. **Trace record** — if `trace is not None`, `self._record_trace(exe.id, trace)`.

Why one call: the parent's advance, its join expectations, the events it emitted, the dedupe
mark, the timers it armed/cancelled, and the children it wants forked must all land together
or not at all. The CAS guards the whole boundary.

### `is_processed(execution_id, event_id) -> bool`

```text
return (execution_id, event_id) in self._processed
```

The dedupe query. True iff that exact pair was recorded by a prior `commit`.

### `pending_outbox() -> list[OutboxEntry]` / `ack_outbox(seq)`

```text
pending_outbox: return list(self._outbox)            # a copy, oldest first
ack_outbox:     self._outbox = [e for e ... e.seq != seq]   # drop the acked entry
```

The relay reads pending entries, delivers them, then acks each by `seq` — `ack_outbox`
rebuilds the list without that entry. Returning a *copy* means iterating the relay's snapshot
is safe even if `commit` appends concurrently (single loop, but still defensive).

### `pending_spawns() -> list[SpawnEntry]` / `ack_spawn(seq)`

```text
pending_spawns: return list(self._spawns)            # a copy, oldest first
ack_spawn:      self._spawns = [s for s ... s.seq != seq]   # drop the acked spawn
```

The orthogonal-fork twin of the outbox pair. The relay drains pending spawns, creates each
child Execution **idempotently** (skip if it already exists), then acks by `seq`.

### `due_timers(now) -> list[tuple[str, str, float]]`

```text
return [(eid, path, fa) for (eid, path), fa in self._timers.items() if fa <= now]
```

Returns every armed timer whose `fire_at <= now`, as `(execution_id, path, fire_at)`. The
sweep calls this on the idle path and delivers a `Timeout` event for each.

### `delete_timer(execution_id, path, fire_at)`

```text
if self._timers.get((execution_id, path)) == fire_at:
    del self._timers[(execution_id, path)]
```

Removes the timer **only if it still holds the exact `fire_at`** that fired. This guard is the
point: if the model re-scheduled the same `(execution_id, path)` to a *new* time between the
sweep reading it and deleting it, the stored `fire_at` no longer matches, the delete is a
no-op, and the freshly re-armed timer survives a stale sweep.

### Trace: `_record_trace` / `append_trace` / `read_trace`

```text
_record_trace(execution_id, entry):
    idx = self._trace_idx.get(execution_id, 0)
    self._trace_idx[execution_id] = idx + 1
    steps = self._trace.setdefault(execution_id, [])
    steps.append({**entry, "index": entry.get("index", idx)})
    if self.trace_max and len(steps) > self.trace_max:   # ring: keep only the last N
        del steps[: len(steps) - self.trace_max]

append_trace(execution_id, entry): self._record_trace(execution_id, entry)   # demo/test seam
read_trace(execution_id):          return list(self._trace.get(execution_id, []))  # a copy
```

`_record_trace` is the internal recorder `commit` calls; `append_trace` is the public
demo/test seam that delegates to it; `read_trace` returns a copy of the per-id step list (or
`[]`).

## The trace ring

`_record_trace` assigns each step a **monotonic per-id index** and trims the list to a ring:

1. Read the next index for this id from `_trace_idx` (default `0`).
2. Advance `_trace_idx[execution_id]` by 1 — so the counter keeps climbing regardless of
   trimming.
3. Append `{**entry, "index": entry.get("index", idx)}`: the caller's entry, stamped with an
   `index` if it didn't already carry one (an explicit `index` in the entry is honored).
4. If `trace_max` is truthy and the list now exceeds it, `del steps[: len(steps) - trace_max]`
   drops the oldest overflow, keeping exactly the **last `trace_max`** steps.

The result: `_trace[id]` is a bounded window of the most recent steps, but the stamped
`index` values reflect the true monotonic position in the full timeline (older indices are
simply no longer present once trimmed). Default window: 200 steps.

## Async twin

`harel/engine/aio_store/dict.py` defines `AsyncDictStore`, a faithful mirror of `DictStore`:

- **Identical structures** — the same `_by_id`, `_outbox`, `_processed`, `_timers`, `_spawns`,
  `_trace`, `_trace_idx`, `trace_max`, `_seq`, `_spawn_seq`, with the same shapes and the same
  `DEFAULT_TRACE_MAX`.
- **Identical logic** — the same CAS condition, the same commit ordering (CAS first, then
  outbox/dedupe/timers/spawns/trace), the same `delete_timer` guard, the same ring trimming.
- **`async def` surface** — `load`, `save`, `commit`, `is_processed`, `pending_outbox`,
  `ack_outbox`, `pending_spawns`, `ack_spawn`, `due_timers`, `delete_timer`, `close`, and the
  trace seams `read_trace`/`append_trace` are coroutines, to satisfy the `AsyncExecutionStore`
  contract.
- **No lock** — and none is needed. A single event loop schedules cooperatively, and **none of
  these methods `await` internally**, so each method body runs atomically between suspension
  points. There is no point at which another task can interleave mid-method, so the dict
  mutations are race-free without explicit locking. (`_record_trace` stays a plain `def`,
  called synchronously from inside `commit`/`append_trace`.)

## Why it's not durable

All state lives in the process's `_by_id`/`_outbox`/`_timers`/`_spawns`/`_processed`/`_trace`
dicts. When the process exits, it's gone — there is no file, no socket, no JSON blob, no fsync.
`close()` is a no-op precisely because there is nothing to release. That is the trade: zero
serialization cost and live-object identity in exchange for no persistence and no
cross-process sharing.

For durability and multi-process operation, use a durable backend (SQLite for one machine;
Redis / Postgres / rqlite for distributed) — they serialize the Execution to a JSON blob with
a broken-out `version` column and implement the same `ExecutionStore` Protocol, so swapping is
a one-line change. See [the stores hub](../stores) and [durability](../durability).
