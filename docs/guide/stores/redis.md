# RedisStore — all-network, no shared filesystem

`RedisStore` is a durable `ExecutionStore` ([`store/redis.py`](../../../src/harel/engine/store/redis.py))
that keeps every Execution, its transactional outbox, its dedupe marks, its durable timers and its
optional execution trace **in Redis** — over the network, with no shared filesystem. That is its
defining property versus `SqliteStore`: there is no local file and no shared volume, so the same
store works across machines and containers as long as they can all reach the same Redis. It is the
natural partner of `RedisTransport` for a **pure-Redis stack** (one backend for both the persistence
seam and the queue seam — see [distribution](../distribution)).

The Redis client is **injected** (duck-typed), so `redis` stays an optional extra: the engine never
imports it at module load, the constructor only grabs `WatchError` lazily, and the test suite passes
a `fakeredis` client instead of a real server. A convenience `from_url` classmethod does the lazy
`import redis` for you:

```text
RedisStore(client, prefix="stm")          # inject any redis-py-compatible client (incl. fakeredis)
RedisStore.from_url("redis://host:6379/0") # lazily imports redis, builds redis.Redis.from_url(...)
```

Every key the store touches is namespaced by `prefix` (default `"stm"`) through the one-line helper:

```text
def _k(self, suffix: str) -> str:
    return f"{self._prefix}:{suffix}"
```

So `_k("exe:abc")` is the key `stm:exe:abc`. Two `RedisStore`s with different prefixes share one
Redis instance without colliding.

## Key space

Redis has no rows or columns, so each concern is mapped to the Redis data type whose native
operations match how the store reads it back. Under the `prefix`:

```text
exe:{id}            string   the Execution as JSON (model_dump_json)
outbox              hash     {seq -> {"t": target_id, "e": event_json}}  deferred events awaiting delivery
outbox:seq          string   INCR counter handing out the monotonic outbox seqs
spawns              hash     {seq -> {"p": parent_id, "c": child_id, "r": root_path, "x": context}}
spawns:seq          string   INCR counter for spawn seqs
processed:{id}      set      the event ids this execution has already handled (dedupe)
timers              zset     member "{id}\x00{path}" scored by fire_at (epoch seconds)
trace:{id}          list     trace steps as JSON, oldest first (ring-trimmed)
trace:seq:{id}      string   per-execution INCR counter → the 0-based trace index
```

Why each type:

- **`exe:{id}` is a plain string** holding the whole Execution JSON. A load is a single `GET`; a
  commit is a single `SET`. The version lives *inside* the JSON (`"version"`), which is the reason
  list/filter queries below have to read and parse the value client-side.
- **`outbox` is a hash, not a list.** Each deferred event is a field keyed by its monotonic `seq`.
  A hash gives O(1) `HDEL` to ack a single delivered entry by seq without scanning a list, while
  `HGETALL` still returns the whole backlog to sort. `outbox:seq` is a separate string counter that
  `INCR` advances to hand out the seqs (see the commit flow — it must be allocated *before* the
  transaction).
- **`spawns` is a hash** with the same shape and the same rationale: pending orthogonal-fork child
  creations keyed by seq, `HDEL`-acked one at a time, with `spawns:seq` as its `INCR` counter. The
  field value packs the four parts of a `SpawnEntry`: `p` parent id, `c` child id, `r` root path,
  `x` the child's seed context.
- **`processed:{id}` is a set.** Dedupe under at-least-once delivery is exactly a membership test:
  `SADD` to record a handled event id, `SISMEMBER` to ask "did we already process this?". The set
  deduplicates ids for free.
- **`timers` is a sorted set** scored by `fire_at`. This is the whole reason for the ZSET: `due_timers`
  is a **range query** — "every timer whose fire time is ≤ now" is `ZRANGEBYSCORE -inf now`, which a
  ZSET answers natively and in score order. The member packs the identity as `"{id}\x00{path}"` (a NUL
  separator), so one global ZSET holds the timers of every execution and the member decodes back to
  `(execution_id, path)`.
- **`trace:{id}` is a list** of step JSON, appended at the tail with `RPUSH` and trimmed with
  `LTRIM` to the last `trace_max` entries — a **ring buffer**: the list keeps only the most recent N
  steps. `trace:seq:{id}` is a per-execution `INCR` counter that stamps each step with a stable
  0-based `index` (so the position survives the ring dropping older entries).

## The CAS commit (WATCH/MULTI/EXEC)

`commit` is the one atomic write per event: it persists the Execution, enqueues its emitted events
into the outbox, records the processed event id, applies the timer ops, enqueues spawn intents, and
appends the optional trace step — **all or nothing**, with an optimistic-concurrency check on the
Execution's `version`. Redis gives this without Lua via `WATCH`/`MULTI`/`EXEC`, which is what lets
`fakeredis` run the same code.

**Step 1 — allocate the seqs up front, OUTSIDE the transaction.** The outbox/spawn seqs and the
trace index come from `INCR`, and `INCR` cannot return its value from inside a `MULTI` block (queued
commands return only their reply after `EXEC`). So they are incremented eagerly, before `WATCH`:

```text
queued = [(int(self._r.incr(self._k("outbox:seq"))), t, e.model_dump_json()) for t, e in emits]
queued_spawns = [(int(self._r.incr(self._k("spawns:seq"))), cid, rp, ctx) for cid, rp, ctx in spawns]
trace_step = None
if trace is not None:
    idx = int(self._r.incr(self._k(f"trace:seq:{exe.id}"))) - 1   # 0-based: returned value minus 1
    trace_step = json.dumps({**trace, "index": idx})
```

A seq burned by a transaction that later aborts is **harmless** — seqs only need to be monotonic and
unique, never gapless. So spending one eagerly and then aborting just leaves a hole, which nothing
depends on.

**Step 2 — WATCH the key, read the current version, run the CAS check.** `WATCH` arms optimistic
locking on `exe:{id}`: if anything writes that key between now and `EXEC`, the `EXEC` aborts. Inside
the watch the store reads the *currently stored* version and compares it to the version the caller
loaded at (`old = exe.version`):

```text
key = self._k(f"exe:{exe.id}")
old = exe.version
with self._r.pipeline() as pipe:
    try:
        pipe.watch(key)
        current = pipe.get(key)
        cur_version = json.loads(current)["version"] if current is not None else None
        if not (current is None and old == 0) and cur_version != old:
            pipe.unwatch()
            raise StoreConflict(exe.id, expected=old, found=cur_version)
        exe.version = old + 1
```

The guard reads as: *unless this is a fresh insert* (`current is None and old == 0`), the stored
version must equal the loaded version; if it moved, another writer won → `StoreConflict` (and we
`unwatch` first to release the watch cleanly). On success the Execution's version is bumped to
`old + 1`, which is what gets serialized below.

**Step 3 — MULTI: queue all the writes, then EXEC them atomically.** `pipe.multi()` opens the
transaction; every following command is *queued*, not run, and `pipe.execute()` runs them as one
atomic unit (and only if the watched key is untouched):

```text
        pipe.multi()
        pipe.set(key, exe.model_dump_json())
        for seq, target_id, event_json in queued:
            pipe.hset(self._k("outbox"), str(seq), json.dumps({"t": target_id, "e": event_json}))
        if processed_event_id is not None:
            pipe.sadd(self._k(f"processed:{exe.id}"), processed_event_id)
        for seq, cid, rp, ctx in queued_spawns:
            pipe.hset(
                self._k("spawns"),
                str(seq),
                json.dumps({"p": exe.id, "c": cid, "r": rp, "x": ctx}),
            )
        for op in timers:
            member = f"{exe.id}\x00{op.path}"
            if op.action == "schedule":
                pipe.zadd(self._k("timers"), {member: op.fire_at})
            else:
                pipe.zrem(self._k("timers"), member)
        if trace_step is not None:
            tkey = self._k(f"trace:{exe.id}")
            pipe.rpush(tkey, trace_step)
            if self.trace_max:
                pipe.ltrim(tkey, -self.trace_max, -1)  # ring: keep the last N
        pipe.execute()
    except self._WatchError:
        exe.version = old  # a concurrent writer won between WATCH and EXEC
        raise StoreConflict(exe.id, expected=old, found=None)
```

So one `EXEC` carries: the Execution `SET`, an `HSET` per outbox event, the dedupe `SADD`, an `HSET`
per spawn, a `ZADD`/`ZREM` per timer op, and the trace `RPUSH` + `LTRIM`. Either they all land or none
do.

**Why WATCH/MULTI is the right primitive.** It gives atomic compare-and-swap *without* a server-side
Lua script: the version check happens under `WATCH` (so the read is consistent with the commit), and
the writes run in a `MULTI` block that aborts if the watched key changed. A concurrent writer that
touches `exe:{id}` between the `WATCH` and the `EXEC` makes `EXEC` fail; redis-py surfaces that as
`WatchError`, which the store catches, rolls `exe.version` back to `old`, and re-raises as
`StoreConflict`. Avoiding Lua is deliberate — `fakeredis` supports `WATCH`/`MULTI`/`EXEC`, so the
exact production code path runs under test.

## The trace ring

The execution trace (opt-in, off by default) is a bounded ring: only the last `trace_max` steps
(`DEFAULT_TRACE_MAX = 200`) are kept. Inside `commit`, **before** the `MULTI`, the per-execution
index is allocated with `INCR` (0-based = returned value − 1) and stamped into the step JSON:

```text
idx = int(self._r.incr(self._k(f"trace:seq:{exe.id}"))) - 1
trace_step = json.dumps({**trace, "index": idx})
```

Then **inside** the `MULTI` the step is appended at the tail and the list is trimmed to its last N:

```text
tkey = self._k(f"trace:{exe.id}")
pipe.rpush(tkey, trace_step)
if self.trace_max:
    pipe.ltrim(tkey, -self.trace_max, -1)  # ring: keep the last N
```

`LTRIM tkey -trace_max -1` keeps only the final `trace_max` elements, dropping the oldest as the
list grows — a fixed-size ring. The `index` lives *inside* the JSON, so it stays meaningful even
after the ring evicts the entries before it. `read_trace` reads the whole list and parses each step:

```text
def read_trace(self, execution_id: str) -> list[dict]:
    return [json.loads(x) for x in self._r.lrange(self._k(f"trace:{execution_id}"), 0, -1)]
```

`append_trace` is the standalone (non-`commit`) writer — same `INCR`-index-then-`RPUSH`-then-`LTRIM`
shape, honoring an `index` already on the entry if present:

```text
def append_trace(self, execution_id: str, entry: dict) -> None:
    idx = int(self._r.incr(self._k(f"trace:seq:{execution_id}"))) - 1
    tkey = self._k(f"trace:{execution_id}")
    self._r.rpush(tkey, json.dumps({**entry, "index": entry.get("index", idx)}))
    if self.trace_max:
        self._r.ltrim(tkey, -self.trace_max, -1)
```

## Reads & sweeps

**`load`** is one `GET` on the Execution string, parsed back through pydantic (or `None` if absent):

```text
def load(self, execution_id: str) -> Optional[Execution]:
    raw = self._r.get(self._k(f"exe:{execution_id}"))
    return Execution.model_validate_json(raw) if raw is not None else None
```

**`list_executions`** is the awkward one, because Redis cannot query inside a value. The store
`SCAN`s the `exe:*` keys, `MGET`s the batch, parses each into an `ExecutionSummary`, and filters
**client-side** with the shared `_matches` (status / definition_id / roots_only). The page cursor is
the **native SCAN cursor**:

```text
cur = int(cursor) if cursor else 0
new_cur, keys = self._r.scan(cursor=cur, match=self._k("exe:*"), count=max(limit, 20))
items = []
for raw in self._r.mget(keys) if keys else []:
    if not raw:
        continue
    data = json.loads(raw)
    summary = ExecutionSummary.from_data(data, data.get("version", 0))
    if _matches(summary, status, definition_id, roots_only):
        items.append(summary)
items.sort(key=lambda s: s.id)  # within-page order only (no global order in SCAN)
return ExecutionPage(items=items, next_cursor=str(new_cur) if new_cur != 0 else None)
```

Ordering is therefore **best-effort**: `SCAN` returns keys in an arbitrary, cursor-driven order with
no global sort, and the status/definition/roots filters run after the fact because those fields live
inside the JSON blob, not in queryable columns. The `items.sort(key=...id)` only orders *within one
page*. A page may also come back smaller than `limit` (the filter rejected some), so callers keep
paging while `next_cursor` is non-`None`.

**`is_processed`** is the dedupe membership test — one `SISMEMBER`:

```text
def is_processed(self, execution_id: str, event_id: str) -> bool:
    return bool(self._r.sismember(self._k(f"processed:{execution_id}"), event_id))
```

**Outbox relay** — `pending_outbox` reads the whole hash with `HGETALL`, rebuilds each `OutboxEntry`,
and sorts by seq so delivery is oldest-first; `ack_outbox` removes a delivered entry with `HDEL`:

```text
def pending_outbox(self) -> list[OutboxEntry]:
    entries = []
    for seq_raw, val_raw in self._r.hgetall(self._k("outbox")).items():
        payload = json.loads(val_raw)
        entries.append(OutboxEntry(int(seq_raw), payload["t"], Event.model_validate_json(payload["e"])))
    return sorted(entries, key=lambda e: e.seq)

def ack_outbox(self, seq: int) -> None:
    self._r.hdel(self._k("outbox"), str(seq))
```

**Spawn relay** — identical shape over the `spawns` hash; `pending_spawns` `HGETALL`s and sorts by
seq, `ack_spawn` `HDEL`s:

```text
def pending_spawns(self) -> list[SpawnEntry]:
    entries = []
    for seq_raw, val_raw in self._r.hgetall(self._k("spawns")).items():
        p = json.loads(val_raw)
        entries.append(SpawnEntry(int(seq_raw), p["p"], p["c"], p["r"], p["x"]))
    return sorted(entries, key=lambda s: s.seq)

def ack_spawn(self, seq: int) -> None:
    self._r.hdel(self._k("spawns"), str(seq))
```

**Timer sweep** — `due_timers` is the ZSET range query: every member scored ≤ `now`, with its score,
decoding the `"{id}\x00{path}"` member back into `(execution_id, path)`:

```text
def due_timers(self, now: float) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    for member_raw, score in self._r.zrangebyscore(self._k("timers"), "-inf", now, withscores=True):
        member = member_raw.decode() if isinstance(member_raw, (bytes, bytearray)) else member_raw
        execution_id, _, path = member.partition("\x00")
        out.append((execution_id, path, float(score)))
    return out
```

`delete_timer` is **guarded on the stored score**: it only `ZREM`s the timer if the current score is
still exactly the `fire_at` the caller knows about. This protects a concurrent re-schedule: if the
model re-armed the same `(execution_id, path)` to a *new* time, a stale sweep trying to delete the
*old* one finds a mismatched score and leaves the new timer intact:

```text
def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
    member = f"{execution_id}\x00{path}"
    score = self._r.zscore(self._k("timers"), member)
    if score is not None and float(score) == fire_at:
        self._r.zrem(self._k("timers"), member)
```

`close` calls `self._r.close()` on the injected client.

## Async twin

`AsyncRedisStore` ([`aio_store/redis.py`](../../../src/harel/engine/aio_store/redis.py)) is the
native-async mirror over `redis.asyncio`: the same key space, the same `WATCH`/`MULTI`/`EXEC`
version-CAS (still no Lua, so `fakeredis.aioredis` runs it), the same outbox-hash / dedupe-set /
timers-ZSET / trace-ring. Every Redis call is `await`ed and the pipeline is an `async with`:

```text
async with self._r.pipeline() as pipe:
    await pipe.watch(key)
    current = await pipe.get(key)
    ...
    pipe.multi()
    pipe.set(key, exe.model_dump_json())
    ...
    await pipe.execute()
```

It adds `load_for_event`, which folds the load and the dedupe check into **one round-trip** by
pipelining the `GET` and the `SISMEMBER` (non-transactional pipeline, just batching):

```text
async def load_for_event(self, execution_id, event_id):
    pipe = self._r.pipeline(transaction=False)
    pipe.get(self._k(f"exe:{execution_id}"))
    pipe.sismember(self._k(f"processed:{execution_id}"), event_id)
    raw, hit = await pipe.execute()
    if raw is None:
        return None, False
    return Execution.model_validate_json(raw), bool(hit)
```

One difference to note: `close` uses the async client's `aclose()` (`await self._r.aclose()`), not
the sync `close()`.

## When to pick / tradeoffs

Pick `RedisStore` when you want a fast, in-memory-ish durable store that is **all-network** — no
local file, no shared volume — so workers on different machines or containers all reach the same
state. It pairs cleanly with `RedisTransport` for a single-backend, pure-Redis deployment (store +
queue on one Redis; see [transports](../transports) and [distribution](../distribution)).

Tradeoffs:

- **List/monitor ordering is best-effort.** `SCAN` has no global order and the status/definition
  filters run client-side over the JSON blobs, so `list_executions` pages are unordered and may come
  back short — fine for a monitor that keeps paging, not for an ordered query API.
- **Durability is Redis durability** — as durable as your Redis persistence config (AOF/RDB).
- The version CAS and the per-event atomic commit give the same single-writer / crash-safe outbox
  guarantees as the other durable backends; see [durability](../durability) for the contract.

See the [stores hub](../stores) for the full backend comparison.
