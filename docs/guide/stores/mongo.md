# MongoStore — document store

`MongoStore` is a durable `ExecutionStore` ([`store/mongo.py`](../../../src/harel/engine/store/mongo.py))
that keeps every Execution, its transactional outbox, its pending orthogonal-fork spawns, its dedupe
marks, its durable timers and its optional execution trace **in MongoDB** — over the network, with no
shared filesystem. Like `RedisStore` it is **all-network**: there is no local file and no shared
volume, so the same store works across machines and containers as long as they can all reach the same
Mongo. It is the natural partner of `MongoTransport` for a **pure-Mongo stack** (one backend for both
the persistence seam and the queue seam — see [distribution](../distribution)).

The Mongo client is **injected** (duck-typed), so `pymongo` stays an optional dependency: the engine
never imports it at module load, the constructor only grabs `ReturnDocument`/`DuplicateKeyError`
lazily, and the test suite passes a `mongomock` client instead of a real server. A convenience
`from_url` classmethod does the lazy `import pymongo` for you, pinging the server and retrying so a
worker starting alongside Mongo (compose) waits for it to accept connections rather than crashing:

```text
MongoStore(client, db_name="harel")              # inject any pymongo-compatible client (incl. mongomock)
MongoStore.from_url("mongodb://host:27017")       # lazily imports pymongo, builds MongoClient, pings + retries
```

Collections live under `db_name` (default `"harel"`): `executions` (the documents) and `counters`
(the monotonic outbox/spawn/trace seq allocator).

## The key design point: one document, one atomic update

MongoDB has **no multi-document transaction without a replica set**. The SQL backends lean on a real
multi-table transaction to commit the Execution snapshot, the outbox rows, the spawn rows, the timer
rows and the dedupe mark together. Mongo cannot assume that machinery is available. So instead of
spreading one Execution's state across separate collections, **everything for one Execution lives in
its single document** — the serialized Execution (`data`), plus the `outbox`/`spawns`/`trace` arrays,
the `timers` sub-document and the `processed` array.

The payoff: a `commit` is therefore **one `update_one`**, and an update of a single document is
**atomic on its own with no replica set required**. That single-document atomicity is the whole reason
for embedding everything — it buys the all-or-nothing commit contract (Execution advance + outbox +
spawns + dedupe + timers, all together or none) on a bare standalone `mongod`.

**Performance note — never round-trip the whole document.** Embedding everything would be ruinous if
every write re-sent the whole (growing) blob. It does not:

- **Writes are partial.** `commit` uses `$set` (the `data` JSON and `version`, plus timer schedule
  keys), `$push` (append to the `outbox`/`spawns`/`trace` arrays), `$addToSet` (the dedupe mark) and
  `$unset` (timer cancels) — never a full `replace_one`.
- **Reads are projected.** `load` pulls only `data`; the relay/sweep reads
  (`pending_outbox`/`pending_spawns`/`due_timers`) project only the relevant array/sub-document and
  never `data`. So a growing `data` blob is not dragged through every queue or timer scan.

## Document model

An `executions` document has this exact shape:

```text
{
  _id:            "<execution id>",          // the Execution id (Mongo primary key)
  definition_id:  "<definition id>",         // top-level so list_executions can filter it server-side
  version:        7,                          // the optimistic-concurrency counter (the CAS guard)
  data:           "<Execution JSON>",         // the full serialized Execution (model_dump_json)
  outbox: [                                   // transactional outbox: deferred events awaiting delivery
    { seq: 42, target_id: "<id|null>", event: "<Event JSON>" },
    ...
  ],
  spawns: [                                   // pending orthogonal-fork child creations
    { seq: 17, parent_id: "<id>", child_id: "<id>", root_path: "Fork.A", context: { ... } },
    ...
  ],
  processed: [ "<event id>", "<event id>", ... ],   // dedupe set: event ids already handled
  timers: { "<enc(path)>": <fire_at>, ... },        // durable timers, keyed by encoded node path
  trace:  [ { ...step, index: 5 }, ... ]            // optional execution-trace ring (last trace_max)
}
```

Every field, and why it is **embedded** rather than living in its own collection:

- **`_id`** — the Execution id, so a load/CAS is a primary-key lookup.
- **`definition_id`** — promoted to a top-level field (not buried in `data`) precisely so
  `list_executions` can filter by it **server-side**; `status`/`parent_id` are not promoted, so those
  filters run client-side (see Reads).
- **`version`** — the optimistic-concurrency counter. The CAS filter `{"_id": id, "version": old}` is
  what makes the single-writer guarantee hold; on a winning commit it advances to `old + 1`. It is
  also duplicated *inside* `data` (the serialized Execution carries its own `version`).
- **`data`** — the whole Execution serialized with `model_dump_json()`. This is the snapshot `load`
  returns; everything else in the document is bookkeeping the engine drives separately.
- **`outbox`** — an array of `{seq, target_id, event}`. Embedded so the event enqueue commits in the
  **same single-document update** as the Execution advance (the transactional-outbox guarantee without
  a multi-document transaction). `seq` is monotonic (for ordered delivery + ack); `target_id` is the
  Execution to deliver to (`null` = no target); `event` is the Event JSON.
- **`spawns`** — an array of `{seq, parent_id, child_id, root_path, context}`. A pending
  child-Execution creation (an orthogonal fork), embedded for the same reason: the fork intent commits
  atomically with the parent's advance and its join expectations (the `children` dict inside `data`),
  so a crash mid-fork cannot lose or double the spawn. A relay creates the child afterwards,
  idempotently.
- **`processed`** — an array of event ids the Execution has already handled (the dedupe set under
  at-least-once delivery). Embedded so recording "handled" lands in the same atomic update as the work
  it guards.
- **`timers`** — a **sub-document** mapping an encoded node path to a `fire_at` epoch time. Embedded so
  arming/cancelling a durable timer commits atomically with the transition that armed it (no
  dual-write; a scheduled timer cannot be lost). Keyed by path so re-arming the same node replaces.
- **`trace`** — an optional array of trace steps (opt-in execution timeline), ring-trimmed to the last
  `trace_max` entries; each step carries a stamped 0-based `index`.

### The `counters` collection and `_next_seq`

The monotonic seqs (`outbox`, `spawn`, and a per-execution `trace:<id>`) come from a separate
`counters` collection, allocated atomically with `find_one_and_update` + `$inc` returning the
post-increment value:

```text
def _next_seq(self, name: str, count: int) -> int:
    doc = self._counters.find_one_and_update(
        {"_id": name}, {"$inc": {"n": count}}, upsert=True, return_document=self._after
    )
    return int(doc["n"]) - count + 1
```

It reserves a block of `count` ids and returns the **first** of the block (`n - count + 1`).
`return_document=ReturnDocument.AFTER` makes `find_one_and_update` return the document *after* the
increment, so `n` is the new high-water mark. A block burned by a commit that later loses the CAS is
**harmless** — seqs only need to be monotonic and unique, never gapless, so a gap is fine.

### Path encoding: `_enc` / `_dec`

Node paths use `.` as the separator (e.g. `Fork.A`). But MongoDB treats a `.` **inside a field name**
as a path operator (it would mean "sub-field `A` of field `Fork`"), so a raw node path cannot be used
as a key in the `timers` sub-document. The store encodes the dot to a character that cannot appear in
a path, reversibly:

```text
@staticmethod
def _enc(path: str) -> str:
    return path.replace(".", "．")

@staticmethod
def _dec(key: str) -> str:
    return key.replace("．", ".")
```

The replacement char is `．` (U+FF0C-class fullwidth full stop, not an ASCII `.`). So `timers.Fork.A`
becomes the single sub-key `timers.Fork．A` and `due_timers` decodes it back when it reads the timers
out.

## The CAS commit (one `update_one`)

`commit` is the one atomic write per event: it persists the Execution, enqueues its emitted events into
the outbox, records the processed event id, applies the timer ops, enqueues the spawn intents, and
appends the optional trace step — **all or nothing** — with an optimistic-concurrency check on the
Execution's `version`. On a single-document Mongo this is one `update_one`, no replica set needed.

**Step 1 — allocate the seqs up front.** The outbox/spawn seqs and the trace index come from the
`counters` collection (one `find_one_and_update` each), allocated before building the update:

```text
outbox_entries: list[dict] = []
if emits:
    base = self._next_seq("outbox", len(emits))
    outbox_entries = [
        {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
        for i, (t, e) in enumerate(emits)
    ]
spawn_entries: list[dict] = []
if spawns:
    base = self._next_seq("spawn", len(spawns))
    spawn_entries = [
        {"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp, "context": dict(ctx)}
        for i, (cid, rp, ctx) in enumerate(spawns)
    ]
trace_step: Optional[dict] = None
if trace is not None:
    trace_step = {**trace, "index": self._next_seq("trace:" + exe.id, 1) - 1}
```

A seq wasted by a later-aborted commit is harmless (gaps are fine), so allocating eagerly before the
CAS is safe.

**Step 2 — bump the version and serialize.** `old` is the version the caller loaded at; the in-memory
Execution is bumped to `old + 1` and serialized into `data`:

```text
old = exe.version
exe.version = old + 1
data = exe.model_dump_json()
```

**Step 3 — build the partial update.** `$set` carries `data`, the new `version`, and each timer
*schedule* (key `timers.{enc(path)}`); `$unset` carries each timer *cancel*; `$push` appends to the
arrays (`outbox`/`spawns` each `{$each: [...]}`, and `trace` with `{$each: [...], $slice: -trace_max}`
— see the ring below); `$addToSet` records the dedupe mark:

```text
set_ops: dict[str, Any] = {"data": data, "version": exe.version}
unset_ops: dict[str, str] = {}
for op in timers:
    key = f"timers.{self._enc(op.path)}"
    if op.action == "schedule":
        set_ops[key] = op.fire_at
    else:
        unset_ops[key] = ""
update: dict[str, Any] = {"$set": set_ops}
push: dict[str, Any] = {}
if outbox_entries:
    push["outbox"] = {"$each": outbox_entries}
if spawn_entries:
    push["spawns"] = {"$each": spawn_entries}
if trace_step is not None:
    push["trace"] = {"$each": [trace_step], **({"$slice": -self.trace_max} if self.trace_max else {})}
if push:
    update["$push"] = push
if processed_event_id is not None:
    update["$addToSet"] = {"processed": processed_event_id}
if unset_ops:
    update["$unset"] = unset_ops
```

Note `$addToSet` for `processed`: it makes the dedupe array set-like (a duplicate event id is a no-op,
not a second copy), which is exactly the at-least-once dedupe semantics.

**Step 4 — the CAS itself.** The update is applied with a filter on both `_id` **and the loaded
`version`**:

```text
res = self._exes.update_one({"_id": exe.id, "version": old}, update)
if res.matched_count == 1:
    return  # CAS won
```

If `matched_count == 1`, a document at exactly `version == old` existed and was updated — **this writer
won**, and because it is a single-document update it landed atomically. If `matched_count == 0`, no
document was at `version == old`: either the Execution is brand-new, or someone else moved the version
(a stale write). The store disambiguates:

```text
existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
if existing is None and old == 0:
    doc: dict[str, Any] = {
        "_id": exe.id,
        "definition_id": exe.definition_id,
        "version": exe.version,
        "data": data,
        "outbox": outbox_entries,
        "spawns": spawn_entries,
        "trace": [trace_step] if trace_step is not None else [],
        "processed": [processed_event_id] if processed_event_id is not None else [],
        "timers": {self._enc(op.path): op.fire_at for op in timers if op.action == "schedule"},
    }
    try:
        self._exes.insert_one(doc)
        return
    except self._DuplicateKeyError:
        existing = self._exes.find_one({"_id": exe.id}, {"version": 1})
exe.version = old  # undo the in-memory bump; the commit did not happen
raise StoreConflict(exe.id, expected=old, found=existing["version"] if existing else None)
```

So: **no document exists and `old == 0`** ⇒ this is a fresh Execution → `insert_one` the **full
document** (note it must seed every embedded field, including `trace: []`, so later `$push`/`$set`
operators have an array/sub-document to target). The insert is itself the atomic create. If two writers
race the insert, the loser hits `DuplicateKeyError` on the `_id` and falls through to the conflict
path. Any other case (a document exists but at a different version, or `old != 0` with no document) is a
**stale write**: the in-memory `version` bump is undone (`exe.version = old`, so the caller can reload
and retry) and `StoreConflict` is raised carrying the expected and found versions.

**Why one `update_one` is atomic without a replica set.** Mongo guarantees that a single-document
write — applying all of an update's `$set`/`$push`/`$addToSet`/`$unset` operators — is atomic on that
document. There is no partial state visible to a reader: either the whole update (Execution advance +
all the embedded outbox/spawn/timer/dedupe changes) is applied, or none of it is. Multi-document
transactions (which *do* need a replica set) are unnecessary here precisely because the store collapsed
everything for one Execution into one document.

## The trace ring

The execution trace (opt-in, off by default) is a bounded **ring**: only the last `trace_max` steps
(`DEFAULT_TRACE_MAX = 200`) are kept. The trimming is **native** — Mongo's `$push` with `$slice` trims
the array in the very same update that appends, so there is no separate read-modify-write:

```text
push["trace"] = {"$each": [trace_step], **({"$slice": -self.trace_max} if self.trace_max else {})}
```

`$slice: -trace_max` keeps only the last `trace_max` elements after the push, dropping the oldest as
the array grows — a fixed-size ring done server-side, atomically, alongside the rest of the commit. The
step's `index` is a per-execution monotonic counter stamped via `_next_seq("trace:" + id, 1) - 1`
(0-based), and because it lives *inside* the step document it stays meaningful even after the ring
evicts the entries before it.

`append_trace` is the standalone (non-`commit`) writer — same `$push`/`$slice` shape, upserting so the
document is created if absent, and honoring an `index` already on the entry if present:

```text
def append_trace(self, execution_id: str, entry: dict) -> None:
    idx = entry.get("index", self._next_seq("trace:" + execution_id, 1) - 1)
    step = {**entry, "index": idx}
    push = {"$each": [step], **({"$slice": -self.trace_max} if self.trace_max else {})}
    self._exes.update_one({"_id": execution_id}, {"$push": {"trace": push}}, upsert=True)
```

`read_trace` projects only the `trace` array:

```text
def read_trace(self, execution_id: str) -> list[dict]:
    doc = self._exes.find_one({"_id": execution_id}, {"trace": 1})
    return list(doc.get("trace", [])) if doc is not None else []
```

(The trace is fully recorded by `MongoStore`. The `commit` docstring across the family marks
execution-trace as recorded by the SQL-family and Dict backends; `MongoStore` implements the same
`$push`/`$slice` ring as Redis.)

## Reads & sweeps

**`load`** is one `find_one` projecting only `data`, parsed back through pydantic (or `None` if
absent) — the growing embedded arrays are never shipped:

```text
def load(self, execution_id: str) -> Optional[Execution]:
    doc = self._exes.find_one({"_id": execution_id}, {"data": 1})
    return Execution.model_validate_json(doc["data"]) if doc is not None else None
```

**`load_for_event`** (on the async twin) folds the load and the dedupe check into **one round-trip**
via an aggregation: `$match` the id, then `$project` `data` plus a server-side `$in` membership flag
`hit` over the `processed` array. The key point is that the (potentially large, ever-growing)
`processed` array is **never shipped to the client** — the membership test runs on the server and only
the boolean `hit` comes back:

```text
cursor = self._exes.aggregate(
    [
        {"$match": {"_id": execution_id}},
        {"$project": {"data": 1, "hit": {"$in": [event_id, {"$ifNull": ["$processed", []]}]}}},
    ]
)
docs = [d async for d in cursor]
if not docs:
    return None, False
return Execution.model_validate_json(docs[0]["data"]), bool(docs[0].get("hit"))
```

`$ifNull: ["$processed", []]` defends against a document that has no `processed` field yet.

**`list_executions`** can only filter `definition_id` server-side (it is a top-level field);
`status`/`parent_id` live inside the `data` JSON string, so they filter **client-side** with the
shared `_matches`. The find projects only `{version, data}` (never the embedded arrays), sorts by
`_id`, applies the cursor as a `skip` offset, and **over-fetches** so client-side filtering still
fills the page:

```text
query: dict = {} if definition_id is None else {"definition_id": definition_id}
off = _decode_offset(cursor)
cur = self._exes.find(query, {"version": 1, "data": 1}).sort("_id", 1).skip(off)
scanned = 0
for doc in cur:
    scanned += 1
    summary = ExecutionSummary.from_data(json.loads(doc["data"]), doc.get("version", 0))
    if _matches(summary, status, definition_id, roots_only):
        items.append(summary)
    if len(items) >= limit:
        break
nxt = _encode_offset(off + scanned) if len(items) >= limit else None
return ExecutionPage(items=items, next_cursor=nxt)
```

The next cursor is `off + scanned` (the over-fetch offset, not `off + limit`), so the next page resumes
after everything actually scanned. If the cursor exhausts before reaching `limit`, there simply is no
next page (`next_cursor = None`). Ordering by `_id` is stable. A page may return fewer than `limit`
matches (the status/roots filter rejected some), so callers keep paging while `next_cursor` is set.

**`is_processed`** is the dedupe membership test — a `find_one` querying the array element directly
(Mongo matches `processed: event_id` against any element of the array), projecting only `_id`:

```text
def is_processed(self, execution_id: str, event_id: str) -> bool:
    return self._exes.find_one({"_id": execution_id, "processed": event_id}, {"_id": 1}) is not None
```

**Outbox relay** — `pending_outbox` finds documents whose `outbox` exists and is non-empty, projecting
only the `outbox` array (never `data`), rebuilds each `OutboxEntry`, and sorts by seq so delivery is
oldest-first; `ack_outbox` `$pull`s the delivered entry by its `outbox.seq`:

```text
def pending_outbox(self) -> list[OutboxEntry]:
    entries: list[OutboxEntry] = []
    for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
        for e in doc.get("outbox", []):
            entries.append(OutboxEntry(e["seq"], e["target_id"], Event.model_validate_json(e["event"])))
    return sorted(entries, key=lambda e: e.seq)

def ack_outbox(self, seq: int) -> None:
    self._exes.update_one({"outbox.seq": seq}, {"$pull": {"outbox": {"seq": seq}}})
```

**Spawn relay** — identical shape over the `spawns` array; `pending_spawns` projects only `spawns` and
sorts by seq, `ack_spawn` `$pull`s by `spawns.seq`:

```text
def pending_spawns(self) -> list[SpawnEntry]:
    entries: list[SpawnEntry] = []
    for doc in self._exes.find({"spawns": {"$exists": True, "$ne": []}}, {"spawns": 1}):
        for s in doc.get("spawns", []):
            entries.append(
                SpawnEntry(s["seq"], s["parent_id"], s["child_id"], s["root_path"], dict(s["context"]))
            )
    return sorted(entries, key=lambda s: s.seq)

def ack_spawn(self, seq: int) -> None:
    self._exes.update_one({"spawns.seq": seq}, {"$pull": {"spawns": {"seq": seq}}})
```

**Timer sweep** — `due_timers` finds documents whose `timers` sub-document exists and is non-empty,
projecting only `timers`, then iterates the sub-document, **decoding** each key back to a real node
path with `_dec` and keeping any whose `fire_at <= now` (sorted by fire time):

```text
def due_timers(self, now: float) -> list[tuple[str, str, float]]:
    out: list[tuple[str, str, float]] = []
    for doc in self._exes.find({"timers": {"$exists": True, "$ne": {}}}, {"timers": 1}):
        for enc, fire_at in (doc.get("timers") or {}).items():
            if fire_at <= now:
                out.append((doc["_id"], self._dec(enc), float(fire_at)))
    return sorted(out, key=lambda t: t[2])
```

`delete_timer` is **guarded on the stored value**: it `$unset`s the timer key only if the document
still holds exactly the `fire_at` the caller knows about. This protects a concurrent re-schedule — if
the model re-armed the same `(execution_id, path)` to a *new* time, a stale sweep trying to delete the
*old* one finds a mismatched value, matches nothing, and leaves the new timer intact:

```text
def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
    key = f"timers.{self._enc(path)}"
    self._exes.update_one({"_id": execution_id, key: fire_at}, {"$unset": {key: ""}})
```

`close` calls `self._client.close()` on the injected client.

## Async twin

`AsyncMongoStore` ([`aio_store/mongo.py`](../../../src/harel/engine/aio_store/mongo.py)) is the
native-async mirror over `motor.motor_asyncio.AsyncIOMotorClient`: the same document model, the same
`counters`/`_next_seq` allocator, the same `_enc`/`_dec` path encoding, and the **same
single-document CAS** — `update_one({"_id": id, "version": old}, update)`, atomic without a replica
set because the whole Execution and its embedded outbox/spawns/timers/processed live in one document.

Every collection method is `await`ed and cursors are iterated with `async for`:

```text
res = await self._exes.update_one({"_id": exe.id, "version": old}, update)
...
async for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
    ...
```

`trace_max` is set in `__init__` (defaulting to `DEFAULT_TRACE_MAX`), exactly as in the sync store, so
the `$push`/`$slice` ring behaves identically. It adds `load_for_event` (the aggregation shown under
Reads). Build it with `await AsyncMongoStore.from_url(url)` (which `await`s the `ping` and `anyio.sleep`s
between retries) or inject an already-connected `AsyncIOMotorClient`.

## When to pick / tradeoffs

Pick `MongoStore` when you are a **document-store shop** — you already run MongoDB and want the engine's
durable state to live there too, all-network (no local file, no shared volume), reachable from workers
on any machine or container. It pairs cleanly with `MongoTransport` for a single-backend, pure-Mongo
deployment (store + queue on one Mongo; see [transports](../transports) and
[distribution](../distribution)).

The elegant part is the **single-document atomicity**: collapsing one Execution's snapshot, outbox,
spawns, timers and dedupe marks into one document means `commit` is one `update_one` and gets the
full all-or-nothing contract on a bare standalone `mongod` — no replica set, no multi-document
transaction. Partial `$set`/`$push`/`$addToSet`/`$unset` writes and projected reads keep that
embedding cheap even as the document grows.

Tradeoffs:

- **List/monitor filtering is partly client-side.** Only `definition_id` is a top-level field;
  `status`/`parent_id` live inside the `data` JSON, so `list_executions` filters them after the fetch
  and over-fetches to fill a page — fine for a monitor that keeps paging while `next_cursor` is set.
- **Durability is Mongo's durability** — as durable as your write concern / journaling config.
- The version CAS and the per-event single-document atomic commit give the same single-writer /
  crash-safe outbox guarantees as the other durable backends; see [durability](../durability) for the
  contract.

See the [stores hub](../stores) for the full backend comparison.
