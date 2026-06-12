# DynamoDBStore — AWS serverless

`DynamoDBStore` is a durable `ExecutionStore`
([`store/dynamodb.py`](../../../src/harel/engine/store/dynamodb.py)) that keeps every Execution,
its transactional outbox, its orthogonal-fork spawn intents, its dedupe marks, its durable timers
and its opt-in execution trace **in AWS DynamoDB**. It is the **serverless** sibling of the SQL
backends — no instance to provision, no file, no shared volume — and the natural store-side partner
of `SqsTransport` for an **all-AWS, no-server** stack: one cloud for both the persistence seam and
the queue seam (see [distribution](../distribution)).

It runs unchanged against **real DynamoDB**, against **LocalStack**, or against **moto** (no AWS
account needed for tests). The boto3 client is **injected** (duck-typed), so `boto3` stays an
optional extra: the engine never imports it at module load, the constructor only grabs
`TypeSerializer`/`TypeDeserializer` and `ClientError` lazily, and tests pass a moto-backed client.
A convenience `create(...)` classmethod does the lazy `import boto3` for you (LocalStack-friendly:
dummy creds + an injected `endpoint_url`; pass `endpoint_url=None` for real AWS), retrying until the
endpoint is reachable.

```text
DynamoDBStore(client, prefix="harel")                  # inject any boto3 dynamodb client (incl. moto)
DynamoDBStore.create(endpoint_url="http://localhost:4566")  # LocalStack: builds the client + ensures tables
DynamoDBStore.create()                                 # real AWS (endpoint_url=None)
```

## Why DynamoDB fits the store seam

DynamoDB hands the engine the two primitives the durability model needs, directly — no hand-rolled
locking, no WATCH/MULTI loop:

- **Conditional writes are the CAS.** A `Put` with a `ConditionExpression` applies only if the
  condition holds against the stored item. That is exactly the optimistic-concurrency check
  (`Execution.version`): insert iff the item is absent, or update iff the stored version still
  matches what we loaded.
- **`TransactWriteItems` makes the whole `commit` atomic across items.** The Execution write plus
  the outbox, spawns, processed (dedupe) and timer writes either *all* apply or *none* do, in one
  transaction. So a stale write never leaks its outbox, and an orthogonal fork's children + the
  parent's join expectations land together. The only constraint is DynamoDB's transaction cap of
  **100 items / 4MB per request** — far above a normal commit (1 Execution + a handful of
  emits / spawns / timers).

## Tables

`_ensure_tables` creates seven tables (idempotent — a pre-existing table is fine), each prefixed by
`prefix` via `_t(name)` → `"{prefix}_{name}"`, all `PAY_PER_REQUEST`:

```text
table         key schema                          contents
-----------   ---------------------------------   ----------------------------------------------------
executions    id (S, HASH)                        the Execution as JSON in `data` + `version`,
                                                   `definition_id`
outbox        seq (N, HASH)                        deferred events: {seq, target_id, event_json}
spawns        seq (N, HASH)                        pending orthogonal-fork children:
                                                   {seq, parent_id, child_id, root_path, context_json}
timers        execution_id (S, HASH), path (S, R)  durable timers: {execution_id, path, fire_at}
processed     execution_id (S, HASH), event_id (S, R)   dedupe marks (one item per handled event)
counters      id (S, HASH)                         the monotonic seq allocator (one item per counter)
trace         execution_id (S, HASH), idx (N, R)   opt-in timeline ring: {execution_id, idx, entry_json}
```

The spec list and the `["HASH", "RANGE"]` role assignment build each `KeySchema`/
`AttributeDefinitions` pair:

```text
specs = [
    ("executions", [("id", "S")]),
    ("outbox", [("seq", "N")]),
    ("spawns", [("seq", "N")]),
    ("timers", [("execution_id", "S"), ("path", "S")]),
    ("processed", [("execution_id", "S"), ("event_id", "S")]),
    ("counters", [("id", "S")]),
    ("trace", [("execution_id", "S"), ("idx", "N")]),
]
roles = ["HASH", "RANGE"]
```

Each table's key schema reflects how the store reads it back:

- **`executions`** — `id` HASH only. A load / save / CAS is a single-item operation keyed by the
  Execution id. The `version`, `status`, `parent_id` etc. live *inside* the `data` JSON; only
  `version` and `definition_id` are also broken out as top-level attributes (the CAS reads `version`;
  `definition_id` can be a server-side filter).
- **`outbox` / `spawns`** — `seq` (N) HASH only. Each deferred event / fork intent is its own item
  keyed by a globally monotonic integer `seq`, so a delivered entry is acked with a single
  `delete_item` by seq and the relay can sort the backlog by seq.
- **`timers`** — `(execution_id HASH, path RANGE)`. Re-arming the same `(execution_id, path)` replaces
  the item (a `Put` upsert); `delete_timer` cancels by the same composite key.
- **`processed`** — `(execution_id HASH, event_id RANGE)`. One item per handled event; a dedupe check
  is a single `get_item`, and all events for one execution share a partition.
- **`counters`** — `id` HASH. One item per counter name (`"outbox"`, `"spawn"`), holding the running
  total `n`. The monotonic seq allocator (see `_next_seq`).
- **`trace`** — `(execution_id HASH, idx RANGE)`. A per-execution contiguous index lets a `Query`
  read the steps in order, and the ring drop a single item by exact key (see the ring trick below).

`ResourceInUseException` (table already exists) is swallowed; any other `ClientError` re-raises:

```text
except self._ClientError as exc:
    if exc.response["Error"]["Code"] != "ResourceInUseException":
        raise  # already exists is fine; anything else is real
```

Creating tables is a convenience for LocalStack / moto / dev; on real AWS the tables usually
pre-exist (managed by IaC) and the create simply no-ops.

### Native ↔ typed attribute-value form (`_raw` / `_item`)

The DynamoDB wire format is the *typed* attribute-value form (`{"S": "..."}`, `{"N": "..."}`), not
plain dicts. The store uses boto3's pure (no-IO) serializers to convert both ways, so the rest of the
code works in native dicts:

```text
def _raw(self, item: dict) -> dict:
    """A native dict → DynamoDB's typed attribute-value form."""
    return {k: self._ser.serialize(v) for k, v in item.items()}

def _item(self, raw: dict) -> dict:
    """DynamoDB's typed form → a native dict (numbers come back as Decimal)."""
    return {k: self._deser.deserialize(v) for k, v in raw.items()}
```

Numbers round-trip through DynamoDB as `Decimal` — which is why `fire_at` is stored as
`Decimal(str(...))` and read back via `float(...)`, and `idx` / `version` via `int(...)`.

### The monotonic seq allocator (`_next_seq`)

Outbox and spawn entries need globally ordered ids so the relay drains them oldest-first. `_next_seq`
reserves a contiguous block of `count` ids from a counter item with a single atomic `ADD`, returning
the first id of the block:

```text
resp = self._db.update_item(
    TableName=self._t("counters"),
    Key=self._raw({"id": name}),
    UpdateExpression="ADD n :k",
    ExpressionAttributeValues={":k": {"N": str(count)}},
    ReturnValues="UPDATED_NEW",
)
return int(resp["Attributes"]["n"]["N"]) - count + 1
```

`ADD n :count` increments (creating the attribute at 0 if absent) and `ReturnValues="UPDATED_NEW"`
returns the post-increment total; subtracting `count` and adding 1 yields the first id of the block.
This runs **before** the transaction (it is a separate write — you cannot read your own counter inside
the same `TransactWriteItems`). A block wasted by a later-cancelled transaction is harmless: seqs are
only required to be monotonic, not gapless.

## The CAS via conditional write + `commit` (TransactWriteItems)

`commit` is the heart of the backend. It allocates seqs, bumps the version, and assembles one
`TransactWriteItems` list whose first item is the Execution `Put` carrying the CAS condition.

First, seqs are allocated up front and the outbox / spawn items built:

```text
if emits:
    base = self._next_seq("outbox", len(emits))
    outbox = [{"seq": base + i, "target_id": t, "event": e.model_dump_json()} for i, (t, e) in enumerate(emits)]
if spawns:
    base = self._next_seq("spawn", len(spawns))
    spawn = [{"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp,
              "context": json.dumps(ctx)} for i, (cid, rp, ctx) in enumerate(spawns)]
```

Then the version is bumped and the Execution item built (the whole Execution as JSON in `data`, plus
the broken-out `version` and `definition_id`):

```text
old = exe.version
exe.version = old + 1
exe_item = {"id": exe.id, "data": exe.model_dump_json(), "version": exe.version,
            "definition_id": exe.definition_id}
```

The CAS condition depends on whether this is an insert or an update:

```text
if old == 0:
    cas = {"ConditionExpression": "attribute_not_exists(id)"}
else:
    cas = {"ConditionExpression": "version = :ov",
           "ExpressionAttributeValues": {":ov": {"N": str(old)}}}
```

- **Insert** (`old == 0`, a never-saved Execution): `attribute_not_exists(id)` — apply only if no
  item with this id exists yet. Two racing creates: one wins, the other's condition fails.
- **Update** (`old > 0`): `version = :ov` — apply only if the stored item's `version` still equals the
  version we loaded. Another writer that already advanced the row makes the condition false.

The transaction list starts with the CAS-conditioned Execution `Put`, then appends the outbox Puts,
the spawn Puts, the processed (dedupe) Put, and each timer Put (schedule) or Delete (cancel):

```text
txn = [{"Put": {"TableName": self._t("executions"), "Item": self._raw(exe_item), **cas}}]
for o in outbox:
    txn.append({"Put": {"TableName": self._t("outbox"), "Item": self._raw(o)}})
for s in spawn:
    txn.append({"Put": {"TableName": self._t("spawns"), "Item": self._raw(s)}})
if processed_event_id is not None:
    txn.append({"Put": {"TableName": self._t("processed"),
                        "Item": self._raw({"execution_id": exe.id, "event_id": processed_event_id})}})
for op in timers:
    if op.action == "schedule":
        txn.append({"Put": {"TableName": self._t("timers"),
                            "Item": self._raw({"execution_id": exe.id, "path": op.path,
                                               "fire_at": Decimal(str(op.fire_at))})}})
    else:
        txn.append({"Delete": {"TableName": self._t("timers"),
                               "Key": self._raw({"execution_id": exe.id, "path": op.path})}})
```

(A `trace` step, if supplied, appends two more items — see the ring section below.)

Finally the transaction is issued, and a CAS miss is translated into a `StoreConflict`:

```text
try:
    self._db.transact_write_items(TransactItems=txn)
except self._ClientError as exc:
    code = exc.response["Error"]["Code"]
    if code not in ("TransactionCanceledException", "ConditionalCheckFailedException"):
        raise  # a real error, not a CAS miss
    exe.version = old  # undo the in-memory bump; the txn was cancelled
    resp = self._db.get_item(TableName=self._t("executions"), Key=self._raw({"id": exe.id}),
                             ProjectionExpression="version")
    found = int(self._item(resp["Item"])["version"]) if "Item" in resp else None
    raise StoreConflict(exe.id, expected=old, found=found)
```

Two error codes are treated as a conflict: DynamoDB raises **`TransactionCanceledException`** when
any item's condition fails inside a transaction; **older moto** raises the bare
**`ConditionalCheckFailedException`** instead, so both are caught. Anything else (throttling, a real
fault) re-raises untouched. On a conflict the in-memory version bump is undone, the *currently
stored* version is read back, and `StoreConflict(exe.id, expected=old, found=found)` is raised so the
caller can reload-and-retry or drop the stale work.

```{important}
**A failed condition cancels the WHOLE transaction.** Because the CAS lives on the first item of the
`TransactWriteItems`, a lost CAS means *nothing* in the transaction is written — not the Execution,
not its outbox, not its spawns, not its dedupe mark, not its timers. This is what guarantees a stale
write never leaks a half-committed `Finished` event or a phantom fork: the outbox and the Execution
advance are atomically all-or-nothing with the version check.
```

`save(exe)` is just `commit(exe, [])` — the version-checked write with no side effects.

## The trace ring — the DynamoDB-specific trick

The opt-in execution trace (off by default) appends one timeline step per commit and keeps only the
last `trace_max` steps (a ring; `DEFAULT_TRACE_MAX = 200`). On the SQL backends a ring is a
`LTRIM`-style range delete or a `$slice`; **DynamoDB has no LTRIM, no `$slice`, and no range-delete
inside an atomic write.** A `Delete` can only target one exact key. So the ring is expressed as
**`Put idx=K` + `Delete idx=K-N` in the SAME `TransactWriteItems`** — append the new step and drop the
one that just fell out of the last-N window, atomically:

```text
if trace is not None:
    idx = self._max_trace_idx(exe.id) + 1  # contiguous per execution (read max, +1)
    txn.append({"Put": {"TableName": self._t("trace"),
                        "Item": self._raw({"execution_id": exe.id, "idx": idx, "entry": json.dumps(trace)})}})
    if self.trace_max and idx - self.trace_max >= 0:
        txn.append({"Delete": {"TableName": self._t("trace"),
                               "Key": self._raw({"execution_id": exe.id, "idx": idx - self.trace_max})}})
```

Deleting an **absent** key is a no-op in DynamoDB, so the `Delete` is safe even before the window
fills (when `idx < trace_max` the guard skips it anyway).

The index is **contiguous per execution** because it is derived by reading the current max and adding
one, not from a counter. `_max_trace_idx` is a `Query` on the partition key, newest first, limited to
one row:

```text
resp = self._db.query(
    TableName=self._t("trace"),
    KeyConditionExpression="execution_id = :e",
    ExpressionAttributeValues={":e": {"S": execution_id}},
    ProjectionExpression="idx",
    ScanIndexForward=False,
    Limit=1,
)
items = resp.get("Items")
return int(self._item(items[0])["idx"]) if items else -1
```

`ScanIndexForward=False` returns the highest `idx` first; `Limit=1` takes just it (or `-1` if the
execution has no trace yet). Because the engine is **single-writer-per-execution** (the transport's
single-active-consumer-per-group guarantee), the read-max → write-max+1 is race-free. Crucially, since
the index comes from the *stored data* and not a separate counter, a **cancelled commit leaves no
gap**: nothing was incremented, so the next successful commit reuses the same `idx`.

```{caution}
**The ring keeps exactly the last N only if `trace_max` is fixed from the first traced commit** — the
production / startup case (`trace_max` is set once at process start). Because exactly *one* item is
dropped per write (`idx - trace_max`), changing `trace_max` mid-stream does not retroactively trim:
*lowering* it over-retains harmlessly (extra old items linger until they age out past the new window),
and the count converges as new commits arrive. It never over-deletes. Set `trace_max` once at startup
and the ring holds exactly N.
```

`read_trace` reads the ring in order (a `Query` ascending) and merges the stored `idx` back into each
entry as its `index`:

```text
resp = self._db.query(
    TableName=self._t("trace"),
    KeyConditionExpression="execution_id = :e",
    ExpressionAttributeValues={":e": {"S": execution_id}},
    ScanIndexForward=True,  # oldest → newest
)
out = []
for raw in resp.get("Items", []):
    it = self._item(raw)
    out.append({**json.loads(it["entry"]), "index": int(it["idx"])})
return out
```

`append_trace` is the standalone (non-transactional) appender used outside a commit — a `put_item`
plus a guarded ring drop, with the same `idx - trace_max` window logic:

```text
idx = entry.get("index", self._max_trace_idx(execution_id) + 1)
self._db.put_item(TableName=self._t("trace"),
                  Item=self._raw({"execution_id": execution_id, "idx": idx, "entry": json.dumps(entry)}))
if self.trace_max and idx - self.trace_max >= 0:
    self._db.delete_item(TableName=self._t("trace"),
                         Key=self._raw({"execution_id": execution_id, "idx": idx - self.trace_max}))
```

## Reads & sweeps

**`load`** — a single `get_item` projecting only the `data` attribute (the `data` keyword is reserved
in DynamoDB, hence the `#d` expression-attribute-name alias), parsed back to an `Execution`:

```text
resp = self._db.get_item(
    TableName=self._t("executions"),
    Key=self._raw({"id": execution_id}),
    ProjectionExpression="#d",
    ExpressionAttributeNames={"#d": "data"},
)
item = resp.get("Item")
return Execution.model_validate_json(self._item(item)["data"]) if item else None
```

**`load_for_event`** (async store) — loads the Execution **and** checks the dedupe mark in one
round-trip with `BatchGetItem` across the `executions` and `processed` tables:

```text
resp = await self._db.batch_get_item(RequestItems={
    self._t("executions"): {"Keys": [self._raw({"id": execution_id})],
                            "ProjectionExpression": "#d",
                            "ExpressionAttributeNames": {"#d": "data"}},
    self._t("processed"): {"Keys": [self._raw({"execution_id": execution_id, "event_id": event_id})]},
})
...
return Execution.model_validate_json(self._item(exe_items[0])["data"]), bool(proc_items)
```

Returns `(execution, already_processed)` — the worker's hot path collapses load + dedupe to a single
DynamoDB call.

**`list_executions`** — a `Scan` (DynamoDB Scan is **unordered**) projecting only `data` + `version`.
`status` / `roots_only` live inside the `data` JSON, so they are filtered **client-side** via the
shared `_matches`; `definition_id` is a broken-out attribute and can be a server-side
`FilterExpression`. The cursor is the base64 of DynamoDB's native `LastEvaluatedKey`:

```text
kwargs = {"TableName": self._t("executions"),
          "ProjectionExpression": "#dat,#v",
          "ExpressionAttributeNames": {"#dat": "data", "#v": "version"},
          "Limit": limit}
if definition_id is not None:
    kwargs["ExpressionAttributeNames"]["#def"] = "definition_id"
    kwargs["FilterExpression"] = "#def = :def"
    kwargs["ExpressionAttributeValues"] = {":def": {"S": definition_id}}
if cursor:
    kwargs["ExclusiveStartKey"] = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
```

`Limit` bounds the items **examined** before the filter, so a page may return **fewer than `limit`**
matches — the caller keeps paging while `next_cursor` is set. `next_cursor` is the base64-encoded
`LastEvaluatedKey` (or `None` when the scan is exhausted).

**`is_processed`** — a single `get_item` on the `processed` table; presence of `Item` is the answer:

```text
resp = self._db.get_item(TableName=self._t("processed"),
                         Key=self._raw({"execution_id": execution_id, "event_id": event_id}))
return "Item" in resp
```

**`pending_outbox` / `ack_outbox`** — the relay drains the outbox: `Scan` (unordered) then sort by
`seq` client-side; an ack is a `delete_item` by `seq`:

```text
rows = self._scan("outbox")
rows.sort(key=lambda r: int(r["seq"]))  # Scan is unordered; sort by seq
return [OutboxEntry(int(r["seq"]), r.get("target_id"), Event.model_validate_json(r["event"])) for r in rows]
```

```text
def ack_outbox(self, seq: int) -> None:
    self._db.delete_item(TableName=self._t("outbox"), Key=self._raw({"seq": seq}))
```

**`pending_spawns` / `ack_spawn`** — identical shape over the `spawns` table, rebuilding a `SpawnEntry`
per row and deleting by `seq` on ack.

**`due_timers`** — a `Scan` with a server-side `FilterExpression` for the due window, then sorted by
`fire_at`:

```text
rows = self._scan("timers",
                  FilterExpression="fire_at <= :now",
                  ExpressionAttributeValues={":now": {"N": str(now)}})
out = [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]
return sorted(out, key=lambda t: t[2])
```

**`delete_timer`** — a `delete_item` **guarded** on the stored `fire_at`, so a concurrent re-schedule
to a new time survives a stale sweep (the guard fails → no-op):

```text
self._db.delete_item(
    TableName=self._t("timers"),
    Key=self._raw({"execution_id": execution_id, "path": path}),
    ConditionExpression="fire_at = :f",
    ExpressionAttributeValues={":f": {"N": str(Decimal(str(fire_at)))}},
)
# on ConditionalCheckFailedException: the guard didn't match (stale sweep) — a no-op, as intended
```

```{note}
**Why the relay/sweep use `Scan` and not a `Query`.** The `outbox`, `spawns` and `timers` tables are
*work queues*: entries are written by `commit` and deleted as soon as the relay delivers / the timer
fires, so these tables **drain and stay near-empty** in steady state. A full-table `Scan` over a
near-empty table is cheap, and it needs no secondary index or partition-key probe — the relay wants
*everything* pending, not the rows for one key. `_scan` follows `LastEvaluatedKey` to drain every page
(a single Scan returns at most 1MB). The trade is that these reads are not point-lookups; that is the
deliberately accepted cost of the document model here.
```

## Async twin

[`aio_store/dynamodb.py`](../../../src/harel/engine/aio_store/dynamodb.py) is `AsyncDynamoDBStore` —
a **native-async** mirror over **aioboto3 / aiobotocore**, not a thread-pool wrapper. Every call is
awaited on **one long-lived aiohttp-backed client**, so concurrent workers (`STM_CONCURRENCY`) issue
**real parallel DynamoDB requests** through the aiohttp connection pool rather than being bounded by a
thread pool. The semantics are identical to the sync store — the same CAS conditions
(`attribute_not_exists(id)` to insert, `version = :ov` to update), the same `TransactWriteItems`
atomicity, the same trace ring. The boto3 `TypeSerializer` / `TypeDeserializer` are pure (no IO), so
they are reused as-is, and `trace_max` is set in `__init__`.

Build it with `await AsyncDynamoDBStore.create(...)`, which owns the client through an
`AsyncExitStack` and releases it in `close()`:

```text
stack = AsyncExitStack()
client = await stack.enter_async_context(aioboto3.Session().client("dynamodb", **kwargs))
inst = cls(client, prefix)
inst._stack = stack
...
await inst._ensure_tables()
```

```text
async def close(self) -> None:
    # release only a client we own (created via create()); an injected client is the caller's to close
    if self._stack is not None:
        await self._stack.aclose()
        self._stack = None
```

You may also inject an already-entered aiobotocore client through the constructor, in which case the
caller owns its lifecycle (`close()` then no-ops because `_stack is None`). The client binds to the
event loop that creates it — build it on the loop you run on (e.g. inside `anyio.run`); never share
one client across loops.

```{note}
Tests mock `AsyncDynamoDBStore` in-process with **`aiomoto`** — plain `moto.mock_aws` **cannot**
intercept aiobotocore's aiohttp transport, so the async twin needs the aiohttp-aware mock.
```

## When to pick / tradeoffs

Choose `DynamoDBStore` when you want a **fully serverless, all-AWS** statechart deployment with
nothing to run or patch: DynamoDB is the store, **`SqsTransport`** is the queue (its per-group
exclusivity is native via `MessageGroupId`), and your workers run on Lambda or ECS/Fargate. It pairs
naturally with `SqsTransport` for a no-server stack and develops locally against LocalStack / moto
with no account.

Tradeoffs to weigh:

- **Scan-based relay reads.** `pending_outbox` / `pending_spawns` / `due_timers` `Scan` their tables
  and sort client-side. This is cheap because those tables drain and stay near-empty, but it is not a
  point-lookup pattern — a backlog that grows large (a stalled relay) makes these scans more
  expensive.
- **`list_executions` is unordered** (Scan) and filters `status` / `roots_only` client-side, so a
  page can return fewer than `limit` matches — page until `next_cursor` is `None`.
- **The trace ring's fixed-`trace_max` caveat** (above): set it once at startup; changing it
  mid-stream over-retains harmlessly rather than trimming immediately.
- **The 100-item / 4MB transaction cap** bounds a single `commit` — far above a normal commit, but a
  pathological fan-out emitting dozens of events plus dozens of spawns in one step could approach it.

See the [stores hub](../stores) for the full backend matrix, and [durability](../durability) for the
CAS / outbox / dedupe model these tables implement.
