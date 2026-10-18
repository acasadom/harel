"""Shared transport contract: the `Transport` Protocol, the `Lease` dataclass, and
the `_PARKED` sentinel. Concrete backends live in sibling modules; the package
`__init__` re-exports them so existing imports keep working."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Protocol, runtime_checkable

from harel.engine.schema import Names, sqlite_transport_upgrade, transport_schema
from harel.spec.states import Event

# sentinel `locked_by` for a message parked by `nack(delay>0)`: non-null (so the
# claim's "available"/in-flight checks skip it) until its `lock_expiry` passes.
_PARKED = "__parked__"

# The SQLite-dialect claim (SQLite and libSQL transports, sync and async), run inside
# BEGIN IMMEDIATE: the group that waited longest (`last_claimed_at`, 0 = never claimed; ties
# by arrival) with nothing in flight, and its head — its oldest message. It walks `groups` in
# that order and stops at the first eligible one, so its cost doesn't grow with the backlog.
# A group with nothing in flight has a free head: unlocked, or its lease or park ended.
# Params: (min_priority, *routing_keys, now) — `{routing}` is `_routing_filter`'s. A template
# (`schema.Names.sql`, then `_sqlite_claim`).
_SQLITE_CLAIM_SQL = (
    "SELECT m.seq, m.group_id, m.event FROM {groups} g "
    "JOIN {messages} m ON m.seq = (SELECT MIN(seq) FROM {messages} WHERE group_id = g.group_id) "
    "WHERE g.priority >= ?{routing} AND NOT EXISTS ("
    "  SELECT 1 FROM {messages} x WHERE x.group_id = g.group_id "
    "  AND x.locked_by IS NOT NULL AND x.lock_expiry >= ?"
    ") ORDER BY g.last_claimed_at, g.rowid LIMIT 1"
)

# The Redis `publish`, server-side and atomic (shared by the sync + async backends). Pushes
# the payload onto the group's FIFO, fixes the group's priority on the FIRST publish (HSETNX,
# clamped to 0..4), and readies it (score 0) in the ZSET of ITS priority tier (`ready:{prio}`)
# — one per priority level, so `claim` can find a high-priority group without scanning past a
# backlog of low-priority ones. NX on the ZADD: a publish into an in-flight/parked group must
# not reset its score. ARGV = prefix, group, payload, priority. Single Redis instance (keys
# computed from prefix — never was Cluster).
_PUBLISH_LUA = """
local prefix = ARGV[1]
local g = ARGV[2]
local payload = ARGV[3]
local priority = tonumber(ARGV[4])
local routing_key = ARGV[5]
local published_at = ARGV[6]
if priority < 0 then priority = 0 end
if priority > 4 then priority = 4 end
-- `qt:{G}` holds the publish times of the LAST messages of `q:{G}`, one each: as long as
-- `q:{G}`, except in a group with messages from before it existed, whose head has no time —
-- `ack` pops a time only when the head has one, so the two line up once those are gone
redis.call('RPUSH', prefix .. ':q:' .. g, payload)
redis.call('RPUSH', prefix .. ':qt:' .. g, published_at)
redis.call('HSETNX', prefix .. ':prio', g, priority)
if routing_key ~= '' then redis.call('HSETNX', prefix .. ':route', g, routing_key) end
local eff = tonumber(redis.call('HGET', prefix .. ':prio', g))
redis.call('ZADD', prefix .. ':ready:' .. eff, 'NX', 0, g)
return 1
"""

# The Redis `claim`, server-side and atomic (shared by the sync + async backends). There is one
# `ready:{prio}` ZSET per priority level (0..4), each scored by the epoch-ms at which its groups
# are next claimable (0 = now; round-robin re-scores to now_ms on ack). `claim(min_priority=m)`
# takes the OLDEST-serviced (lowest-scored) lockable due group across the tiers `t >= m`: it reads
# each tier's lowest-scored due window, and picks the globally lowest-scored lockable head. So a
# high-priority group is served even behind a large backlog of lower-priority ones (the previous
# single-ZSET design filtered priority *inside* a fixed candidate window, so a high-priority group
# outside the window was starved). m=0 spans all tiers → plain round-robin, every group equal.
# One round-trip, atomic → concurrent claimers each get a DISTINCT group with zero lost races.
# ARGV = prefix, now_ms, lease_px_ms, fencing_token, per_tier_candidate_limit, min_priority.
# The candidate window still bounds work WITHIN a tier, where all groups share a priority so
# truncating is pure round-robin. A stale empty group found in the window is dropped (ZREM).
_CLAIM_LUA = """
local prefix = ARGV[1]
local now = tonumber(ARGV[2])
local px = tonumber(ARGV[3])
local token = ARGV[4]
local limit = tonumber(ARGV[5])
local min_prio = tonumber(ARGV[6])
local best_g, best_payload, best_score, best_ready
for t = min_prio, 4 do
  local ready = prefix .. ':ready:' .. t
  local cands = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, limit)
  for i = 1, #cands do
    local g = cands[i]
    local lockkey = prefix .. ':lock:' .. g
    if redis.call('EXISTS', lockkey) == 0 then
      local payload = redis.call('LINDEX', prefix .. ':q:' .. g, 0)
      if payload then
        local score = tonumber(redis.call('ZSCORE', ready, g))
        if (best_g == nil) or (score < best_score) then
          best_g = g
          best_payload = payload
          best_score = score
          best_ready = ready
        end
        break  -- this tier's oldest lockable head; compare it across tiers
      else
        redis.call('ZREM', ready, g)
      end
    end
  end
end
if best_g then
  redis.call('SET', prefix .. ':lock:' .. best_g, token, 'PX', px)
  redis.call('ZADD', best_ready, now + px, best_g)
  return {best_g, best_payload}
end
return nil
"""

# The Redis `ack`, server-side and atomic (shared by the sync + async backends). Fences on the
# lock token (only the current holder mutates), pops the delivered head, then re-readies the
# group in ITS priority tier if more remain (score now_ms → back of the round-robin) or drops
# it (and its priority entry) if drained, and frees the lock — one round-trip. The tier is read
# from the priority hash (before it may be deleted). ARGV = prefix, group, token, now_ms.
_ACK_LUA = """
local prefix = ARGV[1]
local g = ARGV[2]
local token = ARGV[3]
local now_ms = tonumber(ARGV[4]) or 0
local lockkey = prefix .. ':lock:' .. g
if redis.call('GET', lockkey) ~= token then
  return 0
end
local qkey = prefix .. ':q:' .. g
local tkey = prefix .. ':qt:' .. g
if redis.call('LLEN', tkey) == redis.call('LLEN', qkey) then redis.call('LPOP', tkey) end
redis.call('LPOP', qkey)
local prio_key = prefix .. ':prio'
local eff = tonumber(redis.call('HGET', prio_key, g)) or 0
local ready = prefix .. ':ready:' .. eff
if redis.call('LLEN', qkey) == 0 then
  redis.call('ZREM', ready, g)
  -- clean up the group's entries so a recycled group_id gets its new ones on next publish
  redis.call('HDEL', prio_key, g)
  redis.call('HDEL', prefix .. ':route', g)
  redis.call('DEL', tkey)
else
  -- score = now_ms so this group goes to the back of its tier's queue (round-robin)
  redis.call('ZADD', ready, now_ms, g)
end
redis.call('DEL', lockkey)
return 1
"""


# `backlog` on the SQL transports: the queued messages, by their group's routing key. A group is
# held when its next message is leased or parked: in the SQLite dialect the lease is on the
# message (`locked_by`/`lock_expiry`), in Postgres on the group. Params: (now,).
_SQLITE_BACKLOG_SQL = (
    "SELECT g.routing_key, COUNT(m.seq), COUNT(DISTINCT m.group_id), "
    "COUNT(DISTINCT CASE WHEN m.locked_by IS NOT NULL AND m.lock_expiry >= ? THEN m.group_id END), "
    "MIN(m.published_at) "
    "FROM {messages} m JOIN {groups} g ON g.group_id = m.group_id GROUP BY g.routing_key"
)
_PG_BACKLOG_SQL = (
    "SELECT g.routing_key, COUNT(m.seq), COUNT(DISTINCT m.group_id), "
    "COUNT(DISTINCT CASE WHEN g.locked_by IS NOT NULL AND g.lock_expiry >= %s THEN g.group_id END), "
    "MIN(m.published_at) "
    "FROM {messages} m JOIN {groups} g ON g.group_id = m.group_id GROUP BY g.routing_key"
)


def _backlog(rows: Iterable[Any]) -> "Backlog":
    """A `Backlog` from rows of `(routing_key, messages, executions, held, oldest_published_at)`."""
    return Backlog(
        machines=sorted(
            (
                MachineBacklog(key, int(messages), int(executions), int(held), oldest)
                for key, messages, executions, held, oldest in rows
            ),
            key=lambda b: (b.routing_key is None, b.routing_key or ""),
        )
    )


def _mongo_backlog(per_group: Iterable[dict], locks: dict, now: float) -> "Backlog":
    """A `Backlog` from the Mongo transport's messages counted per group (`_id`, `n`,
    `oldest`) and its group locks (`routing_key`, `token`, `available_at` — a group is held
    while a token sits on it and its time hasn't come)."""
    rows: dict = {}
    for g in per_group:
        lock = locks.get(g["_id"], {})
        row = rows.setdefault(lock.get("routing_key"), [0, 0, 0, None])
        row[0] += g["n"]
        row[1] += 1
        if lock.get("token") is not None and lock.get("available_at", 0.0) > now:
            row[2] += 1
        if g.get("oldest") is not None and (row[3] is None or g["oldest"] < row[3]):
            row[3] = g["oldest"]
    return _backlog((key, *r) for key, r in rows.items())


# what `backlog` reads of an SQS queue: its approximate counts, waiting and in flight
_COUNTS = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]


def _sqs_backlog(attrs: dict) -> "Backlog":
    """The one-row `Backlog` of an SQS queue from its attributes (`_COUNTS`)."""
    waiting = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    total = waiting + in_flight
    return Backlog(machines=[MachineBacklog(None, total)] if total else [], by_machine=False)


def _routing_filter(routing_keys: Optional[Iterable[str]], column: str) -> tuple[str, tuple]:
    """The SQL condition (`AND ...`) and its params restricting a claim to `routing_keys`'
    groups — none when None."""
    if routing_keys is None:
        return "", ()
    keys = tuple(sorted(routing_keys))
    if not keys:
        return " AND 1 = 0", ()
    return f" AND {column} IN ({', '.join('?' * len(keys))})", keys


def _sqlite_claim(
    names: Names, min_priority: int, now: float, routing_keys: Optional[Iterable[str]]
) -> tuple[str, tuple]:
    """The SQLite-dialect claim statement and its params."""
    routing, keys = _routing_filter(routing_keys, "g.routing_key")
    sql = names.sql(_SQLITE_CLAIM_SQL.replace("{routing}", "{{routing}}")).replace("{routing}", routing)
    return sql, (min_priority, *keys, now)


def _sqlite_create_schema(
    prefix: str, execute: Callable[[str], Any], columns: Callable[[str], list[str]]
) -> None:
    """Create a SQLite-dialect transport's schema: its tables, the columns a table created by an
    earlier schema lacks (`columns(table)` lists a table's), then its indexes."""
    statements = transport_schema("sqlite", prefix)
    names = Names(prefix)
    for sql in statements:
        if sql.startswith("CREATE TABLE"):
            execute(sql)
    existing = {t: columns(getattr(names, t)) for t in ("groups", "messages")}
    for sql in sqlite_transport_upgrade(existing, prefix):
        execute(sql)
    for sql in statements:
        if not sql.startswith("CREATE TABLE"):
            execute(sql)


def _refuse_routing(transport: str, routing_keys: Optional[Iterable[str]]) -> None:
    """A transport that can't route claims by machine refuses a worker given `machines`,
    rather than claiming every machine's messages for it."""
    if routing_keys is not None:
        raise NotImplementedError(
            f"{transport} can't restrict a claim to some machines: run this worker without "
            f"`machines`, or on a transport that routes them (in-memory, SQLite, libSQL, "
            f"rqlite, Postgres, Mongo)"
        )


# The Redis `backlog`, server-side, a chunk at a time (shared by the sync + async backends): one
# `HSCAN` step over the groups with messages (the `prio` hash has one entry per group until it
# drains) — each one's queue length, routing key, whether it is locked (leased or parked), and
# the publish time of its head when `qt:{G}` is as long as `q:{G}` — aggregated by routing key.
# A call reads about `count` groups, so it holds Redis (single-threaded) for that long only;
# `backlog` calls it until the cursor comes back to 0, and sums the chunks.
# ARGV = prefix, cursor, count. Returns [next_cursor, key, messages, executions, held, oldest,
# key, ...], key '' for none, oldest '' when unknown.
_BACKLOG_LUA = """
local prefix = ARGV[1]
local scan = redis.call('HSCAN', prefix .. ':prio', ARGV[2], 'COUNT', tonumber(ARGV[3]))
local acc = {}
local order = {}
local fields = scan[2]
for i = 1, #fields, 2 do
  local g = fields[i]
  local n = redis.call('LLEN', prefix .. ':q:' .. g)
  if n > 0 then
    local key = redis.call('HGET', prefix .. ':route', g) or ''
    local a = acc[key]
    if not a then a = {0, 0, 0, nil}; acc[key] = a; table.insert(order, key) end
    a[1] = a[1] + n
    a[2] = a[2] + 1
    if redis.call('EXISTS', prefix .. ':lock:' .. g) == 1 then a[3] = a[3] + 1 end
    local tkey = prefix .. ':qt:' .. g
    if redis.call('LLEN', tkey) == n then
      local t = tonumber(redis.call('LINDEX', tkey, 0))
      if t and (a[4] == nil or t < a[4]) then a[4] = t end
    end
  end
end
local out = {scan[1]}
for _, key in ipairs(order) do
  local a = acc[key]
  table.insert(out, key)
  table.insert(out, a[1])
  table.insert(out, a[2])
  table.insert(out, a[3])
  table.insert(out, a[4] and tostring(a[4]) or '')
end
return out
"""

# groups a `_BACKLOG_LUA` call reads
_BACKLOG_CHUNK = 1000


def _text(v: Any) -> str:
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


def _redis_backlog(chunks: list[list]) -> "Backlog":
    """A `Backlog` from the `_BACKLOG_LUA` replies of a whole scan (each without its cursor):
    the chunks' rows summed by routing key, the oldest the earliest. A group the scan returns
    twice (`HSCAN` may, when the hash grows while it runs) counts twice: the counts are a
    monitor's, read across several calls while the queue moves."""
    rows: dict = {}
    for flat in chunks:
        for i in range(0, len(flat), 5):
            key, messages, executions, held, oldest = (_text(v) for v in flat[i : i + 5])
            row = rows.setdefault(key or None, [0, 0, 0, None])
            row[0] += int(messages)
            row[1] += int(executions)
            row[2] += int(held)
            if oldest and (row[3] is None or float(oldest) < row[3]):
                row[3] = float(oldest)
    return _backlog((key, *r) for key, r in rows.items())


@dataclass(frozen=True)
class MachineBacklog:
    """The work queued for the executions of one machine (`routing_key`, the root machine of
    their trees; None for messages published without one): how many `messages` wait, for how
    many `executions`, how many of those are `held` — their next message leased to a worker, or
    parked (a suspended execution's, or one retried after a failure) — and when the oldest
    waiting message was published (None when unknown, as for one published before the
    transport kept the time). A count a transport can't tell is None."""

    routing_key: Optional[str]
    messages: int
    executions: Optional[int] = None
    held: Optional[int] = None
    oldest_published_at: Optional[float] = None


@dataclass(frozen=True)
class Backlog:
    """What `Transport.backlog` returns: one `MachineBacklog` per machine with work queued —
    or, when `by_machine` is False (a transport that can't tell machines apart), a single one
    for the whole queue, with `routing_key` None."""

    machines: list[MachineBacklog]
    by_machine: bool = True


@dataclass
class Lease:
    """A claimed message: the `group_id` it belongs to and the `event`, plus the
    backend's handle to identify it on ack/nack — `seq` (the row/message id, for
    the in-memory and sqlite backends) or `token` (the Redis group-lock fencing
    token). Held until `ack` (delivered) or `nack`/expiry (re-deliver)."""

    seq: int
    group_id: str
    event: Event
    token: str = ""


@runtime_checkable
class Transport(Protocol):
    """A queue with single-active-consumer per group (`group_id` = execution id).

    A transport whose writes go into the same transaction as the store's — both on the
    caller's connection — sets `shares_store_transaction = True` (an optional attribute; absent
    means False). The runner then lets a failed `publish` of a new execution's `Start` reach
    the caller: the transaction it was part of can't commit, so there is nothing for a later
    relay to deliver. Otherwise the `Start` committed to the outbox stays queued, and a later
    flush delivers it."""

    def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        """Enqueue `event` in `group_id`'s FIFO. `priority` and `routing_key` (the machine the
        execution's tree started from) are stored when the publish creates the group (INSERT OR
        IGNORE / HSETNX semantics); publishes to a group that exists leave them as they are. A
        group is deleted once drained, so every publish passes the execution's own."""
        ...

    def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        """Lease the oldest message of some group that has nothing in-flight, for
        `visibility` seconds; None if there is nothing deliverable right now.
        Only considers groups whose `priority >= min_priority` and, when `routing_keys` is
        given, whose `routing_key` is one of them. A transport that can't restrict a claim
        that way raises `NotImplementedError` when given `routing_keys`."""
        ...

    def ack(self, lease: Lease) -> None:
        """The message was handled: remove it, freeing its group."""
        ...

    def backlog(self) -> Backlog:
        """The work queued now, by machine (see `Backlog`): for a monitor, not the hot path —
        it may read every group with work."""
        ...

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        """Return the message to the queue. With `delay=0` it is immediately
        claimable again (retry now); with `delay>0` it is *parked* — not claimable
        (and its group stays blocked) until `delay` seconds pass. Parking lets a
        worker bounce a suspended group's message without spinning on it."""
        ...

    def close(self) -> None:
        """Release any backend resources (connection/client/session)."""
        ...
