"""Shared transport contract: the `Transport` Protocol, the `Lease` dataclass, and
the `_PARKED` sentinel. Concrete backends live in sibling modules; the package
`__init__` re-exports them so existing imports keep working."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from harel.spec.states import Event

# sentinel `locked_by` for a message parked by `nack(delay>0)`: non-null (so the
# claim's "available"/in-flight checks skip it) until its `lock_expiry` passes.
_PARKED = "__parked__"

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
if priority < 0 then priority = 0 end
if priority > 4 then priority = 4 end
redis.call('RPUSH', prefix .. ':q:' .. g, payload)
redis.call('HSETNX', prefix .. ':prio', g, priority)
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
redis.call('LPOP', qkey)
local prio_key = prefix .. ':prio'
local eff = tonumber(redis.call('HGET', prio_key, g)) or 0
local ready = prefix .. ':ready:' .. eff
if redis.call('LLEN', qkey) == 0 then
  redis.call('ZREM', ready, g)
  -- clean up the priority entry so a recycled group_id gets its new priority on next publish
  redis.call('HDEL', prio_key, g)
else
  -- score = now_ms so this group goes to the back of its tier's queue (round-robin)
  redis.call('ZADD', ready, now_ms, g)
end
redis.call('DEL', lockkey)
return 1
"""

# Postgres `claim`/`ack` as PL/pgSQL functions (shared by the sync + async backends). The
# diagnostic showed the PG worker is round-trip-bound (NOT fsync-bound — `synchronous_commit=off`
# didn't help), with ~7 statements/event across the ops. Folding each op's statements into one
# server-side function call is the Postgres analog of the Redis Lua scripts: `claim` (UPDATE-lease
# + SELECT-head + stale-empty cleanup) and `ack` (fence + DELETE + free-lock) each become ONE
# round-trip. Created idempotently in the transport's schema setup. The lease itself was already
# atomic (FOR UPDATE SKIP LOCKED), so this is about round-trips, not a claim race.
_PG_CLAIM_FN = """
CREATE OR REPLACE FUNCTION harel_claim(p_now double precision, p_lease double precision, p_token text, p_min_priority int DEFAULT 0)
RETURNS TABLE(group_id text, seq bigint, event text) AS $$
DECLARE g text;
BEGIN
  LOOP
    UPDATE transport_groups tg SET locked_by = p_token, lock_expiry = p_lease
    WHERE tg.group_id = (
      SELECT s.group_id FROM transport_groups s
      WHERE (s.locked_by IS NULL OR s.lock_expiry < p_now)
        AND s.priority >= p_min_priority
      ORDER BY COALESCE(s.lock_expiry, 0) ASC, s.group_id FOR UPDATE SKIP LOCKED LIMIT 1
    ) RETURNING tg.group_id INTO g;
    IF g IS NULL THEN RETURN; END IF;
    RETURN QUERY SELECT m.group_id, m.seq, m.event FROM transport_messages m
                 WHERE m.group_id = g ORDER BY m.seq LIMIT 1;
    IF FOUND THEN RETURN; END IF;
    DELETE FROM transport_groups WHERE transport_groups.group_id = g AND locked_by = p_token;
  END LOOP;
END; $$ LANGUAGE plpgsql;
"""
_PG_ACK_FN = """
DROP FUNCTION IF EXISTS harel_ack(text, bigint, text);
CREATE OR REPLACE FUNCTION harel_ack(p_group text, p_seq bigint, p_token text, p_now double precision)
RETURNS void AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM transport_groups WHERE group_id = p_group AND locked_by = p_token) THEN
    DELETE FROM transport_messages WHERE seq = p_seq;
    IF EXISTS (SELECT 1 FROM transport_messages WHERE group_id = p_group) THEN
      -- lock_expiry = p_now so this group sorts after fresh ones in the next claim (round-robin)
      UPDATE transport_groups SET locked_by = NULL, lock_expiry = p_now
      WHERE group_id = p_group AND locked_by = p_token;
    ELSE
      -- group drained: delete the row so priority is reset on next publish (no stale priority)
      DELETE FROM transport_groups WHERE group_id = p_group AND locked_by = p_token;
    END IF;
  END IF;
END; $$ LANGUAGE plpgsql;
"""


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
    def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        """Enqueue `event` in `group_id`'s FIFO. `priority` is stored on first publish only
        (INSERT OR IGNORE / HSETNX semantics); subsequent publishes to the same group ignore it."""
        ...

    def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        """Lease the oldest message of some group that has nothing in-flight, for
        `visibility` seconds; None if there is nothing deliverable right now.
        Only considers groups whose `priority >= min_priority`."""
        ...

    def ack(self, lease: Lease) -> None:
        """The message was handled: remove it, freeing its group."""
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
