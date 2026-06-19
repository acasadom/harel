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

# The Redis `claim`, server-side and atomic (shared by the sync + async backends).
# Scan the lowest-due groups, lock the first whose lock is free and whose queue has a
# head, bump its ready-score out of the visibility window, and return (group, head). All
# in ONE round-trip that Redis runs atomically — so concurrent claimers each get a
# DISTINCT group with ZERO lost races. It replaces a client-side ZRANGEBYSCORE-then-loop-
# of-`SET NX`, where workers raced for the same candidate head and burned round-trips on
# lost locks (throughput plateaued ~8 workers and *regressed* beyond). KEYS[1]=ready zset;
# ARGV = prefix, now_ms, lease_px_ms, fencing_token, candidate_limit. The lock:/q: keys are
# computed from the prefix, so this targets a single Redis instance (not Redis Cluster —
# the transport never was). A stale empty group found in the window is dropped (ZREM).
_CLAIM_LUA = """
local ready = KEYS[1]
local prefix = ARGV[1]
local now = tonumber(ARGV[2])
local px = tonumber(ARGV[3])
local token = ARGV[4]
local limit = tonumber(ARGV[5])
local cands = redis.call('ZRANGEBYSCORE', ready, '-inf', now, 'LIMIT', 0, limit)
for i = 1, #cands do
  local g = cands[i]
  local lockkey = prefix .. ':lock:' .. g
  if redis.call('EXISTS', lockkey) == 0 then
    local payload = redis.call('LINDEX', prefix .. ':q:' .. g, 0)
    if payload then
      redis.call('SET', lockkey, token, 'PX', px)
      redis.call('ZADD', ready, now + px, g)
      return {g, payload}
    else
      redis.call('ZREM', ready, g)
    end
  end
end
return nil
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
    def publish(self, group_id: str, event: Event) -> None:
        """Enqueue `event` in `group_id`'s FIFO."""
        ...

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        """Lease the oldest message of some group that has nothing in-flight, for
        `visibility` seconds; None if there is nothing deliverable right now."""
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
