"""RedisTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport._base import Lease
from harel.spec.states import Event


class RedisTransport:
    """`Transport` over Redis, with the per-group exclusivity built by hand since
    Redis has no native message groups:

    - `q:{G}` — a list per group, the FIFO (RPUSH to enqueue, the head is oldest).
    - `lock:{G}` — `SET NX PX` is the group lock *and* the fencing token: only one
      worker holds it (the synchronous mutual exclusion that makes the claim race
      safe), and its TTL (the visibility timeout) auto-releases it if the worker
      dies, so the head becomes claimable again.
    - `ready` — a sorted set of groups that have messages, scored by the epoch-ms
      at which the group is next claimable (0 = now). `claim` reads only the few
      lowest-scored due groups (`ZRANGEBYSCORE -inf now LIMIT 0 K`), so its cost is
      O(log N + K) in the number of pending groups — NOT a full scan of every group
      (the old `SMEMBERS groups`, which collapsed throughput under a large backlog).
      Leasing a group bumps its score to `now + visibility`, so other claimers skip
      it AND it reappears on its own once the lease expires (the expiry-recovery
      timer, with no separate sweep).

    `claim` locks a due group and returns its head without removing it; `ack` (lock
    still owned) pops the head and re-readies the group (or drops it); `nack`
    re-readies now, or parks it for `delay`. The client is injected (any redis-py-
    compatible client, e.g. fakeredis), so `redis` is an optional dependency. NOTE:
    like the sqlite lease, a lock that expires mid-ack can let two workers touch one
    group; the store's version/CAS is the backstop (a stale worker's commit is
    rejected)."""

    # how many lowest-scored due groups `claim` considers per call — bounds the work
    # so it never scales with the total number of pending groups (a contended head
    # group does not starve other ready groups).
    _CANDIDATES = 8

    def __init__(self, client: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._r = client
        self._prefix = prefix
        self._clock = clock  # injectable so the ready-score clock is deterministic in tests

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "RedisTransport":
        """Convenience constructor; imports `redis` lazily (the optional dep)."""
        import redis

        return cls(redis.Redis.from_url(url), prefix)

    def _k_ready(self) -> str:
        return f"{self._prefix}:ready"

    def _k_q(self, group_id: str) -> str:
        return f"{self._prefix}:q:{group_id}"

    def _k_lock(self, group_id: str) -> str:
        return f"{self._prefix}:lock:{group_id}"

    @staticmethod
    def _decode(value: Any) -> Optional[str]:
        if value is None:
            return None
        return value.decode() if isinstance(value, (bytes, bytearray)) else value

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def publish(self, group_id: str, event: Event) -> None:
        pipe = self._r.pipeline()
        pipe.rpush(self._k_q(group_id), event.model_dump_json())
        # NX: never reset the score of a group that is already scheduled — a publish
        # into an in-flight or parked group must not make it claimable before its
        # lease/park elapses. A brand-new group gets score 0 (claimable now).
        pipe.zadd(self._k_ready(), {group_id: 0}, nx=True)
        pipe.execute()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        px = max(1, int(visibility * 1000))
        now = self._now_ms()
        # only the few lowest-scored groups that are due now — O(log N + K), not O(N)
        candidates = self._r.zrangebyscore(self._k_ready(), "-inf", now, start=0, num=self._CANDIDATES)
        for raw in candidates:
            group_id = self._decode(raw)
            assert group_id is not None
            token = f"{worker_id}:{uuid.uuid4().hex}"
            # SET NX PX is the per-group lock: only one worker wins the race for a
            # candidate, and it expires on its own (the lease) if the worker dies.
            if not self._r.set(self._k_lock(group_id), token, nx=True, px=px):
                continue  # held by another worker -> try the next candidate
            payload = self._decode(self._r.lindex(self._k_q(group_id), 0))
            if payload is None:
                # a stale group with no messages: drop it and release the lock
                self._r.zrem(self._k_ready(), group_id)
                self._r.delete(self._k_lock(group_id))
                continue
            # bump the score out by the visibility window: concurrent claimers skip
            # it, and it reappears as a candidate once the lease expires (recovery).
            self._r.zadd(self._k_ready(), {group_id: now + px})
            return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)
        return None

    def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(self._r.get(self._k_lock(group_id))) == token

    def ack(self, lease: Lease) -> None:
        # fencing: only the current lock holder removes the head + frees the group
        if not self._owns(lease.group_id, lease.token):
            return
        self._r.lpop(self._k_q(lease.group_id))
        if self._r.llen(self._k_q(lease.group_id)) == 0:
            self._r.zrem(self._k_ready(), lease.group_id)
        else:
            self._r.zadd(self._k_ready(), {lease.group_id: 0})  # next message claimable now (FIFO)
        self._r.delete(self._k_lock(lease.group_id))

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            # park: not claimable until `delay` passes (score in the future), and keep
            # the lock for the same window so the still-present head isn't re-claimed.
            self._r.zadd(self._k_ready(), {lease.group_id: self._now_ms() + int(delay * 1000)})
            self._r.set(self._k_lock(lease.group_id), lease.token, px=max(1, int(delay * 1000)))
        else:
            # release: re-ready now and drop the lock so the head can be re-claimed
            self._r.zadd(self._k_ready(), {lease.group_id: 0})
            self._r.delete(self._k_lock(lease.group_id))

    def close(self) -> None:
        self._r.close()
