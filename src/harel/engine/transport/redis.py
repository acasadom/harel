"""RedisTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport._base import _ACK_LUA, _CLAIM_LUA, Lease
from harel.spec.states import Event


class RedisTransport:
    """`Transport` over Redis, with the per-group exclusivity built by hand since
    Redis has no native message groups:

    - `q:{G}` — a list per group, the FIFO (RPUSH to enqueue, the head is oldest).
    - `lock:{G}` — the group lock *and* the fencing token: only one worker holds it,
      and its TTL (the visibility timeout) auto-releases it if the worker dies, so the
      head becomes claimable again. `claim` sets it inside the atomic Lua script
      (`_CLAIM_LUA`), so two workers never race for the same group.
    - `ready` — a sorted set of groups that have messages, scored by the epoch-ms
      at which the group is next claimable (0 = now). `claim` reads only the few
      lowest-scored due groups (`ZRANGEBYSCORE -inf now LIMIT 0 K`), so its cost is
      O(log N + K) in the number of pending groups — NOT a full scan of every group
      (the old `SMEMBERS groups`, which collapsed throughput under a large backlog).
      Leasing a group bumps its score to `now + visibility`, so other claimers skip
      it AND it reappears on its own once the lease expires (the expiry-recovery
      timer, with no separate sweep). `claim` runs server-side as one atomic Lua call
      (`_CLAIM_LUA`): concurrent claimers each get a DISTINCT group with zero lost
      lock races (the previous client-side `SET NX` loop made workers race for the
      same head and collapsed throughput past ~8 workers).

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
        self._claim_script = client.register_script(_CLAIM_LUA)  # atomic server-side claim
        self._ack_script = client.register_script(_ACK_LUA)  # atomic server-side ack

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
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one atomic round-trip: the script locks a distinct due group (scoring it out of
        # the window) and returns its head — no client-side SET NX race between workers.
        res = self._claim_script(
            keys=[self._k_ready()], args=[self._prefix, now, px, token, self._CANDIDATES]
        )
        if not res:
            return None
        group_id = self._decode(res[0])
        payload = self._decode(res[1])
        assert group_id is not None and payload is not None
        return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)

    def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(self._r.get(self._k_lock(group_id))) == token

    def ack(self, lease: Lease) -> None:
        # one atomic round-trip: fence on the token, pop the head, re-ready or drop, free the lock
        # (replaces GET+LPOP+LLEN+ZADD/ZREM+DEL and closes the lock-expires-mid-ack window)
        self._ack_script(keys=[self._k_ready()], args=[self._prefix, lease.group_id, lease.token])

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
