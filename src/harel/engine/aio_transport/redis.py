"""AsyncRedisTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport import _ACK_LUA, _CLAIM_LUA, Lease
from harel.spec.states import Event


class AsyncRedisTransport:
    """Async mirror of `RedisTransport` over `redis.asyncio`: per-group exclusivity by hand
    (a `SET PX` group-lock-as-lease + a list per group), and a `ready` ZSET scored by
    available-at time so `claim` reads only the few lowest-scored due groups (O(log N + K),
    not a full scan). `claim` runs `_CLAIM_LUA` server-side, so concurrent claimers each get a
    DISTINCT group atomically (no lost `SET NX` races). Leasing bumps the score (concurrent
    claimers skip it + free expiry recovery). The client is injected (fakeredis.aioredis in
    tests; the Lua claim needs `lupa` for fakeredis)."""

    _CANDIDATES = 8

    def __init__(self, client: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._r = client
        self._prefix = prefix
        self._clock = clock
        self._claim_script = client.register_script(_CLAIM_LUA)
        self._ack_script = client.register_script(_ACK_LUA)

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "AsyncRedisTransport":
        import redis.asyncio as aioredis

        return cls(aioredis.Redis.from_url(url), prefix)

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

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._r.pipeline() as pipe:
            pipe.rpush(self._k_q(group_id), event.model_dump_json())
            pipe.zadd(self._k_ready(), {group_id: 0}, nx=True)
            await pipe.execute()

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        px = max(1, int(visibility * 1000))
        now = self._now_ms()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one atomic round-trip: lock a distinct due group + return its head (no SET NX race)
        res = await self._claim_script(
            keys=[self._k_ready()], args=[self._prefix, now, px, token, self._CANDIDATES]
        )
        if not res:
            return None
        group_id = self._decode(res[0])
        payload = self._decode(res[1])
        assert group_id is not None and payload is not None
        return Lease(seq=0, group_id=group_id, event=Event.model_validate_json(payload), token=token)

    async def _owns(self, group_id: str, token: str) -> bool:
        return self._decode(await self._r.get(self._k_lock(group_id))) == token

    async def ack(self, lease: Lease) -> None:
        # one atomic round-trip: fence on the token, pop the head, re-ready or drop, free the lock
        await self._ack_script(keys=[self._k_ready()], args=[self._prefix, lease.group_id, lease.token])

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if not await self._owns(lease.group_id, lease.token):
            return
        if delay > 0:
            await self._r.zadd(self._k_ready(), {lease.group_id: self._now_ms() + int(delay * 1000)})
            await self._r.set(self._k_lock(lease.group_id), lease.token, px=max(1, int(delay * 1000)))
        else:
            await self._r.zadd(self._k_ready(), {lease.group_id: 0})
            await self._r.delete(self._k_lock(lease.group_id))

    async def close(self) -> None:
        await self._r.aclose()
