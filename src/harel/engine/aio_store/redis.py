"""AsyncRedisStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import _COMMIT_CAS_LUA, DEFAULT_TRACE_MAX
from harel.spec.states import Event


class AsyncRedisStore:
    """Async mirror of `RedisStore` over `redis.asyncio`: a state-only commit takes the
    fast path (version-CAS + SET + dedupe in one atomic Lua round-trip); a complex commit
    (emits/spawns/timers/trace) takes WATCH/MULTI/EXEC. Transactional outbox in a hash,
    dedupe in a set, timers in a sorted set. The client is injected (duck-typed; the Lua
    fast path needs `lupa` under fakeredis.aioredis)."""

    def __init__(self, client: Any, prefix: str = "stm") -> None:
        from redis.exceptions import ResponseError, WatchError

        self._r = client
        self._prefix = prefix
        self._WatchError = WatchError
        self._ResponseError = ResponseError
        self.trace_max = DEFAULT_TRACE_MAX
        self._commit_cas_script = client.register_script(_COMMIT_CAS_LUA)

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "AsyncRedisStore":
        import redis.asyncio as aioredis

        return cls(aioredis.Redis.from_url(url), prefix)

    def _k(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"

    async def load(self, execution_id: str) -> Optional[Execution]:
        raw = await self._r.get(self._k(f"exe:{execution_id}"))
        return Execution.model_validate_json(raw) if raw is not None else None

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one round-trip: pipeline the GET and the SISMEMBER."""
        pipe = self._r.pipeline(transaction=False)
        pipe.get(self._k(f"exe:{execution_id}"))
        pipe.sismember(self._k(f"processed:{execution_id}"), event_id)
        raw, hit = await pipe.execute()
        if raw is None:
            return None, False
        return Execution.model_validate_json(raw), bool(hit)

    async def save(self, exe: Execution) -> None:
        await self.commit(exe, [])

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> None:
        # fast path: an event that only advances state (no emits/spawns/timers/trace) commits
        # in ONE atomic round-trip — version-CAS + SET + optional dedupe — instead of WATCH/MULTI.
        if not emits and not spawns and not timers and trace is None:
            await self._commit_cas(exe, processed_event_id)
            return
        queued = [(int(await self._r.incr(self._k("outbox:seq"))), t, e.model_dump_json()) for t, e in emits]
        queued_spawns = [
            (int(await self._r.incr(self._k("spawns:seq"))), cid, rp, ctx) for cid, rp, ctx in spawns
        ]
        trace_step = None
        if trace is not None:
            idx = int(await self._r.incr(self._k(f"trace:seq:{exe.id}"))) - 1
            trace_step = json.dumps({**trace, "index": idx})
        key = self._k(f"exe:{exe.id}")
        old = exe.version
        async with self._r.pipeline() as pipe:
            try:
                await pipe.watch(key)
                current = await pipe.get(key)
                cur_version = json.loads(current)["version"] if current is not None else None
                if not (current is None and old == 0) and cur_version != old:
                    await pipe.unwatch()
                    raise StoreConflict(exe.id, expected=old, found=cur_version)
                exe.version = old + 1
                pipe.multi()
                pipe.set(key, exe.model_dump_json())
                for seq, target_id, event_json in queued:
                    pipe.hset(self._k("outbox"), str(seq), json.dumps({"t": target_id, "e": event_json}))
                if processed_event_id is not None:
                    pipe.sadd(self._k(f"processed:{exe.id}"), processed_event_id)
                for seq, cid, rp, ctx in queued_spawns:
                    pipe.hset(
                        self._k("spawns"), str(seq), json.dumps({"p": exe.id, "c": cid, "r": rp, "x": ctx})
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
                await pipe.execute()
            except self._WatchError:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=None)

    async def _commit_cas(self, exe: Execution, processed_event_id: Optional[str]) -> None:
        """The fast-path commit: version-CAS + SET (+ dedupe) in one atomic Lua round-trip."""
        key = self._k(f"exe:{exe.id}")
        old = exe.version
        exe.version = old + 1
        try:
            await self._commit_cas_script(
                keys=[key, self._k(f"processed:{exe.id}")],
                args=[exe.model_dump_json(), old, processed_event_id or ""],
            )
        except self._ResponseError as exc:
            exe.version = old
            msg = str(exc)
            if "STM_CONFLICT" in msg:
                tail = msg.split("STM_CONFLICT:")[-1].strip()
                found = int(tail) if tail.lstrip("-").isdigit() else None
                raise StoreConflict(exe.id, expected=old, found=found) from None
            raise

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return bool(await self._r.sismember(self._k(f"processed:{execution_id}"), event_id))

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        idx = int(await self._r.incr(self._k(f"trace:seq:{execution_id}"))) - 1
        tkey = self._k(f"trace:{execution_id}")
        await self._r.rpush(tkey, json.dumps({**entry, "index": entry.get("index", idx)}))
        if self.trace_max:
            await self._r.ltrim(tkey, -self.trace_max, -1)

    async def read_trace(self, execution_id: str) -> list[dict]:
        return [json.loads(x) for x in await self._r.lrange(self._k(f"trace:{execution_id}"), 0, -1)]

    async def pending_spawns(self) -> list[SpawnEntry]:
        entries = []
        for seq_raw, val_raw in (await self._r.hgetall(self._k("spawns"))).items():
            p = json.loads(val_raw)
            entries.append(SpawnEntry(int(seq_raw), p["p"], p["c"], p["r"], p["x"]))
        return sorted(entries, key=lambda s: s.seq)

    async def ack_spawn(self, seq: int) -> None:
        await self._r.hdel(self._k("spawns"), str(seq))

    async def pending_outbox(self) -> list[OutboxEntry]:
        entries = []
        for seq_raw, val_raw in (await self._r.hgetall(self._k("outbox"))).items():
            payload = json.loads(val_raw)
            entries.append(OutboxEntry(int(seq_raw), payload["t"], Event.model_validate_json(payload["e"])))
        return sorted(entries, key=lambda e: e.seq)

    async def ack_outbox(self, seq: int) -> None:
        await self._r.hdel(self._k("outbox"), str(seq))

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        for member_raw, score in await self._r.zrangebyscore(self._k("timers"), "-inf", now, withscores=True):
            member = member_raw.decode() if isinstance(member_raw, (bytes, bytearray)) else member_raw
            execution_id, _, path = member.partition("\x00")
            out.append((execution_id, path, float(score)))
        return out

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        member = f"{execution_id}\x00{path}"
        score = await self._r.zscore(self._k("timers"), member)
        if score is not None and float(score) == fire_at:
            await self._r.zrem(self._k("timers"), member)

    async def close(self) -> None:
        await self._r.aclose()
