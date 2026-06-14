"""RedisStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _matches,
)
from harel.spec.states import Event


class RedisStore:
    """A durable `ExecutionStore` over Redis — the all-network alternative to
    `SqliteStore` (no shared filesystem, so it works across machines / containers
    without a shared volume). Pairs naturally with `RedisTransport` for a pure-
    Redis deployment. The client is injected (duck-typed), so `redis` stays an
    optional dependency and tests use fakeredis.

    Keys (under `prefix`): ``exe:{id}`` = the Execution JSON; ``outbox`` = a hash
    {seq -> emit}; ``outbox:seq`` = the monotonic counter; ``processed:{id}`` = a
    set of handled event ids. `commit` is atomic via WATCH/MULTI/EXEC on the
    Execution key (no Lua, so fakeredis supports it): the version CAS is checked
    under WATCH and the writes go in one EXEC; a concurrent change → `StoreConflict`."""

    def __init__(self, client: Any, prefix: str = "stm") -> None:
        from redis.exceptions import WatchError

        self._r = client
        self._prefix = prefix
        self._WatchError = WatchError
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "RedisStore":
        """Convenience constructor; imports `redis` lazily (the optional dep)."""
        import redis

        return cls(redis.Redis.from_url(url), prefix)

    def _k(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"

    def load(self, execution_id: str) -> Optional[Execution]:
        raw = self._r.get(self._k(f"exe:{execution_id}"))
        return Execution.model_validate_json(raw) if raw is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # Redis can't query inside a value, so list = SCAN the exe:* keys + MGET +
        # filter client-side. Order is arbitrary and a page is best-effort sized (one
        # SCAN round); keep paging while next_cursor is set. cursor = native SCAN cursor.
        status = set(status) if status is not None else None
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

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> None:
        # allocate monotonic outbox seqs up front (INCR can't return its value
        # inside MULTI; a seq wasted by an aborted txn is harmless)
        queued = [(int(self._r.incr(self._k("outbox:seq"))), t, e.model_dump_json()) for t, e in emits]
        queued_spawns = [(int(self._r.incr(self._k("spawns:seq"))), cid, rp, ctx) for cid, rp, ctx in spawns]
        # per-execution 0-based trace index (a list per id, ring-trimmed with LTRIM)
        trace_step = None
        if trace is not None:
            idx = int(self._r.incr(self._k(f"trace:seq:{exe.id}"))) - 1
            trace_step = json.dumps({**trace, "index": idx})
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

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return bool(self._r.sismember(self._k(f"processed:{execution_id}"), event_id))

    def append_trace(self, execution_id: str, entry: dict) -> None:
        idx = int(self._r.incr(self._k(f"trace:seq:{execution_id}"))) - 1
        tkey = self._k(f"trace:{execution_id}")
        self._r.rpush(tkey, json.dumps({**entry, "index": entry.get("index", idx)}))
        if self.trace_max:
            self._r.ltrim(tkey, -self.trace_max, -1)

    def read_trace(self, execution_id: str) -> list[dict]:
        return [json.loads(x) for x in self._r.lrange(self._k(f"trace:{execution_id}"), 0, -1)]

    def pending_spawns(self) -> list[SpawnEntry]:
        entries = []
        for seq_raw, val_raw in self._r.hgetall(self._k("spawns")).items():
            p = json.loads(val_raw)
            entries.append(SpawnEntry(int(seq_raw), p["p"], p["c"], p["r"], p["x"]))
        return sorted(entries, key=lambda s: s.seq)

    def ack_spawn(self, seq: int) -> None:
        self._r.hdel(self._k("spawns"), str(seq))

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        for member_raw, score in self._r.zrangebyscore(self._k("timers"), "-inf", now, withscores=True):
            member = member_raw.decode() if isinstance(member_raw, (bytes, bytearray)) else member_raw
            execution_id, _, path = member.partition("\x00")
            out.append((execution_id, path, float(score)))
        return out

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        member = f"{execution_id}\x00{path}"
        score = self._r.zscore(self._k("timers"), member)
        if score is not None and float(score) == fire_at:
            self._r.zrem(self._k("timers"), member)

    def pending_outbox(self) -> list[OutboxEntry]:
        entries = []
        for seq_raw, val_raw in self._r.hgetall(self._k("outbox")).items():
            payload = json.loads(val_raw)
            entries.append(OutboxEntry(int(seq_raw), payload["t"], Event.model_validate_json(payload["e"])))
        return sorted(entries, key=lambda e: e.seq)

    def ack_outbox(self, seq: int) -> None:
        self._r.hdel(self._k("outbox"), str(seq))

    def close(self) -> None:
        self._r.close()
