"""Async `ExecutionStore` — the async sibling of `harel.engine.store`.

Same contract as the sync `ExecutionStore` (state + transactional outbox + dedupe +
durable timers + spawn-outbox), with every method `async def`. The sync stores in
`store.py` are kept untouched; the sync public API reaches these via the anyio facade.

This module holds the `AsyncExecutionStore` Protocol + `AsyncDictStore` (in-memory). The
networked async backends (sqlite/redis/postgres) live alongside, added in later phases.
The data classes (`OutboxEntry`/`SpawnEntry`/`TimerOp`/`StoreConflict`) are reused as-is.
"""

from __future__ import annotations

import json
from typing import Any, Optional, Protocol, runtime_checkable

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.spec.states import Event


@runtime_checkable
class AsyncExecutionStore(Protocol):
    """Async mirror of `ExecutionStore`: identical semantics, awaited IO. Backend-agnostic,
    so the deferred backends (rqlite/sqs/mongo/surreal/dynamo) slot in unchanged later."""

    async def load(self, execution_id: str) -> Optional[Execution]: ...

    async def save(self, exe: Execution) -> None: ...

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: "tuple[TimerOp, ...]" = (),
        spawns: "tuple[tuple[str, str, dict], ...]" = (),
    ) -> None: ...

    async def is_processed(self, execution_id: str, event_id: str) -> bool: ...

    async def pending_outbox(self) -> list[OutboxEntry]: ...

    async def ack_outbox(self, seq: int) -> None: ...

    async def pending_spawns(self) -> "list[SpawnEntry]": ...

    async def ack_spawn(self, seq: int) -> None: ...

    async def due_timers(self, now: float) -> "list[tuple[str, str, float]]": ...

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None: ...

    async def close(self) -> None: ...


class AsyncDictStore:
    """In-memory `AsyncExecutionStore`: the async mirror of `DictStore`. Returns the same
    `Execution` object that was saved (no serialization), so callers holding a reference see
    mutations — the identity contract the in-place test harness relies on. No lock: a single
    event loop schedules cooperatively and none of these methods await internally, so each
    runs atomically between suspension points."""

    def __init__(self) -> None:
        self._by_id: dict[str, Execution] = {}
        self._outbox: list[OutboxEntry] = []
        self._processed: set[tuple[str, str]] = set()
        self._timers: dict[tuple[str, str], float] = {}
        self._spawns: list[SpawnEntry] = []
        self._seq = 0
        self._spawn_seq = 0

    async def load(self, execution_id: str) -> Optional[Execution]:
        return self._by_id.get(execution_id)

    async def save(self, exe: Execution) -> None:
        prev = self._by_id.get(exe.id)
        if prev is not None and prev is not exe and prev.version != exe.version:
            raise StoreConflict(exe.id, expected=exe.version, found=prev.version)
        exe.version += 1
        self._by_id[exe.id] = exe

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        await self.save(exe)  # CAS first: raises before any emit is enqueued
        for target_id, event in emits:
            self._seq += 1
            self._outbox.append(OutboxEntry(self._seq, target_id, event))
        if processed_event_id is not None:
            self._processed.add((exe.id, processed_event_id))
        for op in timers:
            if op.action == "schedule":
                self._timers[(exe.id, op.path)] = op.fire_at
            else:
                self._timers.pop((exe.id, op.path), None)
        for child_id, root_path, context in spawns:
            self._spawn_seq += 1
            self._spawns.append(SpawnEntry(self._spawn_seq, exe.id, child_id, root_path, dict(context)))

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (execution_id, event_id) in self._processed

    async def pending_outbox(self) -> list[OutboxEntry]:
        return list(self._outbox)

    async def ack_outbox(self, seq: int) -> None:
        self._outbox = [e for e in self._outbox if e.seq != seq]

    async def pending_spawns(self) -> list[SpawnEntry]:
        return list(self._spawns)

    async def ack_spawn(self, seq: int) -> None:
        self._spawns = [s for s in self._spawns if s.seq != seq]

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        return [(eid, path, fa) for (eid, path), fa in self._timers.items() if fa <= now]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        if self._timers.get((execution_id, path)) == fire_at:
            del self._timers[(execution_id, path)]

    async def close(self) -> None:
        pass


class AsyncSqliteStore:
    """Async mirror of `SqliteStore` over `aiosqlite`: each Execution stored as JSON keyed
    by id, version-CAS via UPDATE-WHERE-version, the whole `commit` one atomic transaction.
    aiosqlite serializes a connection's ops on its own worker thread, so the multi-statement
    commit stays atomic. Build with `await AsyncSqliteStore.create(path)` (the connection must
    be awaited open); `:memory:` is a non-persistent variant for tests."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @classmethod
    async def create(cls, path: str = ":memory:") -> "AsyncSqliteStore":
        import aiosqlite

        conn = await aiosqlite.connect(str(path))
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS executions "
            "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS timers "
            "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, "
            "PRIMARY KEY (execution_id, path))"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS spawns "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
            "root_path TEXT NOT NULL, context TEXT NOT NULL)"
        )
        await conn.commit()
        return cls(conn)

    async def load(self, execution_id: str) -> Optional[Execution]:
        cur = await self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,))
        row = await cur.fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    async def _write(self, exe: Execution) -> None:
        """CAS write WITHOUT committing (so it batches atomically with the outbox inserts)."""
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = await self._conn.execute(
            "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            found_cur = await self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,))
            found = await found_cur.fetchone()
            if found is None and old == 0:
                await self._conn.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    async def save(self, exe: Execution) -> None:
        try:
            await self._write(exe)
            await self._conn.commit()
        except StoreConflict:
            await self._conn.rollback()
            raise

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        try:
            await self._write(exe)
            for target_id, event in emits:
                await self._conn.execute(
                    "INSERT INTO outbox (target_id, event) VALUES (?, ?)",
                    (target_id, event.model_dump_json()),
                )
            if processed_event_id is not None:
                await self._conn.execute(
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) VALUES (?, ?)",
                    (exe.id, processed_event_id),
                )
            for child_id, root_path, context in spawns:
                await self._conn.execute(
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                    (exe.id, child_id, root_path, json.dumps(context)),
                )
            for op in timers:
                if op.action == "schedule":
                    await self._conn.execute(
                        "INSERT INTO timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                        "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                        (exe.id, op.path, op.fire_at),
                    )
                else:
                    await self._conn.execute(
                        "DELETE FROM timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                    )
            await self._conn.commit()
        except StoreConflict:
            await self._conn.rollback()
            raise

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        cur = await self._conn.execute(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return (await cur.fetchone()) is not None

    async def pending_outbox(self) -> list[OutboxEntry]:
        cur = await self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq")
        rows = await cur.fetchall()
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        await self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
        await self._conn.commit()

    async def pending_spawns(self) -> list[SpawnEntry]:
        cur = await self._conn.execute(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
        )
        rows = await cur.fetchall()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        await self._conn.execute("DELETE FROM spawns WHERE seq = ?", (seq,))
        await self._conn.commit()

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        cur = await self._conn.execute(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, fa) for eid, path, fa in await cur.fetchall()]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        await self._conn.execute(
            "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )
        await self._conn.commit()

    async def close(self) -> None:
        await self._conn.close()


class AsyncRedisStore:
    """Async mirror of `RedisStore` over `redis.asyncio`: version-CAS via WATCH/MULTI/EXEC
    (no Lua, so fakeredis.aioredis works), transactional outbox in a hash, dedupe in a set,
    timers in a sorted set. The client is injected (duck-typed)."""

    def __init__(self, client: Any, prefix: str = "stm") -> None:
        from redis.exceptions import WatchError

        self._r = client
        self._prefix = prefix
        self._WatchError = WatchError

    @classmethod
    def from_url(cls, url: str, prefix: str = "stm") -> "AsyncRedisStore":
        import redis.asyncio as aioredis

        return cls(aioredis.Redis.from_url(url), prefix)

    def _k(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"

    async def load(self, execution_id: str) -> Optional[Execution]:
        raw = await self._r.get(self._k(f"exe:{execution_id}"))
        return Execution.model_validate_json(raw) if raw is not None else None

    async def save(self, exe: Execution) -> None:
        await self.commit(exe, [])

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        queued = [(int(await self._r.incr(self._k("outbox:seq"))), t, e.model_dump_json()) for t, e in emits]
        queued_spawns = [
            (int(await self._r.incr(self._k("spawns:seq"))), cid, rp, ctx) for cid, rp, ctx in spawns
        ]
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
                await pipe.execute()
            except self._WatchError:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=None)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return bool(await self._r.sismember(self._k(f"processed:{execution_id}"), event_id))

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
