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
from contextlib import AsyncExitStack
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


class AsyncPostgresStore:
    """Async mirror of `PostgresStore` over `psycopg_pool.AsyncConnectionPool`: version-CAS via
    UPDATE-WHERE-version (Postgres row-locks serialize writers — one wins rowcount 1, the loser
    rowcount 0 raises StoreConflict). Each method checks out a connection from the pool for the
    duration of one transaction, so concurrent workers make real parallel DB requests. Build with
    `await AsyncPostgresStore.from_dsn(dsn, pool_size=N)`."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @classmethod
    async def from_dsn(cls, dsn: str, pool_size: int = 10) -> "AsyncPostgresStore":
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS executions "
                    "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INT NOT NULL)"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS outbox "
                    "(seq BIGSERIAL PRIMARY KEY, target_id TEXT, event TEXT NOT NULL)"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS processed_events "
                    "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS timers "
                    "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at DOUBLE PRECISION NOT NULL, "
                    "PRIMARY KEY (execution_id, path))"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS spawns "
                    "(seq BIGSERIAL PRIMARY KEY, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
                    "root_path TEXT NOT NULL, context TEXT NOT NULL)"
                )
            await conn.commit()
        return cls(pool)

    async def load(self, execution_id: str) -> Optional[Execution]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT data FROM executions WHERE id = %s", (execution_id,))
                row = await cur.fetchone()
            await conn.commit()
        return Execution.model_validate_json(row[0]) if row is not None else None

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
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        try:
            async with self._pool.connection() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "UPDATE executions SET data = %s, version = %s WHERE id = %s AND version = %s",
                            (data, exe.version, exe.id, old),
                        )
                        if cur.rowcount == 0:
                            await cur.execute("SELECT version FROM executions WHERE id = %s", (exe.id,))
                            row = await cur.fetchone()
                            if row is None and old == 0:
                                await cur.execute(
                                    "INSERT INTO executions (id, definition_id, data, version) "
                                    "VALUES (%s, %s, %s, %s)",
                                    (exe.id, exe.definition_id, data, exe.version),
                                )
                            else:
                                exe.version = old
                                await conn.rollback()
                                raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
                        for target_id, event in emits:
                            await cur.execute(
                                "INSERT INTO outbox (target_id, event) VALUES (%s, %s)",
                                (target_id, event.model_dump_json()),
                            )
                        if processed_event_id is not None:
                            await cur.execute(
                                "INSERT INTO processed_events (execution_id, event_id) VALUES (%s, %s) "
                                "ON CONFLICT DO NOTHING",
                                (exe.id, processed_event_id),
                            )
                        for child_id, root_path, context in spawns:
                            await cur.execute(
                                "INSERT INTO spawns (parent_id, child_id, root_path, context) "
                                "VALUES (%s, %s, %s, %s)",
                                (exe.id, child_id, root_path, json.dumps(context)),
                            )
                        for op in timers:
                            if op.action == "schedule":
                                await cur.execute(
                                    "INSERT INTO timers (execution_id, path, fire_at) VALUES (%s, %s, %s) "
                                    "ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at",
                                    (exe.id, op.path, op.fire_at),
                                )
                            else:
                                await cur.execute(
                                    "DELETE FROM timers WHERE execution_id = %s AND path = %s",
                                    (exe.id, op.path),
                                )
                    await conn.commit()
                except StoreConflict:
                    raise
                except Exception:
                    exe.version = old
                    await conn.rollback()
                    raise
        except StoreConflict:
            raise

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM processed_events WHERE execution_id = %s AND event_id = %s",
                    (execution_id, event_id),
                )
                found = await cur.fetchone() is not None
            await conn.commit()
        return found

    async def pending_outbox(self) -> list[OutboxEntry]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq")
                rows = await cur.fetchall()
            await conn.commit()
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM outbox WHERE seq = %s", (seq,))
            await conn.commit()

    async def pending_spawns(self) -> list[SpawnEntry]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM spawns WHERE seq = %s", (seq,))
            await conn.commit()

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= %s ORDER BY fire_at",
                    (now,),
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "DELETE FROM timers WHERE execution_id = %s AND path = %s AND fire_at = %s",
                    (execution_id, path, fire_at),
                )
            await conn.commit()

    async def close(self) -> None:
        await self._pool.close()


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


class AsyncSurrealStore:
    """Async mirror of `SurrealStore` over `surrealdb.AsyncSurreal`: version-CAS via a
    server-side `BEGIN … COMMIT` block with a `THROW`-gated upsert (identical semantics to
    the sync version — every `query()` is now awaited). Works on every connection type
    including the in-process `mem://` engine the tests use. Build with
    `await AsyncSurrealStore.from_url(url)` or inject an already-connected `AsyncSurreal`."""

    def __init__(self, client: Any) -> None:
        from surrealdb import SurrealError

        self._db = client
        self._SurrealError = SurrealError

    @classmethod
    async def from_url(
        cls,
        url: str,
        namespace: str = "harel",
        database: str = "harel",
        username: Optional[str] = None,
        password: Optional[str] = None,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncSurrealStore":
        import anyio
        from surrealdb import AsyncSurreal

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = AsyncSurreal(url)
                await client.connect()
                if username is not None:
                    await client.signin({"username": username, "password": password})
                await client.use(namespace, database)
                await client.query("INFO FOR DB")
                return cls(client)
            except Exception as exc:  # noqa: BLE001
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("surreal connect failed")

    async def _next_seq(self, name: str, count: int) -> int:
        res = await self._db.query(
            "UPSERT type::thing('counter',$n) SET v = (v ?? 0) + $k RETURN v", {"n": name, "k": count}
        )
        return int(res[0]["v"]) - count + 1

    async def load(self, execution_id: str) -> Optional[Execution]:
        res = await self._db.query("SELECT data FROM type::thing('executions',$id)", {"id": execution_id})
        return Execution.model_validate_json(res[0]["data"]) if res else None

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
        outbox: list[dict] = []
        if emits:
            base = await self._next_seq("outbox", len(emits))
            outbox = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn: list[dict] = []
        if spawns:
            base = await self._next_seq("spawn", len(spawns))
            spawn = [
                {"seq": base + i, "child_id": cid, "root_path": rp, "context": json.dumps(ctx)}
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()

        stmts = [
            "BEGIN",
            "LET $cur = (SELECT version FROM type::thing('executions',$id))",
            "IF array::len($cur) == 0 { "
            "IF $ov != 0 { THROW 'conflict' }; "
            "CREATE type::thing('executions',$id) SET data=$data, version=$nv, definition_id=$def; "
            "} ELSE { "
            "LET $u = (UPDATE type::thing('executions',$id) SET data=$data, version=$nv "
            "WHERE version=$ov RETURN AFTER); "
            "IF array::len($u) == 0 { THROW 'conflict' }; }",
        ]
        bind: dict[str, Any] = {
            "id": exe.id,
            "ov": old,
            "nv": exe.version,
            "data": data,
            "def": exe.definition_id,
        }
        for i, o in enumerate(outbox):
            stmts.append(f"CREATE outbox SET seq=$o{i}s, target_id=$o{i}t, event=$o{i}e")
            bind[f"o{i}s"], bind[f"o{i}t"], bind[f"o{i}e"] = o["seq"], o["target_id"], o["event"]
        for i, s in enumerate(spawn):
            stmts.append(
                f"CREATE spawns SET seq=$s{i}s, parent_id=$id, child_id=$s{i}c, "
                f"root_path=$s{i}r, context=$s{i}x"
            )
            bind[f"s{i}s"], bind[f"s{i}c"] = s["seq"], s["child_id"]
            bind[f"s{i}r"], bind[f"s{i}x"] = s["root_path"], s["context"]
        if processed_event_id is not None:
            stmts.append("UPSERT type::thing('processed',[$id,$pe]) SET execution_id=$id, event_id=$pe")
            bind["pe"] = processed_event_id
        for i, op in enumerate(timers):
            if op.action == "schedule":
                stmts.append(
                    f"UPSERT type::thing('timers',[$id,$t{i}p]) "
                    f"SET execution_id=$id, path=$t{i}p, fire_at=$t{i}f"
                )
                bind[f"t{i}p"], bind[f"t{i}f"] = op.path, op.fire_at
            else:
                stmts.append(f"DELETE type::thing('timers',[$id,$t{i}p])")
                bind[f"t{i}p"] = op.path
        stmts.append("COMMIT")

        try:
            await self._db.query(";\n".join(stmts) + ";", bind)
        except self._SurrealError:
            exe.version = old
            res = await self._db.query("SELECT version FROM type::thing('executions',$id)", {"id": exe.id})
            found = res[0]["version"] if res else None
            if found is not None and found != old:
                raise StoreConflict(exe.id, expected=old, found=found)
            raise

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        res = await self._db.query(
            "SELECT id FROM type::thing('processed',[$e,$ev])",
            {"e": execution_id, "ev": event_id},
        )
        return bool(res)

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._db.query("SELECT seq, target_id, event FROM outbox ORDER BY seq ASC")
        return [OutboxEntry(r["seq"], r["target_id"], Event.model_validate_json(r["event"])) for r in rows]

    async def ack_outbox(self, seq: int) -> None:
        await self._db.query("DELETE outbox WHERE seq=$s", {"s": seq})

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._db.query(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq ASC"
        )
        return [
            SpawnEntry(r["seq"], r["parent_id"], r["child_id"], r["root_path"], json.loads(r["context"]))
            for r in rows
        ]

    async def ack_spawn(self, seq: int) -> None:
        await self._db.query("DELETE spawns WHERE seq=$s", {"s": seq})

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._db.query(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= $now ORDER BY fire_at ASC",
            {"now": now},
        )
        return [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        await self._db.query(
            "DELETE timers WHERE execution_id=$e AND path=$p AND fire_at=$f",
            {"e": execution_id, "p": path, "f": fire_at},
        )

    async def close(self) -> None:
        await self._db.close()


class AsyncDynamoDBStore:
    """Native-async mirror of `DynamoDBStore` over **aioboto3/aiobotocore** — every call is
    awaited on one long-lived aiohttp-backed client, so concurrent workers (`STM_CONCURRENCY`)
    issue real parallel DynamoDB requests (the aiohttp connection pool), not thread-pool-bounded
    ones. Same semantics as the sync store: conditional writes are the CAS
    (`attribute_not_exists(id)` to insert, `version = :ov` to update) and `TransactWriteItems`
    makes the whole `commit` atomic — a stale write cancels the txn (`TransactionCanceledException`)
    and never leaks its outbox. The `boto3` `TypeSerializer`/`TypeDeserializer` are pure (no IO),
    so they are reused as-is.

    Build with `await AsyncDynamoDBStore.create(...)` (owns its client; `close()` releases it) or
    inject an already-entered aiobotocore client via the constructor (the caller then owns its
    lifecycle). The client binds to the loop that creates it — build it on the loop you run on
    (e.g. inside `anyio.run`), never share one client across loops. Tests mock in-process with
    `aiomoto` (plain `moto.mock_aws` cannot intercept aiobotocore's aiohttp transport)."""

    def __init__(self, client: Any, prefix: str = "harel") -> None:
        from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
        from botocore.exceptions import ClientError

        self._db = client
        self._prefix = prefix
        self._ser = TypeSerializer()
        self._deser = TypeDeserializer()
        self._ClientError = ClientError
        self._stack: Any = None  # set by create() when this store owns the client

    @classmethod
    async def create(
        cls,
        endpoint_url: Optional[str] = None,
        region: str = "us-east-1",
        prefix: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncDynamoDBStore":
        """Open an aioboto3 client (LocalStack-friendly: dummy creds + injected `endpoint_url`;
        pass `endpoint_url=None` for real AWS) and ensure the tables exist, retrying until the
        endpoint is reachable. The client is kept open for the store's life and released by
        `close()`."""
        import aioboto3
        import anyio
        from botocore.exceptions import BotoCoreError, ClientError

        kwargs: dict[str, Any] = {"region_name": region}
        if endpoint_url is not None:
            kwargs.update(endpoint_url=endpoint_url, aws_access_key_id="test", aws_secret_access_key="test")
        stack = AsyncExitStack()
        client = await stack.enter_async_context(aioboto3.Session().client("dynamodb", **kwargs))
        inst = cls(client, prefix)
        inst._stack = stack
        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                await inst._ensure_tables()
                return inst
            except (BotoCoreError, ClientError) as exc:
                last = exc
                await anyio.sleep(retry_delay)
        await stack.aclose()
        raise last if last is not None else RuntimeError("dynamodb connect failed")

    def _t(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    async def _ensure_tables(self) -> None:
        """Create the tables if absent (idempotent — a pre-existing table is fine)."""
        specs = [
            ("executions", [("id", "S")]),
            ("outbox", [("seq", "N")]),
            ("spawns", [("seq", "N")]),
            ("timers", [("execution_id", "S"), ("path", "S")]),
            ("processed", [("execution_id", "S"), ("event_id", "S")]),
            ("counters", [("id", "S")]),
        ]
        roles = ["HASH", "RANGE"]
        for name, keys in specs:
            try:
                await self._db.create_table(
                    TableName=self._t(name),
                    KeySchema=[{"AttributeName": k, "KeyType": roles[i]} for i, (k, _) in enumerate(keys)],
                    AttributeDefinitions=[{"AttributeName": k, "AttributeType": t} for k, t in keys],
                    BillingMode="PAY_PER_REQUEST",
                )
            except self._ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceInUseException":
                    raise  # already exists is fine; anything else is real

    def _raw(self, item: dict) -> dict:
        return {k: self._ser.serialize(v) for k, v in item.items()}

    def _item(self, raw: dict) -> dict:
        return {k: self._deser.deserialize(v) for k, v in raw.items()}

    async def _scan(self, table: str, **params: Any) -> list[dict]:
        """Scan a table, following `LastEvaluatedKey` to drain every page (a single Scan
        returns at most 1MB). `params` adds scan options such as a `FilterExpression`."""
        items: list[dict] = []
        kwargs: dict[str, Any] = {"TableName": self._t(table), **params}
        while True:
            resp = await self._db.scan(**kwargs)
            items.extend(self._item(it) for it in resp.get("Items", []))
            start = resp.get("LastEvaluatedKey")
            if not start:
                return items
            kwargs["ExclusiveStartKey"] = start

    async def _next_seq(self, name: str, count: int) -> int:
        """Reserve `count` monotonic ids from the `name` counter (an atomic ADD); return the
        first. A block wasted by a later-cancelled transaction is harmless."""
        resp = await self._db.update_item(
            TableName=self._t("counters"),
            Key=self._raw({"id": name}),
            UpdateExpression="ADD n :k",
            ExpressionAttributeValues={":k": {"N": str(count)}},
            ReturnValues="UPDATED_NEW",
        )
        return int(resp["Attributes"]["n"]["N"]) - count + 1

    async def load(self, execution_id: str) -> Optional[Execution]:
        resp = await self._db.get_item(
            TableName=self._t("executions"),
            Key=self._raw({"id": execution_id}),
            ProjectionExpression="#d",
            ExpressionAttributeNames={"#d": "data"},
        )
        item = resp.get("Item")
        return Execution.model_validate_json(self._item(item)["data"]) if item else None

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
        from decimal import Decimal

        # allocate monotonic seqs up front (a seq wasted by a cancelled txn is harmless)
        outbox: list[dict] = []
        if emits:
            base = await self._next_seq("outbox", len(emits))
            outbox = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn: list[dict] = []
        if spawns:
            base = await self._next_seq("spawn", len(spawns))
            spawn = [
                {
                    "seq": base + i,
                    "parent_id": exe.id,
                    "child_id": cid,
                    "root_path": rp,
                    "context": json.dumps(ctx),
                }
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        exe_item = {
            "id": exe.id,
            "data": exe.model_dump_json(),
            "version": exe.version,
            "definition_id": exe.definition_id,
        }
        # the Execution Put carries the CAS: insert iff absent (old==0), else update iff the
        # stored version still matches — a failed condition cancels the whole transaction
        if old == 0:
            cas: dict[str, Any] = {"ConditionExpression": "attribute_not_exists(id)"}
        else:
            cas = {
                "ConditionExpression": "version = :ov",
                "ExpressionAttributeValues": {":ov": {"N": str(old)}},
            }
        txn: list[dict] = [{"Put": {"TableName": self._t("executions"), "Item": self._raw(exe_item), **cas}}]
        for o in outbox:
            txn.append({"Put": {"TableName": self._t("outbox"), "Item": self._raw(o)}})
        for s in spawn:
            txn.append({"Put": {"TableName": self._t("spawns"), "Item": self._raw(s)}})
        if processed_event_id is not None:
            txn.append(
                {
                    "Put": {
                        "TableName": self._t("processed"),
                        "Item": self._raw({"execution_id": exe.id, "event_id": processed_event_id}),
                    }
                }
            )
        for op in timers:
            if op.action == "schedule":
                txn.append(
                    {
                        "Put": {
                            "TableName": self._t("timers"),
                            "Item": self._raw(
                                {"execution_id": exe.id, "path": op.path, "fire_at": Decimal(str(op.fire_at))}
                            ),
                        }
                    }
                )
            else:
                txn.append(
                    {
                        "Delete": {
                            "TableName": self._t("timers"),
                            "Key": self._raw({"execution_id": exe.id, "path": op.path}),
                        }
                    }
                )

        try:
            await self._db.transact_write_items(TransactItems=txn)
        except self._ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code not in ("TransactionCanceledException", "ConditionalCheckFailedException"):
                raise  # a real error, not a CAS miss
            exe.version = old  # undo the in-memory bump; the txn was cancelled
            resp = await self._db.get_item(
                TableName=self._t("executions"),
                Key=self._raw({"id": exe.id}),
                ProjectionExpression="version",
            )
            found = int(self._item(resp["Item"])["version"]) if "Item" in resp else None
            raise StoreConflict(exe.id, expected=old, found=found)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        resp = await self._db.get_item(
            TableName=self._t("processed"),
            Key=self._raw({"execution_id": execution_id, "event_id": event_id}),
        )
        return "Item" in resp

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._scan("outbox")
        rows.sort(key=lambda r: int(r["seq"]))  # Scan is unordered; sort by seq
        return [
            OutboxEntry(int(r["seq"]), r.get("target_id"), Event.model_validate_json(r["event"]))
            for r in rows
        ]

    async def ack_outbox(self, seq: int) -> None:
        await self._db.delete_item(TableName=self._t("outbox"), Key=self._raw({"seq": seq}))

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._scan("spawns")
        rows.sort(key=lambda r: int(r["seq"]))
        return [
            SpawnEntry(int(r["seq"]), r["parent_id"], r["child_id"], r["root_path"], json.loads(r["context"]))
            for r in rows
        ]

    async def ack_spawn(self, seq: int) -> None:
        await self._db.delete_item(TableName=self._t("spawns"), Key=self._raw({"seq": seq}))

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._scan(
            "timers",
            FilterExpression="fire_at <= :now",
            ExpressionAttributeValues={":now": {"N": str(now)}},
        )
        out = [(r["execution_id"], r["path"], float(r["fire_at"])) for r in rows]
        return sorted(out, key=lambda t: t[2])

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        from decimal import Decimal

        # guarded on the stored value: a concurrent re-schedule to a new time wins
        try:
            await self._db.delete_item(
                TableName=self._t("timers"),
                Key=self._raw({"execution_id": execution_id, "path": path}),
                ConditionExpression="fire_at = :f",
                ExpressionAttributeValues={":f": {"N": str(Decimal(str(fire_at)))}},
            )
        except self._ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise  # the guard didn't match (stale sweep) — a no-op, as intended

    async def close(self) -> None:
        # release only a client we own (created via create()); an injected client is the
        # caller's to close
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None


class AsyncRqliteStore:
    """Async mirror of `RqliteStore` over `httpx.AsyncClient`: the same guarded-upsert
    CAS (no interactive transactions — all writes in one transactional request, each
    side-write conditioned on the Execution row holding our exact `data`) with every
    HTTP call awaited. Build with `await AsyncRqliteStore.from_url(url)`."""

    def __init__(self, client: Any, base_url: str, timeout: float = 10.0) -> None:
        self._client = client
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    @classmethod
    async def from_url(
        cls,
        url: str,
        timeout: float = 10.0,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncRqliteStore":
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                store = cls(client, url, timeout)
                await store._execute(
                    [
                        "CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, "
                        "definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)",
                        "CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "target_id TEXT, event TEXT NOT NULL)",
                        "CREATE TABLE IF NOT EXISTS processed_events "
                        "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
                        "PRIMARY KEY (execution_id, event_id))",
                        "CREATE TABLE IF NOT EXISTS timers (execution_id TEXT NOT NULL, "
                        "path TEXT NOT NULL, fire_at REAL NOT NULL, "
                        "PRIMARY KEY (execution_id, path))",
                        "CREATE TABLE IF NOT EXISTS spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
                        "root_path TEXT NOT NULL, context TEXT NOT NULL)",
                    ]
                )
                return store
            except Exception as exc:  # noqa: BLE001
                await client.aclose()
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    async def _execute(self, statements: list, transaction: bool = False) -> list:
        params = {"transaction": ""} if transaction else {}
        resp = await self._client.post(
            f"{self._base}/db/execute", params=params, json=statements, timeout=self._timeout
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        for res in results:
            if "error" in res:
                raise RuntimeError(f"rqlite execute error: {res['error']}")
        return results

    async def _query(self, sql: str, params: tuple) -> list:
        resp = await self._client.post(
            f"{self._base}/db/query",
            params={"level": "strong"},
            json=[[sql, *params]],
            timeout=self._timeout,
        )
        resp.raise_for_status()
        result = resp.json()["results"][0]
        if "error" in result:
            raise RuntimeError(f"rqlite query error: {result['error']}")
        return result.get("values") or []

    async def load(self, execution_id: str) -> Optional[Execution]:
        rows = await self._query("SELECT data FROM executions WHERE id = ?", (execution_id,))
        return Execution.model_validate_json(rows[0][0]) if rows else None

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
        old = exe.version
        exe.version = old + 1
        new = exe.version
        data = exe.model_dump_json()
        statements: list = [
            [
                "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version "
                "WHERE executions.version = ?",
                exe.id,
                exe.definition_id,
                data,
                new,
                old,
            ]
        ]
        for target_id, event in emits:
            statements.append(
                [
                    "INSERT INTO outbox (target_id, event) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    target_id,
                    event.model_dump_json(),
                    exe.id,
                    data,
                ]
            )
        if processed_event_id is not None:
            statements.append(
                [
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    processed_event_id,
                    exe.id,
                    data,
                ]
            )
        for op in timers:
            statements.append(
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? "
                    "AND EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    op.path,
                    exe.id,
                    data,
                ]
            )
            if op.action == "schedule":
                statements.append(
                    [
                        "INSERT INTO timers (execution_id, path, fire_at) SELECT ?, ?, ? "
                        "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                        exe.id,
                        op.path,
                        op.fire_at,
                        exe.id,
                        data,
                    ]
                )
        for child_id, root_path, context in spawns:
            statements.append(
                [
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) SELECT ?, ?, ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    child_id,
                    root_path,
                    json.dumps(context),
                    exe.id,
                    data,
                ]
            )
        results = await self._execute(statements, transaction=True)
        if results[0].get("rows_affected", 0) == 0:
            exe.version = old
            found = await self._query("SELECT version FROM executions WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = await self._query(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._query("SELECT seq, target_id, event FROM outbox ORDER BY seq", ())
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        await self._execute([["DELETE FROM outbox WHERE seq = ?", seq]])

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._query(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq", ()
        )
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        await self._execute([["DELETE FROM spawns WHERE seq = ?", seq]])

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._query(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        await self._execute(
            [
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    async def close(self) -> None:
        await self._client.aclose()


class AsyncMongoStore:
    """Async mirror of `MongoStore` over `motor.motor_asyncio.AsyncIOMotorClient`:
    every collection method is awaited, cursors iterated with `async for`. Same
    single-document CAS (the whole Execution + its embedded outbox/spawns/timers lives
    in one document, so `update_one` with `version=old` filter is atomic without a
    replica set). Build with `await AsyncMongoStore.from_url(url)` or inject an
    already-connected `AsyncIOMotorClient`."""

    def __init__(self, client: Any, db_name: str = "harel") -> None:
        from pymongo import ReturnDocument
        from pymongo.errors import DuplicateKeyError

        self._client = client
        self._db = client[db_name]
        self._exes = self._db["executions"]
        self._counters = self._db["counters"]
        self._after = ReturnDocument.AFTER
        self._DuplicateKeyError = DuplicateKeyError

    @classmethod
    async def from_url(
        cls,
        url: str,
        db_name: str = "harel",
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncMongoStore":
        import anyio
        import motor.motor_asyncio
        from pymongo.errors import PyMongoError

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                client: Any = motor.motor_asyncio.AsyncIOMotorClient(url)
                await client.admin.command("ping")
                return cls(client, db_name)
            except PyMongoError as exc:
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("mongo connect failed")

    @staticmethod
    def _enc(path: str) -> str:
        return path.replace(".", "．")

    @staticmethod
    def _dec(key: str) -> str:
        return key.replace("．", ".")

    async def _next_seq(self, name: str, count: int) -> int:
        doc = await self._counters.find_one_and_update(
            {"_id": name}, {"$inc": {"n": count}}, upsert=True, return_document=self._after
        )
        return int(doc["n"]) - count + 1

    async def load(self, execution_id: str) -> Optional[Execution]:
        doc = await self._exes.find_one({"_id": execution_id}, {"data": 1})
        return Execution.model_validate_json(doc["data"]) if doc is not None else None

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
        outbox_entries: list[dict] = []
        if emits:
            base = await self._next_seq("outbox", len(emits))
            outbox_entries = [
                {"seq": base + i, "target_id": t, "event": e.model_dump_json()}
                for i, (t, e) in enumerate(emits)
            ]
        spawn_entries: list[dict] = []
        if spawns:
            base = await self._next_seq("spawn", len(spawns))
            spawn_entries = [
                {"seq": base + i, "parent_id": exe.id, "child_id": cid, "root_path": rp, "context": dict(ctx)}
                for i, (cid, rp, ctx) in enumerate(spawns)
            ]

        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()

        set_ops: dict[str, Any] = {"data": data, "version": exe.version}
        unset_ops: dict[str, str] = {}
        for op in timers:
            key = f"timers.{self._enc(op.path)}"
            if op.action == "schedule":
                set_ops[key] = op.fire_at
            else:
                unset_ops[key] = ""
        update: dict[str, Any] = {"$set": set_ops}
        push: dict[str, Any] = {}
        if outbox_entries:
            push["outbox"] = {"$each": outbox_entries}
        if spawn_entries:
            push["spawns"] = {"$each": spawn_entries}
        if push:
            update["$push"] = push
        if processed_event_id is not None:
            update["$addToSet"] = {"processed": processed_event_id}
        if unset_ops:
            update["$unset"] = unset_ops

        res = await self._exes.update_one({"_id": exe.id, "version": old}, update)
        if res.matched_count == 1:
            return  # CAS won

        existing = await self._exes.find_one({"_id": exe.id}, {"version": 1})
        if existing is None and old == 0:
            doc: dict[str, Any] = {
                "_id": exe.id,
                "definition_id": exe.definition_id,
                "version": exe.version,
                "data": data,
                "outbox": outbox_entries,
                "spawns": spawn_entries,
                "processed": [processed_event_id] if processed_event_id is not None else [],
                "timers": {self._enc(op.path): op.fire_at for op in timers if op.action == "schedule"},
            }
            try:
                await self._exes.insert_one(doc)
                return
            except self._DuplicateKeyError:
                existing = await self._exes.find_one({"_id": exe.id}, {"version": 1})
        exe.version = old
        raise StoreConflict(exe.id, expected=old, found=existing["version"] if existing else None)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (
            await self._exes.find_one({"_id": execution_id, "processed": event_id}, {"_id": 1})
        ) is not None

    async def pending_outbox(self) -> list[OutboxEntry]:
        entries: list[OutboxEntry] = []
        async for doc in self._exes.find({"outbox": {"$exists": True, "$ne": []}}, {"outbox": 1}):
            for e in doc.get("outbox", []):
                entries.append(OutboxEntry(e["seq"], e["target_id"], Event.model_validate_json(e["event"])))
        return sorted(entries, key=lambda e: e.seq)

    async def ack_outbox(self, seq: int) -> None:
        await self._exes.update_one({"outbox.seq": seq}, {"$pull": {"outbox": {"seq": seq}}})

    async def pending_spawns(self) -> list[SpawnEntry]:
        entries: list[SpawnEntry] = []
        async for doc in self._exes.find({"spawns": {"$exists": True, "$ne": []}}, {"spawns": 1}):
            for s in doc.get("spawns", []):
                entries.append(
                    SpawnEntry(s["seq"], s["parent_id"], s["child_id"], s["root_path"], dict(s["context"]))
                )
        return sorted(entries, key=lambda s: s.seq)

    async def ack_spawn(self, seq: int) -> None:
        await self._exes.update_one({"spawns.seq": seq}, {"$pull": {"spawns": {"seq": seq}}})

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        out: list[tuple[str, str, float]] = []
        async for doc in self._exes.find({"timers": {"$exists": True, "$ne": {}}}, {"timers": 1}):
            for enc, fire_at in (doc.get("timers") or {}).items():
                if fire_at <= now:
                    out.append((doc["_id"], self._dec(enc), float(fire_at)))
        return sorted(out, key=lambda t: t[2])

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        key = f"timers.{self._enc(path)}"
        await self._exes.update_one({"_id": execution_id, key: fire_at}, {"$unset": {key: ""}})

    async def close(self) -> None:
        self._client.close()
