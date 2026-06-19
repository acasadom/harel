"""AsyncPostgresStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import _PG_COMMIT_FN, _PG_SCHEMA_LOCK, DEFAULT_TRACE_MAX
from harel.spec.states import Event


class AsyncPostgresStore:
    """Async mirror of `PostgresStore` over `psycopg_pool.AsyncConnectionPool`: version-CAS via
    UPDATE-WHERE-version (Postgres row-locks serialize writers — one wins rowcount 1, the loser
    rowcount 0 raises StoreConflict). Each method checks out a connection from the pool for the
    duration of one transaction, so concurrent workers make real parallel DB requests. Build with
    `await AsyncPostgresStore.from_dsn(dsn, pool_size=N)`."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    async def from_dsn(cls, dsn: str, pool_size: int = 10) -> "AsyncPostgresStore":
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                # serialize concurrent schema setup (CREATE OR REPLACE FUNCTION rewrites pg_proc)
                await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_SCHEMA_LOCK,))
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
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS trace "
                    "(execution_id TEXT NOT NULL, idx INT NOT NULL, entry TEXT NOT NULL, "
                    "PRIMARY KEY (execution_id, idx))"
                )
                await cur.execute(_PG_COMMIT_FN)  # the state-only commit fast path (one round-trip)
            await conn.commit()
        return cls(pool)

    async def _write_trace(self, cur: Any, execution_id: str, entry: dict) -> None:
        """Append one trace step on the given cursor (inside commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        await cur.execute(
            "INSERT INTO trace (execution_id, idx, entry) "
            "SELECT %s, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = %s), -1) + 1, %s",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            await cur.execute(
                "DELETE FROM trace WHERE execution_id = %s AND idx <= "
                "(SELECT MAX(idx) FROM trace WHERE execution_id = %s) - %s",
                (execution_id, execution_id, self.trace_max),
            )

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await self._write_trace(cur, execution_id, entry)
            await conn.commit()

    async def read_trace(self, execution_id: str) -> list[dict]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT idx, entry FROM trace WHERE execution_id = %s ORDER BY idx", (execution_id,)
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    async def load(self, execution_id: str) -> Optional[Execution]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT data FROM executions WHERE id = %s", (execution_id,))
                row = await cur.fetchone()
            await conn.commit()
        return Execution.model_validate_json(row[0]) if row is not None else None

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load the Execution and whether `event_id` is already processed in **one** round-trip
        (the worker's per-event dedupe check, folded into the load instead of a second query)."""
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT e.data, EXISTS(SELECT 1 FROM processed_events p "
                    "WHERE p.execution_id = %s AND p.event_id = %s) "
                    "FROM executions e WHERE e.id = %s",
                    (execution_id, event_id, execution_id),
                )
                row = await cur.fetchone()
            await conn.commit()
        if row is None:
            return None, False
        return Execution.model_validate_json(row[0]), bool(row[1])

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
        # fast path: a state-only event (no emits/spawns/timers/trace) commits in ONE atomic
        # round-trip via the version-CAS function — instead of UPDATE + (SELECT/INSERT) + INSERT.
        if not emits and not spawns and not timers and trace is None:
            await self._commit_cas(exe, processed_event_id)
            return
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
                        if trace is not None:
                            await self._write_trace(cur, exe.id, trace)
                    await conn.commit()
                except StoreConflict:
                    raise
                except Exception:
                    exe.version = old
                    await conn.rollback()
                    raise
        except StoreConflict:
            raise

    async def _commit_cas(self, exe: Execution, processed_event_id: Optional[str]) -> None:
        """The fast-path commit: version-CAS + write (+ dedupe) in one atomic Lua-style round-trip
        via `harel_commit_cas`. Returns false on a version conflict (no RAISE, so the txn is clean)."""
        old = exe.version
        exe.version = old + 1
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT harel_commit_cas(%s, %s, %s, %s, %s)",
                    (exe.id, exe.definition_id, exe.model_dump_json(), old, processed_event_id or ""),
                )
                ok = (await cur.fetchone())[0]
            if not ok:
                exe.version = old
                await conn.rollback()
                raise StoreConflict(exe.id, expected=old, found=None)
            await conn.commit()

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
