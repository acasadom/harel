"""AsyncPostgresStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.schema import DEFAULT_PREFIX, Names, store_schema
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import (
    _IDS_WITH_PREFIX_SQL,
    _PG_SCHEMA_LOCK,
    _PURGE_COMPANIONS_SQL,
    DEFAULT_TRACE_MAX,
    Step,
    _decode_offset,
    _like_prefix,
    _listing_page,
    _listing_sql_pg,
)
from harel.spec.states import Event


class AsyncPostgresStore:
    """Async mirror of `PostgresStore` over `psycopg_pool.AsyncConnectionPool`: version-CAS via
    UPDATE-WHERE-version (Postgres row-locks serialize writers — one wins rowcount 1, the loser
    rowcount 0 raises StoreConflict). Each method checks out a connection from the pool for the
    duration of one transaction, so concurrent workers make real parallel DB requests. Build with
    `await AsyncPostgresStore.from_dsn(dsn, pool_size=N)`."""

    def __init__(self, pool: Any, *, prefix: str = DEFAULT_PREFIX) -> None:
        self._n = Names(prefix)
        self._pool = pool
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    async def from_dsn(
        cls, dsn: str, pool_size: int = 10, *, prefix: str = DEFAULT_PREFIX, create_schema: bool = True
    ) -> "AsyncPostgresStore":
        """Open a pool on `dsn`; `prefix` names its tables and functions (see
        `harel.engine.schema`), and with `create_schema=False` they must already exist."""
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        if create_schema:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # serialize concurrent schema setup (CREATE OR REPLACE FUNCTION rewrites pg_proc)
                    await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_SCHEMA_LOCK,))
                    for sql in store_schema("postgres", prefix):
                        await cur.execute(sql)
                await conn.commit()
        return cls(pool, prefix=prefix)

    async def _write_trace(self, cur: Any, execution_id: str, entry: dict) -> None:
        """Append one trace step on the given cursor (inside commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        await cur.execute(
            f"INSERT INTO {self._n.trace} (execution_id, idx, entry) "
            f"SELECT %s, COALESCE((SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = %s), -1) + 1, %s",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            await cur.execute(
                f"DELETE FROM {self._n.trace} WHERE execution_id = %s AND idx <= "
                f"(SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = %s) - %s",
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
                    f"SELECT idx, entry FROM {self._n.trace} WHERE execution_id = %s ORDER BY idx",
                    (execution_id,),
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    async def load(self, execution_id: str) -> Optional[Execution]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT data FROM {self._n.executions} WHERE id = %s", (execution_id,))
                row = await cur.fetchone()
            await conn.commit()
        return Execution.model_validate_json(row[0]) if row is not None else None

    async def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        """See `PostgresStore.list_executions`."""
        off = _decode_offset(cursor)
        sql, params = _listing_sql_pg(self._n.executions, status, definition_id, roots_only, limit, off)
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                rows = await cur.fetchall()
            await conn.commit()  # end the read transaction
        return _listing_page(rows, limit, off)

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load the Execution and whether `event_id` is already processed in **one** round-trip
        (the worker's per-event dedupe check, folded into the load instead of a second query)."""
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT e.data, EXISTS(SELECT 1 FROM {self._n.processed_events} p "
                    "WHERE p.execution_id = %s AND p.event_id = %s) "
                    f"FROM {self._n.executions} e WHERE e.id = %s",
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
        step: Optional[Step] = None,
    ) -> list[int]:
        # fast path: a state-only event (no emits/spawns/timers/trace) commits in ONE atomic
        # round-trip via the version-CAS function — instead of UPDATE + (SELECT/INSERT) + INSERT.
        if not emits and not spawns and not timers and trace is None:
            await self._commit_cas(exe, processed_event_id)
            return []
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        try:
            async with self._pool.connection() as conn:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            f"UPDATE {self._n.executions} SET data = %s, version = %s WHERE id = %s AND version = %s",
                            (data, exe.version, exe.id, old),
                        )
                        if cur.rowcount == 0:
                            await cur.execute(
                                f"SELECT version FROM {self._n.executions} WHERE id = %s", (exe.id,)
                            )
                            row = await cur.fetchone()
                            if row is None and old == 0:
                                await cur.execute(
                                    f"INSERT INTO {self._n.executions} (id, definition_id, data, version) "
                                    "VALUES (%s, %s, %s, %s)",
                                    (exe.id, exe.definition_id, data, exe.version),
                                )
                            else:
                                exe.version = old
                                await conn.rollback()
                                raise StoreConflict(exe.id, expected=old, found=row[0] if row else None)
                        seqs = []
                        for target_id, event in emits:
                            await cur.execute(
                                f"INSERT INTO {self._n.outbox} (target_id, event) VALUES (%s, %s) RETURNING seq",
                                (target_id, event.model_dump_json()),
                            )
                            row = await cur.fetchone()
                            seqs.append(row[0])
                        if processed_event_id is not None:
                            await cur.execute(
                                f"INSERT INTO {self._n.processed_events} (execution_id, event_id) VALUES (%s, %s) "
                                "ON CONFLICT DO NOTHING",
                                (exe.id, processed_event_id),
                            )
                        for child_id, root_path, context in spawns:
                            await cur.execute(
                                f"INSERT INTO {self._n.spawns} (parent_id, child_id, root_path, context) "
                                "VALUES (%s, %s, %s, %s)",
                                (exe.id, child_id, root_path, json.dumps(context)),
                            )
                        for op in timers:
                            if op.action == "schedule":
                                await cur.execute(
                                    f"INSERT INTO {self._n.timers} (execution_id, path, fire_at) VALUES (%s, %s, %s) "
                                    "ON CONFLICT (execution_id, path) DO UPDATE SET fire_at = EXCLUDED.fire_at",
                                    (exe.id, op.path, op.fire_at),
                                )
                            else:
                                await cur.execute(
                                    f"DELETE FROM {self._n.timers} WHERE execution_id = %s AND path = %s",
                                    (exe.id, op.path),
                                )
                        if trace is not None:
                            await self._write_trace(cur, exe.id, trace)
                    await conn.commit()
                    return seqs
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
        via the `commit_cas` function (`schema.Names.commit_cas`). Returns false on a version conflict (no RAISE, so the txn is clean)."""
        old = exe.version
        exe.version = old + 1
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT {self._n.commit_cas}(%s, %s, %s, %s, %s)",
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
                    f"SELECT 1 FROM {self._n.processed_events} WHERE execution_id = %s AND event_id = %s",
                    (execution_id, event_id),
                )
                found = await cur.fetchone() is not None
            await conn.commit()
        return found

    async def pending_outbox(self) -> list[OutboxEntry]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT seq, target_id, event FROM {self._n.outbox} ORDER BY seq")
                rows = await cur.fetchall()
            await conn.commit()
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"DELETE FROM {self._n.outbox} WHERE seq = %s", (seq,))
            await conn.commit()

    async def pending_spawns(self) -> list[SpawnEntry]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT seq, parent_id, child_id, root_path, context FROM {self._n.spawns} ORDER BY seq"
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"DELETE FROM {self._n.spawns} WHERE seq = %s", (seq,))
            await conn.commit()

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT execution_id, path, fire_at FROM {self._n.timers} WHERE fire_at <= %s ORDER BY fire_at",
                    (now,),
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._n.timers} WHERE execution_id = %s AND path = %s AND fire_at = %s",
                    (execution_id, path, fire_at),
                )
            await conn.commit()

    async def ids_with_prefix(self, prefix: str) -> list[str]:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    self._n.sql(_IDS_WITH_PREFIX_SQL).replace("?", "%s"), (_like_prefix(prefix),)
                )
                rows = await cur.fetchall()
            await conn.commit()
        return [r[0] for r in rows]

    async def purge(self, execution_id: str, expected_version: int) -> bool:
        # an exception leaves the transaction to the pool, which rolls back a returned connection
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._n.executions} WHERE id = %s AND version = %s",
                    (execution_id, expected_version),
                )
                deleted = cur.rowcount
                if not deleted:
                    await cur.execute(f"SELECT 1 FROM {self._n.executions} WHERE id = %s", (execution_id,))
                    if await cur.fetchone() is not None:
                        await conn.rollback()
                        return False  # moved on: touch nothing
                for sql in map(self._n.sql, _PURGE_COMPANIONS_SQL):
                    await cur.execute(sql.replace("?", "%s"), (execution_id,))
            await conn.commit()
        return bool(deleted)

    async def close(self) -> None:
        await self._pool.close()
