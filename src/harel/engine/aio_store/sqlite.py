"""AsyncSqliteStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import DEFAULT_TRACE_MAX
from harel.spec.states import Event


class AsyncSqliteStore:
    """Async mirror of `SqliteStore` over `aiosqlite`: each Execution stored as JSON keyed
    by id, version-CAS via UPDATE-WHERE-version, the whole `commit` one atomic transaction.
    aiosqlite serializes a connection's ops on its own worker thread, so the multi-statement
    commit stays atomic. Build with `await AsyncSqliteStore.create(path)` (the connection must
    be awaited open); `:memory:` is a non-persistent variant for tests."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.trace_max = DEFAULT_TRACE_MAX

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
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS trace "
            "(execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, idx))"
        )
        await conn.commit()
        return cls(conn)

    async def _write_trace(self, execution_id: str, entry: dict) -> None:
        """Append one trace step WITHOUT committing (batches into commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        await self._conn.execute(
            "INSERT INTO trace (execution_id, idx, entry) "
            "SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ?",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            await self._conn.execute(
                "DELETE FROM trace WHERE execution_id = ? AND idx <= "
                "(SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ?",
                (execution_id, execution_id, self.trace_max),
            )

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        await self._write_trace(execution_id, entry)
        await self._conn.commit()

    async def read_trace(self, execution_id: str) -> list[dict]:
        cur = await self._conn.execute(
            "SELECT idx, entry FROM trace WHERE execution_id = ? ORDER BY idx", (execution_id,)
        )
        return [{**json.loads(entry), "index": idx} for idx, entry in await cur.fetchall()]

    async def load(self, execution_id: str) -> Optional[Execution]:
        cur = await self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,))
        row = await cur.fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one round-trip (the worker's per-event pair)."""
        cur = await self._conn.execute(
            "SELECT (SELECT data FROM executions WHERE id = ?), "
            "EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?)",
            (execution_id, execution_id, event_id),
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None, False
        return Execution.model_validate_json(row[0]), bool(row[1])

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
        trace: Optional[dict] = None,
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
            if trace is not None:
                await self._write_trace(exe.id, trace)
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
