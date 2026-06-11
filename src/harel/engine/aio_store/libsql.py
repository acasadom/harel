"""AsyncLibsqlStore — an async ExecutionStore backend."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, TimerOp
from harel.spec.states import Event


class AsyncLibsqlStore:
    """Async store over **libSQL** (Turso's SQLite fork). **EXPERIMENTAL** (local-file path
    tested in-process; the Turso/`sqld` path is wired but unvalidated against a real account).
    The `libsql` package is a synchronous
    sqlite3-style driver, so this wraps the sync `LibsqlStore` and off-loads each call to a
    thread (`asyncio.to_thread`) — not blocking the event loop — serialized by a lock (one libSQL
    connection, used one op at a time, which suits this single-writer-class backend). `file:`
    local for tests, or an embedded replica / Turso via `sync_url` + `auth_token`. Build with
    `await AsyncLibsqlStore.create(database, sync_url=..., auth_token=...)`."""

    def __init__(self, sync_store: Any) -> None:
        self._s = sync_store
        self._lock = asyncio.Lock()

    @property
    def trace_max(self) -> int:
        return self._s.trace_max

    @trace_max.setter
    def trace_max(self, value: int) -> None:
        self._s.trace_max = value

    @classmethod
    async def create(
        cls,
        database: str = ":memory:",
        *,
        auth_token: str = "",
        sync_url: Optional[str] = None,
        sync_interval: Optional[float] = None,
    ) -> "AsyncLibsqlStore":
        from harel.engine.store import LibsqlStore

        sync = await asyncio.to_thread(
            LibsqlStore, database, auth_token=auth_token, sync_url=sync_url, sync_interval=sync_interval
        )
        return cls(sync)

    async def load(self, execution_id: str) -> Optional[Execution]:
        async with self._lock:
            return await asyncio.to_thread(self._s.load, execution_id)

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        async with self._lock:
            return await asyncio.to_thread(self._s.load_for_event, execution_id, event_id)

    async def save(self, exe: Execution) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.save, exe)

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._s.commit,
                exe,
                emits,
                processed_event_id=processed_event_id,
                timers=timers,
                spawns=spawns,
                trace=trace,
            )

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.append_trace, execution_id, entry)

    async def read_trace(self, execution_id: str) -> list[dict]:
        async with self._lock:
            return await asyncio.to_thread(self._s.read_trace, execution_id)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._s.is_processed, execution_id, event_id)

    async def pending_outbox(self) -> list[OutboxEntry]:
        async with self._lock:
            return await asyncio.to_thread(self._s.pending_outbox)

    async def ack_outbox(self, seq: int) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.ack_outbox, seq)

    async def pending_spawns(self) -> list[SpawnEntry]:
        async with self._lock:
            return await asyncio.to_thread(self._s.pending_spawns)

    async def ack_spawn(self, seq: int) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.ack_spawn, seq)

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        async with self._lock:
            return await asyncio.to_thread(self._s.due_timers, now)

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.delete_timer, execution_id, path, fire_at)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._s.close)
