"""AsyncLibsqlTransport — an async Transport backend."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from harel.engine.transport import Lease
from harel.spec.states import Event


class AsyncLibsqlTransport:
    """Async transport over **libSQL**. **EXPERIMENTAL** (local-file path tested in-process; the
    Turso/`sqld` path is wired but unvalidated against a real account). Wraps the synchronous
    `LibsqlTransport` (the `libsql`
    package is sqlite3-style/sync), off-loading each call to a thread, serialized by a lock (one
    connection, one op at a time; the `BEGIN IMMEDIATE` claim is single-writer anyway). `file:`
    local, or an embedded replica / Turso via `sync_url` + `auth_token`. Build with
    `await AsyncLibsqlTransport.create(database, sync_url=..., auth_token=...)`."""

    def __init__(self, sync_transport: Any) -> None:
        self._t = sync_transport
        self._lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        database: str = ":memory:",
        *,
        auth_token: str = "",
        sync_url: Optional[str] = None,
        sync_interval: Optional[float] = None,
    ) -> "AsyncLibsqlTransport":
        from harel.engine.transport import LibsqlTransport

        sync = await asyncio.to_thread(
            LibsqlTransport,
            database,
            auth_token=auth_token,
            sync_url=sync_url,
            sync_interval=sync_interval,
        )
        return cls(sync)

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._lock:
            await asyncio.to_thread(self._t.publish, group_id, event)

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        async with self._lock:
            return await asyncio.to_thread(self._t.claim, worker_id, visibility)

    async def ack(self, lease: Lease) -> None:
        async with self._lock:
            await asyncio.to_thread(self._t.ack, lease)

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._lock:
            await asyncio.to_thread(self._t.nack, lease, delay)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._t.close)
