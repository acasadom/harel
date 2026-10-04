"""AsyncSqliteTransport — an async Transport backend."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional

from harel.engine.schema import DEFAULT_PREFIX, Names, transport_schema
from harel.engine.transport import _PARKED, Lease
from harel.engine.transport._base import _SQLITE_CLAIM_SQL
from harel.spec.states import Event


class AsyncSqliteTransport:
    """Async mirror of `SqliteTransport` over `aiosqlite`. `claim` runs inside
    `BEGIN IMMEDIATE` so SQLite's global write-lock serializes claims (race-free per-group
    exclusivity with plain SQL); the lease (`lock_expiry`) recovers a crashed worker's
    message. Build with `await AsyncSqliteTransport.create(path)`.

    A SQLite transaction belongs to the connection, not to the coroutine: `_lock` makes each
    method exclusive on the connection, so one coroutine's `BEGIN IMMEDIATE` never lands
    inside (or rolls back) another's transaction."""

    def __init__(
        self, conn: Any, clock: Callable[[], float] = time.time, *, prefix: str = DEFAULT_PREFIX
    ) -> None:
        self._n = Names(prefix)
        self._conn = conn
        self._clock = clock
        self._lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        path: str = ":memory:",
        clock: Callable[[], float] = time.time,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "AsyncSqliteTransport":
        """Open `path`; `prefix` names its tables (see `harel.engine.schema`), and with
        `create_schema=False` they must already exist."""
        import aiosqlite

        conn = await aiosqlite.connect(str(path), isolation_level=None)  # autocommit; BEGIN by hand
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        if create_schema:
            for sql in transport_schema("sqlite", prefix):
                await conn.execute(sql)
        return cls(conn, clock, prefix=prefix)

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        async with self._lock:
            await self._conn.execute(
                f"INSERT INTO {self._n.messages} (group_id, event) VALUES (?, ?)",
                (group_id, event.model_dump_json()),
            )
            await self._conn.execute(
                f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority) VALUES (?, ?)",
                (group_id, priority),
            )

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        async with self._lock:
            return await self._claim_locked(worker_id, visibility, min_priority)

    async def _claim_locked(self, worker_id: str, visibility: float, min_priority: int) -> Optional[Lease]:
        now = self._clock()
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._conn.execute(
                self._n.sql(_SQLITE_CLAIM_SQL),
                (min_priority, now),
            )
            row = await cur.fetchone()
            if row is None:
                await self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            await self._conn.execute(
                f"UPDATE {self._n.groups} SET last_claimed_at = ? WHERE group_id = ?", (now, group_id)
            )
            await self._conn.execute(
                f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            await self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except BaseException:
            await self._conn.execute("ROLLBACK")
            raise

    async def ack(self, lease: Lease) -> None:
        async with self._lock:
            await self._conn.execute("BEGIN IMMEDIATE")
            try:
                await self._conn.execute(f"DELETE FROM {self._n.messages} WHERE seq = ?", (lease.seq,))
                await self._conn.execute(
                    f"DELETE FROM {self._n.groups} WHERE group_id = ? AND NOT EXISTS "
                    f"(SELECT 1 FROM {self._n.messages} WHERE group_id = ?)",
                    (lease.group_id, lease.group_id),
                )
                await self._conn.execute("COMMIT")
            except BaseException:
                await self._conn.execute("ROLLBACK")
                raise

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._lock:
            if delay > 0:
                await self._conn.execute(
                    f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                    (_PARKED, self._clock() + delay, lease.seq),
                )
            else:
                await self._conn.execute(
                    f"UPDATE {self._n.messages} SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?",
                    (lease.seq,),
                )

    async def close(self) -> None:
        async with self._lock:
            await self._conn.close()
