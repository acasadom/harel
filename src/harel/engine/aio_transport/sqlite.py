"""AsyncSqliteTransport — an async Transport backend."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional

from harel.engine._sqlite import aenable_wal
from harel.engine.schema import DEFAULT_PREFIX, Names, sqlite_transport_upgrade, transport_schema
from harel.engine.transport import _PARKED, Backlog, Lease
from harel.engine.transport._base import _SQLITE_BACKLOG_SQL, _backlog, _sqlite_claim
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
        await aenable_wal(conn)
        if create_schema:
            for sql in transport_schema("sqlite", prefix):
                if not sql.startswith("CREATE TABLE"):
                    continue
                await conn.execute(sql)
            names, existing = Names(prefix), {}
            for table in ("groups", "messages"):
                cur = await conn.execute(f"PRAGMA table_info({getattr(names, table)})")
                existing[table] = [row[1] for row in await cur.fetchall()]
            for sql in sqlite_transport_upgrade(existing, prefix):
                await conn.execute(sql)
            for sql in transport_schema("sqlite", prefix):
                if not sql.startswith("CREATE TABLE"):
                    await conn.execute(sql)
        return cls(conn, clock, prefix=prefix)

    async def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        async with self._lock:
            await self._conn.execute(
                f"INSERT INTO {self._n.messages} (group_id, event, published_at) VALUES (?, ?, ?)",
                (group_id, event.model_dump_json(), self._clock()),
            )
            await self._conn.execute(
                f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority, routing_key) VALUES (?, ?, ?)",
                (group_id, priority, routing_key),
            )

    async def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        async with self._lock:
            return await self._claim_locked(worker_id, visibility, min_priority, routing_keys)

    async def _claim_locked(
        self, worker_id: str, visibility: float, min_priority: int, routing_keys: Optional[frozenset[str]]
    ) -> Optional[Lease]:
        now = self._clock()
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._conn.execute(*_sqlite_claim(self._n, min_priority, now, routing_keys))
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

    async def backlog(self) -> Backlog:
        async with self._lock:
            cur = await self._conn.execute(self._n.sql(_SQLITE_BACKLOG_SQL), (self._clock(),))
            return _backlog(await cur.fetchall())

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
