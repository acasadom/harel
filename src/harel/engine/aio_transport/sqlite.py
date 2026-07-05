"""AsyncSqliteTransport — an async Transport backend."""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

from harel.engine.transport import _PARKED, Lease
from harel.spec.states import Event


class AsyncSqliteTransport:
    """Async mirror of `SqliteTransport` over `aiosqlite`. `claim` runs inside
    `BEGIN IMMEDIATE` so SQLite's global write-lock serializes claims (race-free per-group
    exclusivity with plain SQL); the lease (`lock_expiry`) recovers a crashed worker's
    message. Build with `await AsyncSqliteTransport.create(path)`."""

    def __init__(self, conn: Any, clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._clock = clock

    @classmethod
    async def create(
        cls, path: str = ":memory:", clock: Callable[[], float] = time.time
    ) -> "AsyncSqliteTransport":
        import aiosqlite

        conn = await aiosqlite.connect(str(path), isolation_level=None)  # autocommit; BEGIN by hand
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS messages "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, "
            "locked_by TEXT, lock_expiry REAL)"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS groups "
            "(group_id TEXT PRIMARY KEY, last_claimed_at REAL NOT NULL DEFAULT 0.0, "
            "priority INT NOT NULL DEFAULT 0)"
        )
        await conn.execute("INSERT OR IGNORE INTO groups (group_id) SELECT DISTINCT group_id FROM messages")
        return cls(conn, clock)

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        await self._conn.execute(
            "INSERT INTO messages (group_id, event) VALUES (?, ?)", (group_id, event.model_dump_json())
        )
        await self._conn.execute(
            "INSERT OR IGNORE INTO groups (group_id, priority) VALUES (?, ?)", (group_id, priority)
        )

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._conn.execute(
                "SELECT m.seq, m.group_id, m.event FROM messages m "
                "JOIN groups g ON g.group_id = m.group_id "
                "WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                "AND m.group_id NOT IN ("
                "  SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                ") AND g.priority >= ?"
                " ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1",
                (now, now, min_priority),
            )
            row = await cur.fetchone()
            if row is None:
                await self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            await self._conn.execute(
                "UPDATE groups SET last_claimed_at = ? WHERE group_id = ?", (now, group_id)
            )
            await self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            await self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except Exception:
            await self._conn.execute("ROLLBACK")
            raise

    async def ack(self, lease: Lease) -> None:
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            await self._conn.execute("DELETE FROM messages WHERE seq = ?", (lease.seq,))
            await self._conn.execute(
                "DELETE FROM groups WHERE group_id = ? AND NOT EXISTS "
                "(SELECT 1 FROM messages WHERE group_id = ?)",
                (lease.group_id, lease.group_id),
            )
            await self._conn.execute("COMMIT")
        except Exception:
            await self._conn.execute("ROLLBACK")
            raise

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            await self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (_PARKED, self._clock() + delay, lease.seq),
            )
        else:
            await self._conn.execute(
                "UPDATE messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?", (lease.seq,)
            )

    async def close(self) -> None:
        await self._conn.close()
