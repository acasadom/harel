"""AsyncPostgresTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.schema import DEFAULT_PREFIX, Names, transport_schema
from harel.engine.store._base import _PG_SCHEMA_LOCK
from harel.engine.transport import Lease
from harel.spec.states import Event


class AsyncPostgresTransport:
    """Async mirror of `PostgresTransport` over `psycopg_pool.AsyncConnectionPool`: per-group
    exclusivity is a per-group row in `transport_groups` (lease = `locked_by` token +
    `lock_expiry`), and `claim` leases a claimable group with `SELECT … FOR UPDATE SKIP LOCKED`
    so concurrent workers lease *different* groups in parallel — no global lock serializing
    claims (the old `pg_advisory_xact_lock` made the transport a one-claim-at-a-time bottleneck).
    Each method checks out a pool connection. Build with
    `await AsyncPostgresTransport.from_dsn(dsn, pool_size=N)`."""

    def __init__(
        self, pool: Any, clock: Callable[[], float] = time.time, *, prefix: str = DEFAULT_PREFIX
    ) -> None:
        self._n = Names(prefix)
        self._pool = pool
        self._clock = clock

    @classmethod
    async def from_dsn(
        cls,
        dsn: str,
        clock: Callable[[], float] = time.time,
        pool_size: int = 10,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "AsyncPostgresTransport":
        """Open a pool on `dsn`; `prefix` names its tables and functions (see
        `harel.engine.schema`), and with `create_schema=False` they must already exist."""
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        if create_schema:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # serialize concurrent schema setup: `CREATE OR REPLACE FUNCTION` rewrites
                    # pg_proc and several workers opening at once would collide
                    await cur.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_SCHEMA_LOCK,))
                    for sql in transport_schema("postgres", prefix):
                        await cur.execute(sql)
                await conn.commit()
        return cls(pool, clock, prefix=prefix)

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {self._n.messages} (group_id, event) VALUES (%s, %s)",
                    (group_id, event.model_dump_json()),
                )
                # ready the group iff new; the no-op update locks its row, as the ack
                # function does, so a concurrent ack can't delete it under this message
                await cur.execute(
                    f"INSERT INTO {self._n.groups} (group_id, locked_by, lock_expiry, priority) "
                    "VALUES (%s, NULL, NULL, %s) "
                    f"ON CONFLICT (group_id) DO UPDATE SET priority = {self._n.groups}.priority",
                    (group_id, priority),
                )
            await conn.commit()

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one round-trip: the function leases the lowest claimable group (FOR UPDATE SKIP LOCKED,
        # already race-free), drops stale empty groups, and returns its head — all server-side.
        async with self._pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT group_id, seq, event FROM {self._n.claim}(%s, %s, %s, %s)",
                        (now, now + visibility, token, min_priority),
                    )
                    row = await cur.fetchone()
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        if row is None:
            return None
        return Lease(row[1], row[0], Event.model_validate_json(row[2]), token=token)

    async def ack(self, lease: Lease) -> None:
        # one round-trip: the function fences on the token, deletes the head, frees the lock
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT {self._n.ack}(%s, %s, %s, %s)",
                    (lease.group_id, lease.seq, lease.token, self._clock()),
                )
            await conn.commit()

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                if delay > 0:
                    await cur.execute(
                        f"UPDATE {self._n.groups} SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                        (self._clock() + delay, lease.group_id, lease.token),
                    )
                else:
                    await cur.execute(
                        f"UPDATE {self._n.groups} SET locked_by = NULL, lock_expiry = NULL "
                        "WHERE group_id = %s AND locked_by = %s",
                        (lease.group_id, lease.token),
                    )
            await conn.commit()

    async def close(self) -> None:
        await self._pool.close()
