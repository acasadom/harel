"""AsyncPostgresTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

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

    def __init__(self, pool: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._pool = pool
        self._clock = clock

    @classmethod
    async def from_dsn(
        cls, dsn: str, prefix: str = "stm", clock: Callable[[], float] = time.time, pool_size: int = 10
    ) -> "AsyncPostgresTransport":
        from psycopg_pool import AsyncConnectionPool

        pool = AsyncConnectionPool(conninfo=dsn, min_size=1, max_size=pool_size, open=False)
        await pool.open()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS transport_messages "
                    "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS transport_messages_group ON transport_messages (group_id, seq)"
                )
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS transport_groups "
                    "(group_id TEXT PRIMARY KEY, locked_by TEXT, lock_expiry DOUBLE PRECISION)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS transport_groups_claimable ON transport_groups (lock_expiry)"
                )
            await conn.commit()
        return cls(pool, prefix, clock)

    async def publish(self, group_id: str, event: Event) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
                    (group_id, event.model_dump_json()),
                )
                await cur.execute(
                    "INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL) "
                    "ON CONFLICT (group_id) DO NOTHING",
                    (group_id,),
                )
            await conn.commit()

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        async with self._pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    while True:
                        token = f"{worker_id}:{uuid.uuid4().hex}"
                        # FOR UPDATE SKIP LOCKED: a concurrent claimer skips this group's row and
                        # leases a different group — parallel claims, no global serialization
                        await cur.execute(
                            "UPDATE transport_groups SET locked_by = %s, lock_expiry = %s WHERE group_id = ("
                            "  SELECT group_id FROM transport_groups "
                            "  WHERE locked_by IS NULL OR lock_expiry < %s "
                            "  ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1"
                            ") RETURNING group_id",
                            (token, now + visibility, now),
                        )
                        grow = await cur.fetchone()
                        if grow is None:
                            await conn.commit()
                            return None
                        group_id = grow[0]
                        await cur.execute(
                            "SELECT seq, event FROM transport_messages WHERE group_id = %s ORDER BY seq LIMIT 1",
                            (group_id,),
                        )
                        head = await cur.fetchone()
                        if head is None:
                            await cur.execute(
                                "DELETE FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                                (group_id, token),
                            )
                            continue
                        await conn.commit()
                        return Lease(head[0], group_id, Event.model_validate_json(head[1]), token=token)
            except Exception:
                await conn.rollback()
                raise

    async def ack(self, lease: Lease) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
                if await cur.fetchone() is not None:
                    await cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
                    await cur.execute(
                        "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                        "WHERE group_id = %s AND locked_by = %s",
                        (lease.group_id, lease.token),
                    )
            await conn.commit()

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                if delay > 0:
                    await cur.execute(
                        "UPDATE transport_groups SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                        (self._clock() + delay, lease.group_id, lease.token),
                    )
                else:
                    await cur.execute(
                        "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                        "WHERE group_id = %s AND locked_by = %s",
                        (lease.group_id, lease.token),
                    )
            await conn.commit()

    async def close(self) -> None:
        await self._pool.close()
