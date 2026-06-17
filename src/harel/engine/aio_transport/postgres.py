"""AsyncPostgresTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport import _PG_ACK_FN, _PG_CLAIM_FN, Lease
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
                # serialize concurrent schema setup: `CREATE OR REPLACE FUNCTION` rewrites pg_proc
                # and several workers opening at once would collide ("tuple concurrently updated")
                await cur.execute("SELECT pg_advisory_xact_lock(7723019)")
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
                await cur.execute(_PG_CLAIM_FN)  # server-side claim (one round-trip)
                await cur.execute(_PG_ACK_FN)  # server-side ack (one round-trip)
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
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one round-trip: the function leases the lowest claimable group (FOR UPDATE SKIP LOCKED,
        # already race-free), drops stale empty groups, and returns its head — all server-side.
        async with self._pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT group_id, seq, event FROM harel_claim(%s, %s, %s)",
                        (now, now + visibility, token),
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
                await cur.execute("SELECT harel_ack(%s, %s, %s)", (lease.group_id, lease.seq, lease.token))
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
