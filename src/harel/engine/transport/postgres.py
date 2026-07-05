"""PostgresTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport._base import _PG_ACK_FN, _PG_CLAIM_FN, Lease
from harel.spec.states import Event


class PostgresTransport:
    """`Transport` over PostgreSQL — a multi-machine queue with no Redis (the classic
    DB-as-queue). `transport_messages` is the FIFO; per-group exclusivity is a **per-group
    row** in `transport_groups` carrying the lease (`locked_by` token + `lock_expiry`).

    `claim` leases a claimable group with **`SELECT … FOR UPDATE SKIP LOCKED`**: Postgres's
    row lock makes the per-group selection race-free, and SKIP LOCKED lets concurrent workers
    lease *different* groups in parallel — so claims do not serialize. (The previous design
    took a single global `pg_advisory_xact_lock` to serialize every claim, which made the
    whole transport a bottleneck — one claim at a time regardless of worker count. This is the
    same per-group + SKIP LOCKED approach DBOS uses for its Postgres queue.) Lease times are
    the client clock (epoch float). A claimed group's head message is returned but not removed;
    `ack` removes it and frees the group (fenced by the lease token); `nack` frees it now or
    parks it for `delay`.

    The connection is injected (duck-typed), so `psycopg` is an optional extra. `prefix` is
    accepted for API compatibility (the table names are fixed)."""

    def __init__(self, conn: Any, prefix: str = "stm", clock: Callable[[], float] = time.time) -> None:
        self._conn = conn
        self._clock = clock
        with conn.cursor() as cur:
            # serialize concurrent schema setup: `CREATE OR REPLACE FUNCTION` rewrites pg_proc and
            # several connections opening at once would collide ("tuple concurrently updated")
            cur.execute("SELECT pg_advisory_xact_lock(7723019)")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_messages "
                "(seq BIGSERIAL PRIMARY KEY, group_id TEXT NOT NULL, event TEXT NOT NULL)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS transport_messages_group ON transport_messages (group_id, seq)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS transport_groups "
                "(group_id TEXT PRIMARY KEY, locked_by TEXT, lock_expiry DOUBLE PRECISION)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS transport_groups_claimable ON transport_groups (lock_expiry)"
            )
            cur.execute(_PG_CLAIM_FN)  # server-side claim (one round-trip)
            cur.execute(_PG_ACK_FN)  # server-side ack (one round-trip)
        conn.commit()

    @classmethod
    def from_dsn(cls, dsn: str, connect_retries: int = 15, retry_delay: float = 1.0) -> "PostgresTransport":
        import time as _time

        import psycopg

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(psycopg.connect(dsn))
            except psycopg.OperationalError as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("postgres connect failed")

    def publish(self, group_id: str, event: Event) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transport_messages (group_id, event) VALUES (%s, %s)",
                (group_id, event.model_dump_json()),
            )
            # ready the group iff new (ON CONFLICT DO NOTHING) — a publish into an in-flight or
            # parked group must not reset its lease
            cur.execute(
                "INSERT INTO transport_groups (group_id, locked_by, lock_expiry) VALUES (%s, NULL, NULL) "
                "ON CONFLICT (group_id) DO NOTHING",
                (group_id,),
            )
        self._conn.commit()

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one round-trip: the function leases the lowest claimable group (FOR UPDATE SKIP LOCKED,
        # already race-free), drops stale empty groups, and returns its head — all server-side.
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT group_id, seq, event FROM harel_claim(%s, %s, %s)",
                    (now, now + visibility, token),
                )
                row = cur.fetchone()
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        if row is None:
            return None
        return Lease(row[1], row[0], Event.model_validate_json(row[2]), token=token)

    def ack(self, lease: Lease) -> None:
        # one round-trip: the function fences on the token, deletes the head, frees the lock
        with self._conn.cursor() as cur:
            cur.execute("SELECT harel_ack(%s, %s, %s, %s)", (lease.group_id, lease.seq, lease.token, self._clock()))
        self._conn.commit()

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._conn.cursor() as cur:
            if delay > 0:
                # park: keep the token so the still-present head isn't re-claimed until `delay` passes
                cur.execute(
                    "UPDATE transport_groups SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                    (self._clock() + delay, lease.group_id, lease.token),
                )
            else:
                cur.execute(
                    "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                    "WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
