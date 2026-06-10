"""PostgresTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport._base import Lease
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
        try:
            with self._conn.cursor() as cur:
                while True:
                    token = f"{worker_id}:{uuid.uuid4().hex}"
                    # lease one claimable group; FOR UPDATE SKIP LOCKED locks that group row so a
                    # concurrent claimer skips it and takes a different group (parallel, no global lock)
                    cur.execute(
                        "UPDATE transport_groups SET locked_by = %s, lock_expiry = %s WHERE group_id = ("
                        "  SELECT group_id FROM transport_groups "
                        "  WHERE locked_by IS NULL OR lock_expiry < %s "
                        "  ORDER BY group_id FOR UPDATE SKIP LOCKED LIMIT 1"
                        ") RETURNING group_id",
                        (token, now + visibility, now),
                    )
                    grow = cur.fetchone()
                    if grow is None:
                        self._conn.commit()
                        return None
                    group_id = grow[0]
                    cur.execute(
                        "SELECT seq, event FROM transport_messages WHERE group_id = %s ORDER BY seq LIMIT 1",
                        (group_id,),
                    )
                    head = cur.fetchone()
                    if head is None:
                        # drained group (last message already acked): drop its row and keep looking
                        cur.execute(
                            "DELETE FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                            (group_id, token),
                        )
                        continue
                    self._conn.commit()
                    return Lease(head[0], group_id, Event.model_validate_json(head[1]), token=token)
        except Exception:
            self._conn.rollback()
            raise

    def ack(self, lease: Lease) -> None:
        with self._conn.cursor() as cur:
            # fence: only the current lease holder removes the head + frees the group
            cur.execute(
                "SELECT 1 FROM transport_groups WHERE group_id = %s AND locked_by = %s",
                (lease.group_id, lease.token),
            )
            if cur.fetchone() is not None:
                cur.execute("DELETE FROM transport_messages WHERE seq = %s", (lease.seq,))
                cur.execute(
                    "UPDATE transport_groups SET locked_by = NULL, lock_expiry = NULL "
                    "WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
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
