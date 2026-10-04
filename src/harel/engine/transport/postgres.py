"""PostgresTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.schema import DEFAULT_PREFIX, Names, transport_schema
from harel.engine.store._base import _PG_SCHEMA_LOCK
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

    The connection is injected (duck-typed), so `psycopg` is an optional extra. `prefix` names
    its tables and functions (see `harel.engine.schema`); with `create_schema=False` they must
    already exist."""

    def __init__(
        self,
        conn: Any,
        clock: Callable[[], float] = time.time,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> None:
        self._n = Names(prefix)
        self._conn = conn
        self._clock = clock
        if create_schema:
            with conn.cursor() as cur:
                # serialize concurrent schema setup: `CREATE OR REPLACE FUNCTION` rewrites pg_proc
                # and several connections opening at once would collide ("tuple concurrently updated")
                cur.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_SCHEMA_LOCK,))
                for sql in transport_schema("postgres", prefix):
                    cur.execute(sql)
            conn.commit()

    @classmethod
    def from_dsn(
        cls,
        dsn: str,
        connect_retries: int = 15,
        retry_delay: float = 1.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "PostgresTransport":
        import time as _time

        import psycopg

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(psycopg.connect(dsn), prefix=prefix, create_schema=create_schema)
            except psycopg.OperationalError as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("postgres connect failed")

    def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self._n.messages} (group_id, event) VALUES (%s, %s)",
                (group_id, event.model_dump_json()),
            )
            # ready the group iff new — a publish into an in-flight or parked group must not
            # reset its lease or priority, so the update changes nothing. It does lock the row,
            # which the ack function locks too: a concurrent ack can't decide the group is
            # drained and delete it under this message (see `schema`)
            cur.execute(
                f"INSERT INTO {self._n.groups} (group_id, locked_by, lock_expiry, priority) "
                "VALUES (%s, NULL, NULL, %s) "
                f"ON CONFLICT (group_id) DO UPDATE SET priority = {self._n.groups}.priority",
                (group_id, priority),
            )
        self._conn.commit()

    def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        # one round-trip: the function leases the lowest claimable group (FOR UPDATE SKIP LOCKED,
        # already race-free), drops stale empty groups, and returns its head — all server-side.
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT group_id, seq, event FROM {self._n.claim}(%s, %s, %s, %s)",
                    (now, now + visibility, token, min_priority),
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
            cur.execute(
                f"SELECT {self._n.ack}(%s, %s, %s, %s)",
                (lease.group_id, lease.seq, lease.token, self._clock()),
            )
        self._conn.commit()

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._conn.cursor() as cur:
            if delay > 0:
                # park: keep the token so the still-present head isn't re-claimed until `delay` passes
                cur.execute(
                    f"UPDATE {self._n.groups} SET lock_expiry = %s WHERE group_id = %s AND locked_by = %s",
                    (self._clock() + delay, lease.group_id, lease.token),
                )
            else:
                cur.execute(
                    f"UPDATE {self._n.groups} SET locked_by = NULL, lock_expiry = NULL "
                    "WHERE group_id = %s AND locked_by = %s",
                    (lease.group_id, lease.token),
                )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
