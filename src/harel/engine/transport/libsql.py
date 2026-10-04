"""LibsqlTransport — a Transport backend."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Optional, Union

from harel.engine.schema import DEFAULT_PREFIX, Names, transport_schema
from harel.engine.transport._base import _PARKED, _SQLITE_CLAIM_SQL, Lease
from harel.spec.states import Event


class LibsqlTransport:
    """Durable `Transport` over **libSQL** (Turso's SQLite fork) via the `libsql` package.
    **EXPERIMENTAL** (local-file path tested in-process; the Turso/`sqld` path is wired but
    unvalidated against a real account). SQLite-compatible, so identical to `SqliteTransport`: `claim` runs inside `BEGIN IMMEDIATE`
    so the write-lock serializes claims (race-free per-group exclusivity, no row/advisory
    locks), and `lock_expiry` is the lease. The connection is a local file, or an embedded
    replica against a Turso/`sqld` primary (`sync_url` + `auth_token`). `libsql` is synchronous;
    the async worker reaches it through `AsyncLibsqlTransport`."""

    def __init__(
        self,
        database: Union[str, Path] = ":memory:",
        clock: Callable[[], float] = time.time,
        *,
        auth_token: str = "",
        sync_url: Optional[str] = None,
        sync_interval: Optional[float] = None,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> None:
        """`prefix` names its tables (see `harel.engine.schema`); with `create_schema=False`
        they must already exist."""
        import libsql

        self._n = Names(prefix)

        kwargs: dict[str, Any] = {"isolation_level": None, "_check_same_thread": False}
        if sync_url is not None:
            kwargs["sync_url"] = sync_url
            kwargs["auth_token"] = auth_token
            if sync_interval is not None:
                kwargs["sync_interval"] = sync_interval
        self._conn = libsql.connect(str(database), **kwargs)
        if create_schema:
            for sql in transport_schema("sqlite", prefix):
                self._conn.execute(sql)
        self._clock = clock

    def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        self._conn.execute(
            f"INSERT INTO {self._n.messages} (group_id, event) VALUES (?, ?)",
            (group_id, event.model_dump_json()),
        )
        self._conn.execute(
            f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority) VALUES (?, ?)", (group_id, priority)
        )

    def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                self._n.sql(_SQLITE_CLAIM_SQL),
                (min_priority, now),
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            self._conn.execute(
                f"UPDATE {self._n.groups} SET last_claimed_at = ? WHERE group_id = ?", (now, group_id)
            )
            self._conn.execute(
                f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def ack(self, lease: Lease) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(f"DELETE FROM {self._n.messages} WHERE seq = ?", (lease.seq,))
            self._conn.execute(
                f"DELETE FROM {self._n.groups} WHERE group_id = ? AND NOT EXISTS "
                f"(SELECT 1 FROM {self._n.messages} WHERE group_id = ?)",
                (lease.group_id, lease.group_id),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            self._conn.execute(
                f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (_PARKED, self._clock() + delay, lease.seq),
            )
        else:
            self._conn.execute(
                f"UPDATE {self._n.messages} SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?",
                (lease.seq,),
            )

    def close(self) -> None:
        self._conn.close()
