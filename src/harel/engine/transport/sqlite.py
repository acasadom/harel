"""SqliteTransport — a Transport backend."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Callable, Optional, Union

from harel.engine.schema import DEFAULT_PREFIX, Names
from harel.engine.transport._base import _PARKED, Lease, _sqlite_claim, _sqlite_create_schema
from harel.spec.states import Event


class SqliteTransport:
    """Durable `Transport` over SQLite. `claim` runs inside `BEGIN IMMEDIATE`, so
    SQLite's global write-lock serializes claims across processes — the per-group
    exclusivity selection is then race-free with plain SQL (no row/advisory
    locks). One connection per thread/process on the same file (WAL mode); the
    lease (`lock_expiry`) recovers a message a crashed worker was holding.

    Round-robin fairness: the groups table tracks `last_claimed_at` per group.
    `claim` picks the group with the oldest `last_claimed_at` (0 = never claimed),
    updating it immediately so the group moves to the back of the queue."""

    def __init__(
        self,
        path: Union[str, Path] = ":memory:",
        clock: Callable[[], float] = time.time,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> None:
        """`prefix` names its tables (see `harel.engine.schema`); with `create_schema=False`
        they must already exist."""
        self._n = Names(prefix)
        # isolation_level=None -> autocommit; we drive BEGIN IMMEDIATE/COMMIT by hand in claim.
        self._conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait for the write-lock instead of erroring
        if create_schema:
            _sqlite_create_schema(prefix, self._conn.execute, self._group_columns)
        self._clock = clock

    def _group_columns(self) -> list[str]:
        return [row[1] for row in self._conn.execute(f"PRAGMA table_info({self._n.groups})").fetchall()]

    def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        self._conn.execute(
            f"INSERT INTO {self._n.messages} (group_id, event) VALUES (?, ?)",
            (group_id, event.model_dump_json()),
        )
        self._conn.execute(
            f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority, routing_key) VALUES (?, ?, ?)",
            (group_id, priority, routing_key),
        )

    def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        now = self._clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(*_sqlite_claim(self._n, min_priority, now, routing_keys)).fetchone()
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
