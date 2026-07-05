"""SqliteTransport — a Transport backend."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Callable, Optional, Union

from harel.engine.transport._base import _PARKED, Lease
from harel.spec.states import Event


class SqliteTransport:
    """Durable `Transport` over SQLite. `claim` runs inside `BEGIN IMMEDIATE`, so
    SQLite's global write-lock serializes claims across processes — the per-group
    exclusivity selection is then race-free with plain SQL (no row/advisory
    locks). One connection per thread/process on the same file (WAL mode); the
    lease (`lock_expiry`) recovers a message a crashed worker was holding.

    Round-robin fairness: a `groups` table tracks `last_claimed_at` per group.
    `claim` picks the group with the oldest `last_claimed_at` (0 = never claimed),
    updating it immediately so the group moves to the back of the queue."""

    def __init__(self, path: Union[str, Path] = ":memory:", clock: Callable[[], float] = time.time) -> None:
        # isolation_level=None -> autocommit; we drive BEGIN IMMEDIATE/COMMIT by hand in claim.
        self._conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait for the write-lock instead of erroring
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS messages "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, event TEXT NOT NULL, "
            "locked_by TEXT, lock_expiry REAL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS groups "
            "(group_id TEXT PRIMARY KEY, last_claimed_at REAL NOT NULL DEFAULT 0.0)"
        )
        self._clock = clock

    def publish(self, group_id: str, event: Event) -> None:
        self._conn.execute(
            "INSERT INTO messages (group_id, event) VALUES (?, ?)",
            (group_id, event.model_dump_json()),
        )
        self._conn.execute("INSERT OR IGNORE INTO groups (group_id) VALUES (?)", (group_id,))

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                "SELECT m.seq, m.group_id, m.event FROM messages m "
                "JOIN groups g ON g.group_id = m.group_id "
                "WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                "AND m.group_id NOT IN ("
                "  SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                ") ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                self._conn.execute("COMMIT")
                return None
            seq, group_id, event = row
            self._conn.execute(
                "UPDATE groups SET last_claimed_at = ? WHERE group_id = ?", (now, group_id)
            )
            self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (worker_id, now + visibility, seq),
            )
            self._conn.execute("COMMIT")
            return Lease(seq, group_id, Event.model_validate_json(event))
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def ack(self, lease: Lease) -> None:
        self._conn.execute("DELETE FROM messages WHERE seq = ?", (lease.seq,))
        self._conn.execute(
            "DELETE FROM groups WHERE group_id = ? AND NOT EXISTS "
            "(SELECT 1 FROM messages WHERE group_id = ?)",
            (lease.group_id, lease.group_id),
        )

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            self._conn.execute(
                "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                (_PARKED, self._clock() + delay, lease.seq),
            )
        else:
            self._conn.execute(
                "UPDATE messages SET locked_by = NULL, lock_expiry = NULL WHERE seq = ?",
                (lease.seq,),
            )

    def close(self) -> None:
        self._conn.close()
