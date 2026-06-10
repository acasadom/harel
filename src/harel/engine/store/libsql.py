"""LibsqlStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _encode_offset,
)
from harel.spec.states import Event


class LibsqlStore:
    """Durable `ExecutionStore` over **libSQL** (Turso's SQLite fork) via the `libsql`
    package — SQLite-compatible (DB-API), so the SQL, the version-CAS and the one-transaction
    `commit` are identical to `SqliteStore`.

    **EXPERIMENTAL**: the local-file path is covered in-process by the test suite; the Turso/
    `sqld` embedded-replica path (``sync_url``) is wired but not yet validated against a real
    Turso account, and its primary-follower replication is eventually consistent (read from the
    primary for CAS, or expect extra `StoreConflict` retries). The connection adapts by argument:

    - a local file (``LibsqlStore("state.db")``) — like SQLite;
    - an **embedded replica** (``sync_url=`` + ``auth_token=``) — local reads from the file,
      writes routed to the Turso/`sqld` primary and synced back;
    - so the same backend is a single-file embed AND a distributed (Turso/`sqld`) store.

    `libsql` is synchronous (a `sqlite3` driver); the async worker reaches it through
    `AsyncLibsqlStore`, which off-loads to a thread. `:memory:` is the test variant."""

    def __init__(
        self,
        database: Union[str, Path] = ":memory:",
        *,
        auth_token: str = "",
        sync_url: Optional[str] = None,
        sync_interval: Optional[float] = None,
    ) -> None:
        import libsql

        kwargs: dict[str, Any] = {"_check_same_thread": False}
        if sync_url is not None:  # embedded replica against a Turso/sqld primary
            kwargs["sync_url"] = sync_url
            kwargs["auth_token"] = auth_token
            if sync_interval is not None:
                kwargs["sync_interval"] = sync_interval
        self._conn = libsql.connect(str(database), **kwargs)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS executions "
            "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, PRIMARY KEY (execution_id, event_id))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS timers "
            "(execution_id TEXT NOT NULL, path TEXT NOT NULL, fire_at REAL NOT NULL, "
            "PRIMARY KEY (execution_id, path))"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS spawns "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
            "root_path TEXT NOT NULL, context TEXT NOT NULL)"
        )
        self._conn.commit()

    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one query (the worker's per-event pair)."""
        row = self._conn.execute(
            "SELECT (SELECT data FROM executions WHERE id = ?), "
            "EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?)",
            (execution_id, execution_id, event_id),
        ).fetchone()
        if row is None or row[0] is None:
            return None, False
        return Execution.model_validate_json(row[0]), bool(row[1])

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        where, params = ["1=1"], []
        if definition_id is not None:
            where.append("definition_id = ?")
            params.append(definition_id)
        if status is not None:
            statuses = [s.value for s in status]
            where.append(f"json_extract(data,'$.status') IN ({','.join('?' * len(statuses))})")
            params += statuses
        if roots_only:
            where.append("json_extract(data,'$.parent_id') IS NULL")
        off = _decode_offset(cursor)
        rows = self._conn.execute(
            "SELECT id, definition_id, version, json_extract(data,'$.status'), "
            "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
            "json_extract(data,'$.parent_id') FROM executions "
            f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit + 1, off),
        ).fetchall()
        items = [
            ExecutionSummary(
                id=r[0],
                definition_id=r[1],
                version=r[2],
                status=r[3],
                outcome=r[4],
                active_path=r[5],
                parent_id=r[6],
            )
            for r in rows[:limit]
        ]
        nxt = _encode_offset(off + limit) if len(rows) > limit else None
        return ExecutionPage(items=items, next_cursor=nxt)

    def _write(self, exe: Execution) -> None:
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = self._conn.execute(
            "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            found = self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,)).fetchone()
            if found is None and old == 0:
                self._conn.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    def save(self, exe: Execution) -> None:
        try:
            self._write(exe)
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
    ) -> None:
        try:
            self._write(exe)
            for target_id, event in emits:
                self._conn.execute(
                    "INSERT INTO outbox (target_id, event) VALUES (?, ?)",
                    (target_id, event.model_dump_json()),
                )
            if processed_event_id is not None:
                self._conn.execute(
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) VALUES (?, ?)",
                    (exe.id, processed_event_id),
                )
            for child_id, root_path, context in spawns:
                self._conn.execute(
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                    (exe.id, child_id, root_path, json.dumps(context)),
                )
            for op in timers:
                if op.action == "schedule":
                    self._conn.execute(
                        "INSERT INTO timers (execution_id, path, fire_at) VALUES (?, ?, ?) "
                        "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                        (exe.id, op.path, op.fire_at),
                    )
                else:
                    self._conn.execute(
                        "DELETE FROM timers WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                    )
            self._conn.commit()
        except StoreConflict:
            self._conn.rollback()
            raise

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        ).fetchone()
        return row is not None

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq").fetchall()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
        self._conn.commit()

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._conn.execute(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq"
        ).fetchall()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._conn.execute("DELETE FROM spawns WHERE seq = ?", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._conn.execute(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        ).fetchall()
        return [(eid, path, fa) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._conn.execute(
            "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
