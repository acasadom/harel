"""SqliteStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Union

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


class SqliteStore:
    """A durable `ExecutionStore` over SQLite (stdlib): each Execution is stored
    as JSON keyed by id, committed on every save. A fresh `SqliteStore` on the
    same file reads the committed state — so a run survives a process restart and
    resumes. `:memory:` gives a non-persistent variant for tests."""

    def __init__(self, path: Union[str, Path] = ":memory:") -> None:
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the single writer
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait for the write-lock instead of erroring
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS executions "
            "(id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, data TEXT NOT NULL, "
            "version INTEGER NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox "
            "(seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, event TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS processed_events "
            "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, event_id))"
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
        # PREVIEW: execution trace for the monitor timeline (not yet engine-written)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS trace "
            "(execution_id TEXT NOT NULL, idx INTEGER NOT NULL, entry TEXT NOT NULL, "
            "PRIMARY KEY (execution_id, idx))"
        )
        self._conn.commit()

    def append_trace(self, execution_id: str, entry: dict) -> None:
        """PREVIEW seam (see DictStore): append a trace step for the monitor timeline."""
        (count,) = self._conn.execute(
            "SELECT COUNT(*) FROM trace WHERE execution_id = ?", (execution_id,)
        ).fetchone()
        idx = entry.get("index", count)
        self._conn.execute(
            "INSERT OR REPLACE INTO trace (execution_id, idx, entry) VALUES (?, ?, ?)",
            (execution_id, idx, json.dumps({**entry, "index": idx})),
        )
        self._conn.commit()

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._conn.execute(
            "SELECT entry FROM trace WHERE execution_id = ? ORDER BY idx", (execution_id,)
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._conn.execute("SELECT data FROM executions WHERE id = ?", (execution_id,)).fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # project only the scalar summary fields out of the JSON blob (never pull `data`);
        # status/outcome/active_path/parent_id live inside it, so json_extract reaches them.
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
            (*params, limit + 1, off),  # fetch one extra to know if there's a next page
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
        """The CAS write of `exe`, WITHOUT committing the transaction (so it can
        be batched atomically with outbox inserts in `commit`)."""
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = self._conn.execute(
            "UPDATE executions SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            # no row matched `old`: either a brand-new Execution (no row yet) or a
            # stale write (the row moved past `old`). Distinguish by existence.
            found = self._conn.execute("SELECT version FROM executions WHERE id = ?", (exe.id,)).fetchone()
            if found is None and old == 0:
                self._conn.execute(
                    "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old  # undo the in-memory bump; the commit did not happen
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

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._conn.execute("SELECT seq, target_id, event FROM outbox ORDER BY seq").fetchall()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._conn.execute("DELETE FROM outbox WHERE seq = ?", (seq,))
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
