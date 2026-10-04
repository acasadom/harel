"""`ConnectionStore` — an `ExecutionStore` over a connection the caller owns.

harel's own `SqliteStore` opens its connection and commits every write itself: a durable
checkpoint of its own. This store writes through the caller's `sqlite3` connection and never
commits or rolls back — the caller's transaction does. So the machine's advance commits, or
rolls back, together with the caller's own writes.

It is a whole store written outside harel: the protocol and helpers from
`harel.engine.store.base`, the table names and the schema from `harel.engine.schema`, and
checked against the same contracts harel runs on its own stores (`harel.testing`).
"""

from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.schema import DEFAULT_PREFIX, Names
from harel.engine.store.base import (
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    decode_offset,
    encode_offset,
    like_prefix,
    matches,
)
from harel.spec.states import Event


class ConnectionStore:
    """An `ExecutionStore` on the caller's connection, inside the caller's transactions.
    The schema is the caller's too: apply `harel.engine.schema.store_schema("sqlite")`."""

    def __init__(self, conn: sqlite3.Connection, *, prefix: str = DEFAULT_PREFIX) -> None:
        self._conn = conn
        self._n = Names(prefix)
        self.trace_max = DEFAULT_TRACE_MAX

    def _q(self, template: str, params: tuple = ()) -> sqlite3.Cursor:
        return self._conn.execute(self._n.sql(template), params)

    # --- the execution ---------------------------------------------------------------------
    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._q("SELECT data FROM {executions} WHERE id = ?", (execution_id,)).fetchone()
        return Execution.model_validate_json(row[0]) if row else None

    def _write(self, exe: Execution) -> None:
        old, new = exe.version, exe.version + 1
        exe.version = new
        data = exe.model_dump_json()
        updated = self._q(
            "UPDATE {executions} SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, new, exe.id, old),
        ).rowcount
        if updated:
            return
        found = self._q("SELECT version FROM {executions} WHERE id = ?", (exe.id,)).fetchone()
        if found is None and old == 0:
            self._q("INSERT INTO {executions} VALUES (?, ?, ?, ?)", (exe.id, exe.definition_id, data, new))
            return
        exe.version = old
        raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    def save(self, exe: Execution) -> None:
        self._write(exe)

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> list[int]:
        """Every write of one step, on the caller's connection — atomic because they share
        the caller's transaction, which this store never ends."""
        self._write(exe)
        seqs = []
        for target_id, event in emits:
            cur = self._q(
                "INSERT INTO {outbox} (target_id, event) VALUES (?, ?)", (target_id, event.model_dump_json())
            )
            seqs.append(cur.lastrowid)
        if processed_event_id is not None:
            self._q("INSERT OR IGNORE INTO {processed_events} VALUES (?, ?)", (exe.id, processed_event_id))
        for child_id, root_path, context in spawns:
            self._q(
                "INSERT INTO {spawns} (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                (exe.id, child_id, root_path, json.dumps(context)),
            )
        for op in timers:
            self._q("DELETE FROM {timers} WHERE execution_id = ? AND path = ?", (exe.id, op.path))
            if op.action == "schedule":
                self._q("INSERT INTO {timers} VALUES (?, ?, ?)", (exe.id, op.path, op.fire_at))
        if trace is not None:
            last = self._q("SELECT MAX(idx) FROM {trace} WHERE execution_id = ?", (exe.id,)).fetchone()[0]
            idx = 0 if last is None else last + 1
            self._q("INSERT INTO {trace} VALUES (?, ?, ?)", (exe.id, idx, json.dumps(trace)))
            self._q("DELETE FROM {trace} WHERE execution_id = ? AND idx <= ?", (exe.id, idx - self.trace_max))
        return seqs

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        return (
            self._q(
                "SELECT 1 FROM {processed_events} WHERE execution_id = ? AND event_id = ?",
                (execution_id, event_id),
            ).fetchone()
            is not None
        )

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        statuses = set(status) if status is not None else None
        summaries = [
            ExecutionSummary(
                id=e.id,
                definition_id=e.definition_id,
                version=e.version,
                status=e.status,
                outcome=e.outcome,
                active_path=e.active_path,
                parent_id=e.parent_id,
                finished_at=e.finished_at,
            )
            for e in (
                Execution.model_validate_json(r[0])
                for r in self._q("SELECT data FROM {executions} ORDER BY id")
            )
        ]
        found = [s for s in summaries if matches(s, statuses, definition_id, roots_only)]
        off = decode_offset(cursor)
        more = len(found) > off + limit
        return ExecutionPage(
            items=found[off : off + limit], next_cursor=encode_offset(off + limit) if more else None
        )

    def ids_with_prefix(self, prefix: str) -> list[str]:
        return [
            r[0]
            for r in self._q(
                "SELECT id FROM {executions} WHERE id LIKE ? ESCAPE '\\'", (like_prefix(prefix),)
            )
        ]

    # --- deferred work -----------------------------------------------------------------------
    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._q("SELECT seq, target_id, event FROM {outbox} ORDER BY seq")
        return [OutboxEntry(seq, target, Event.model_validate_json(event)) for seq, target, event in rows]

    def ack_outbox(self, seq: int) -> None:
        self._q("DELETE FROM {outbox} WHERE seq = ?", (seq,))

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._q("SELECT seq, parent_id, child_id, root_path, context FROM {spawns} ORDER BY seq")
        return [
            SpawnEntry(seq, parent, child, root, json.loads(ctx)) for seq, parent, child, root, ctx in rows
        ]

    def ack_spawn(self, seq: int) -> None:
        self._q("DELETE FROM {spawns} WHERE seq = ?", (seq,))

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._q(
            "SELECT execution_id, path, fire_at FROM {timers} WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, fire_at) for eid, path, fire_at in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._q(
            "DELETE FROM {timers} WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._q("SELECT idx, entry FROM {trace} WHERE execution_id = ? ORDER BY idx", (execution_id,))
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    # --- retention ---------------------------------------------------------------------------
    def purge(self, execution_id: str, expected_version: int) -> bool:
        deleted = self._q(
            "DELETE FROM {executions} WHERE id = ? AND version = ?", (execution_id, expected_version)
        ).rowcount
        if not deleted and self.load(execution_id) is not None:
            return False  # moved on: touch nothing
        for table, column in (
            ("processed_events", "execution_id"),
            ("trace", "execution_id"),
            ("timers", "execution_id"),
            ("outbox", "target_id"),
            ("spawns", "parent_id"),
        ):
            self._q(f"DELETE FROM {{{table}}} WHERE {column} = ?", (execution_id,))
        return bool(deleted)

    def close(self) -> None:
        """The connection is the caller's: nothing to release."""
