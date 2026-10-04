"""LibsqlStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Optional, Union

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.schema import DEFAULT_PREFIX, Names, store_schema
from harel.engine.store._base import (
    _IDS_WITH_PREFIX_SQL,
    _PURGE_COMPANIONS_SQL,
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _like_prefix,
    _listing_page,
    _listing_sql_sqlite,
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
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> None:
        """`prefix` names its tables (see `harel.engine.schema`); with `create_schema=False`
        they must already exist."""
        import libsql

        self._n = Names(prefix)

        kwargs: dict[str, Any] = {"_check_same_thread": False}
        if sync_url is not None:  # embedded replica against a Turso/sqld primary
            kwargs["sync_url"] = sync_url
            kwargs["auth_token"] = auth_token
            if sync_interval is not None:
                kwargs["sync_interval"] = sync_interval
        self._conn = libsql.connect(str(database), **kwargs)
        if create_schema:
            for sql in store_schema("sqlite", prefix):
                self._conn.execute(sql)
        self.trace_max = DEFAULT_TRACE_MAX
        self._conn.commit()

    def _write_trace(self, execution_id: str, entry: dict) -> None:
        """Append one trace step WITHOUT committing (batches into commit's txn). Two statements:
        `idx` computed inline (MAX+1, monotonic) so no pre-read, then the ring cap. `read_trace`
        takes `index` from the `idx` column."""
        self._conn.execute(
            f"INSERT INTO {self._n.trace} (execution_id, idx, entry) "
            f"SELECT ?, COALESCE((SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?), -1) + 1, ?",
            (execution_id, execution_id, json.dumps(entry)),
        )
        if self.trace_max:
            self._conn.execute(
                f"DELETE FROM {self._n.trace} WHERE execution_id = ? AND idx <= "
                f"(SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?) - ?",
                (execution_id, execution_id, self.trace_max),
            )

    def append_trace(self, execution_id: str, entry: dict) -> None:
        self._write_trace(execution_id, entry)
        self._conn.commit()

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._conn.execute(
            f"SELECT idx, entry FROM {self._n.trace} WHERE execution_id = ? ORDER BY idx", (execution_id,)
        ).fetchall()
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    def load(self, execution_id: str) -> Optional[Execution]:
        row = self._conn.execute(
            f"SELECT data FROM {self._n.executions} WHERE id = ?", (execution_id,)
        ).fetchone()
        return Execution.model_validate_json(row[0]) if row is not None else None

    def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one query (the worker's per-event pair)."""
        row = self._conn.execute(
            f"SELECT (SELECT data FROM {self._n.executions} WHERE id = ?), "
            f"EXISTS(SELECT 1 FROM {self._n.processed_events} WHERE execution_id = ? AND event_id = ?)",
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
        off = _decode_offset(cursor)
        sql, params = _listing_sql_sqlite(self._n.executions, status, definition_id, roots_only, limit, off)
        return _listing_page(self._conn.execute(sql, params).fetchall(), limit, off)

    def _write(self, exe: Execution) -> None:
        old = exe.version
        exe.version = old + 1
        data = exe.model_dump_json()
        cur = self._conn.execute(
            f"UPDATE {self._n.executions} SET data = ?, version = ? WHERE id = ? AND version = ?",
            (data, exe.version, exe.id, old),
        )
        if cur.rowcount == 0:
            found = self._conn.execute(
                f"SELECT version FROM {self._n.executions} WHERE id = ?", (exe.id,)
            ).fetchone()
            if found is None and old == 0:
                self._conn.execute(
                    f"INSERT INTO {self._n.executions} (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                    (exe.id, exe.definition_id, data, exe.version),
                )
            else:
                exe.version = old
                raise StoreConflict(exe.id, expected=old, found=found[0] if found else None)

    def save(self, exe: Execution) -> None:
        old = exe.version
        try:
            self._write(exe)
            self._conn.commit()
        except BaseException:
            self._conn.rollback()  # any failure: release the write lock, keep nothing
            exe.version = old
            raise

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> list[int]:
        old = exe.version
        try:
            self._write(exe)
            seqs = []
            for target_id, event in emits:
                cur = self._conn.execute(
                    f"INSERT INTO {self._n.outbox} (target_id, event) VALUES (?, ?)",
                    (target_id, event.model_dump_json()),
                )
                seqs.append(cur.lastrowid)
            if processed_event_id is not None:
                self._conn.execute(
                    f"INSERT OR IGNORE INTO {self._n.processed_events} (execution_id, event_id) VALUES (?, ?)",
                    (exe.id, processed_event_id),
                )
            for child_id, root_path, context in spawns:
                self._conn.execute(
                    f"INSERT INTO {self._n.spawns} (parent_id, child_id, root_path, context) VALUES (?, ?, ?, ?)",
                    (exe.id, child_id, root_path, json.dumps(context)),
                )
            for op in timers:
                if op.action == "schedule":
                    self._conn.execute(
                        f"INSERT INTO {self._n.timers} (execution_id, path, fire_at) VALUES (?, ?, ?) "
                        "ON CONFLICT(execution_id, path) DO UPDATE SET fire_at = excluded.fire_at",
                        (exe.id, op.path, op.fire_at),
                    )
                else:
                    self._conn.execute(
                        f"DELETE FROM {self._n.timers} WHERE execution_id = ? AND path = ?", (exe.id, op.path)
                    )
            if trace is not None:
                self._write_trace(exe.id, trace)
            self._conn.commit()
            return seqs
        except BaseException:
            self._conn.rollback()  # any failure discards the whole batch and its write lock
            exe.version = old
            raise

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        row = self._conn.execute(
            f"SELECT 1 FROM {self._n.processed_events} WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        ).fetchone()
        return row is not None

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._conn.execute(
            f"SELECT seq, target_id, event FROM {self._n.outbox} ORDER BY seq"
        ).fetchall()
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._conn.execute(f"DELETE FROM {self._n.outbox} WHERE seq = ?", (seq,))
        self._conn.commit()

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._conn.execute(
            f"SELECT seq, parent_id, child_id, root_path, context FROM {self._n.spawns} ORDER BY seq"
        ).fetchall()
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._conn.execute(f"DELETE FROM {self._n.spawns} WHERE seq = ?", (seq,))
        self._conn.commit()

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._conn.execute(
            f"SELECT execution_id, path, fire_at FROM {self._n.timers} WHERE fire_at <= ? ORDER BY fire_at",
            (now,),
        ).fetchall()
        return [(eid, path, fa) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._conn.execute(
            f"DELETE FROM {self._n.timers} WHERE execution_id = ? AND path = ? AND fire_at = ?",
            (execution_id, path, fire_at),
        )
        self._conn.commit()

    def ids_with_prefix(self, prefix: str) -> list[str]:
        return [
            r[0]
            for r in self._conn.execute(self._n.sql(_IDS_WITH_PREFIX_SQL), (_like_prefix(prefix),)).fetchall()
        ]

    def purge(self, execution_id: str, expected_version: int) -> bool:
        try:
            deleted = self._conn.execute(
                f"DELETE FROM {self._n.executions} WHERE id = ? AND version = ?",
                (execution_id, expected_version),
            ).rowcount
            if (
                not deleted
                and self._conn.execute(
                    f"SELECT 1 FROM {self._n.executions} WHERE id = ?", (execution_id,)
                ).fetchone()
            ):
                self._conn.rollback()
                return False  # moved on: touch nothing
            for sql in map(self._n.sql, _PURGE_COMPANIONS_SQL):
                self._conn.execute(sql, (execution_id,))
            self._conn.commit()
            return bool(deleted)
        except BaseException:
            self._conn.rollback()
            raise

    def close(self) -> None:
        self._conn.close()
