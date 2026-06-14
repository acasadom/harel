"""RqliteStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, ExecutionSummary, Status
from harel.engine.store._base import (
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _encode_offset,
)
from harel.spec.states import Event


class RqliteStore:
    """A durable `ExecutionStore` over **rqlite** — distributed SQLite with Raft
    (HA, strong reads), spoken over its HTTP API. Same contract as SqliteStore.

    rqlite has no interactive (multi-roundtrip) transactions, so `commit` is one
    transactional request whose writes are all **guarded on the CAS succeeding**:
    the Execution upsert applies only `WHERE version = old`, and each outbox/dedupe
    insert runs only `WHERE EXISTS(... version = new)`. So a version mismatch makes
    the whole request a no-op, detected by the upsert's `rows_affected == 0`
    (→ `StoreConflict`). Reads use `level=strong` (linearizable, via the leader)."""

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        import requests

        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._execute(
            [
                "CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, definition_id TEXT NOT NULL, "
                "data TEXT NOT NULL, version INTEGER NOT NULL)",
                "CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, target_id TEXT, "
                "event TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS processed_events (execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
                "PRIMARY KEY (execution_id, event_id))",
                "CREATE TABLE IF NOT EXISTS timers (execution_id TEXT NOT NULL, path TEXT NOT NULL, "
                "fire_at REAL NOT NULL, PRIMARY KEY (execution_id, path))",
                "CREATE TABLE IF NOT EXISTS spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                "parent_id TEXT NOT NULL, child_id TEXT NOT NULL, root_path TEXT NOT NULL, context TEXT NOT NULL)",
                "CREATE TABLE IF NOT EXISTS trace (execution_id TEXT NOT NULL, idx INTEGER NOT NULL, "
                "entry TEXT NOT NULL, PRIMARY KEY (execution_id, idx))",
            ]
        )
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    def from_url(cls, url: str, connect_retries: int = 30, retry_delay: float = 1.0) -> "RqliteStore":
        """Build a store, retrying until rqlite is up and has elected a leader (a
        worker starting alongside rqlite in compose waits rather than crashing)."""
        import time

        import requests

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                return cls(url)
            except requests.exceptions.RequestException as exc:
                last = exc
                time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    def _execute(self, statements: list, transaction: bool = False) -> list:
        params = {"transaction": ""} if transaction else {}
        resp = self._session.post(
            f"{self._base}/db/execute", params=params, json=statements, timeout=self._timeout
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        for res in results:
            if "error" in res:
                raise RuntimeError(f"rqlite execute error: {res['error']}")
        return results

    def _query(self, sql: str, params: tuple) -> list:
        resp = self._session.post(
            f"{self._base}/db/query", params={"level": "strong"}, json=[[sql, *params]], timeout=self._timeout
        )
        resp.raise_for_status()
        result = resp.json()["results"][0]
        if "error" in result:
            raise RuntimeError(f"rqlite query error: {result['error']}")
        return result.get("values") or []

    def load(self, execution_id: str) -> Optional[Execution]:
        rows = self._query("SELECT data FROM executions WHERE id = ?", (execution_id,))
        return Execution.model_validate_json(rows[0][0]) if rows else None

    def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        # distributed SQLite: same json_extract projection as SqliteStore, over _query.
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
        rows = self._query(
            "SELECT id, definition_id, version, json_extract(data,'$.status'), "
            "json_extract(data,'$.outcome'), json_extract(data,'$.active_path'), "
            "json_extract(data,'$.parent_id') FROM executions "
            f"WHERE {' AND '.join(where)} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit + 1, off),
        )
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

    def save(self, exe: Execution) -> None:
        self.commit(exe, [])

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> None:
        old = exe.version
        exe.version = old + 1  # bump BEFORE dumping so the stored JSON carries the new version
        new = exe.version
        data = exe.model_dump_json()
        # the upsert applies only WHERE version=old; every other write is guarded on
        # the row holding our `data` — so a CAS miss leaves the whole txn a no-op
        statements: list = [
            [
                "INSERT INTO executions (id, definition_id, data, version) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET data = excluded.data, version = excluded.version "
                "WHERE executions.version = ?",
                exe.id,
                exe.definition_id,
                data,
                new,
                old,
            ]
        ]
        # guard each side-write on the row holding *our* exact `data` (not just
        # version=new): that is true iff our upsert won the CAS, so a concurrent
        # writer that reached the same version with different state can't make our
        # outbox leak. (Two byte-identical writes are idempotent; the target dedupes.)
        for target_id, event in emits:
            statements.append(
                [
                    "INSERT INTO outbox (target_id, event) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    target_id,
                    event.model_dump_json(),
                    exe.id,
                    data,
                ]
            )
        if processed_event_id is not None:
            statements.append(
                [
                    "INSERT OR IGNORE INTO processed_events (execution_id, event_id) SELECT ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    processed_event_id,
                    exe.id,
                    data,
                ]
            )
        # timer ops, also guarded on our data winning the CAS. Schedule = delete+insert
        # (upsert), so re-entry replaces the fire_at; cancel = delete.
        for op in timers:
            statements.append(
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? "
                    "AND EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    op.path,
                    exe.id,
                    data,
                ]
            )
            if op.action == "schedule":
                statements.append(
                    [
                        "INSERT INTO timers (execution_id, path, fire_at) SELECT ?, ?, ? "
                        "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                        exe.id,
                        op.path,
                        op.fire_at,
                        exe.id,
                        data,
                    ]
                )
        # spawns, guarded on our data winning the CAS (like the outbox)
        for child_id, root_path, context in spawns:
            statements.append(
                [
                    "INSERT INTO spawns (parent_id, child_id, root_path, context) SELECT ?, ?, ?, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    child_id,
                    root_path,
                    json.dumps(context),
                    exe.id,
                    data,
                ]
            )
        # trace step, guarded on our CAS win. idx computed inline (MAX+1) so no pre-read;
        # the cap delete runs after, in the same transactional request (sees the new row).
        if trace is not None:
            statements.append(
                [
                    "INSERT INTO trace (execution_id, idx, entry) "
                    "SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ? "
                    "WHERE EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                    exe.id,
                    exe.id,
                    json.dumps(trace),
                    exe.id,
                    data,
                ]
            )
            if self.trace_max:
                statements.append(
                    [
                        "DELETE FROM trace WHERE execution_id = ? AND idx <= "
                        "(SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ? "
                        "AND EXISTS (SELECT 1 FROM executions WHERE id = ? AND data = ?)",
                        exe.id,
                        exe.id,
                        self.trace_max,
                        exe.id,
                        data,
                    ]
                )
        results = self._execute(statements, transaction=True)
        if results[0].get("rows_affected", 0) == 0:
            exe.version = old  # CAS missed: undo the in-memory bump (nothing was written)
            found = self._query("SELECT version FROM executions WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)
        # success: exe.version is already `new`

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = self._query(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    def append_trace(self, execution_id: str, entry: dict) -> None:
        """The demo/test seam (unguarded): append a step with idx = MAX+1, then cap."""
        self._execute(
            [
                [
                    "INSERT INTO trace (execution_id, idx, entry) "
                    "SELECT ?, COALESCE((SELECT MAX(idx) FROM trace WHERE execution_id = ?), -1) + 1, ?",
                    execution_id,
                    execution_id,
                    json.dumps(entry),
                ]
            ]
        )
        if self.trace_max:
            self._execute(
                [
                    [
                        "DELETE FROM trace WHERE execution_id = ? AND idx <= "
                        "(SELECT MAX(idx) FROM trace WHERE execution_id = ?) - ?",
                        execution_id,
                        execution_id,
                        self.trace_max,
                    ]
                ]
            )

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._query(
            "SELECT idx, entry FROM trace WHERE execution_id = ? ORDER BY idx", (execution_id,)
        )
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._query("SELECT seq, target_id, event FROM outbox ORDER BY seq", ())
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._execute([["DELETE FROM outbox WHERE seq = ?", seq]])

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._query("SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq", ())
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._execute([["DELETE FROM spawns WHERE seq = ?", seq]])

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._query(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._execute(
            [
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    def close(self) -> None:
        self._session.close()
