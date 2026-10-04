"""RqliteStore — a durable ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.schema import DEFAULT_PREFIX, Names, store_schema
from harel.engine.store._base import (
    _IDS_WITH_PREFIX_SQL,
    _PURGE_COMPANIONS_SQL,
    DEFAULT_TRACE_MAX,
    OutboxEntry,
    SpawnEntry,
    Step,
    StoreConflict,
    TimerOp,
    _decode_offset,
    _like_prefix,
    _listing_page,
    _listing_sql_sqlite,
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

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> None:
        """`prefix` names its tables (see `harel.engine.schema`); with `create_schema=False`
        they must already exist."""
        import requests

        self._n = Names(prefix)
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        if create_schema:
            self._execute(store_schema("sqlite", prefix))
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    def from_url(
        cls,
        url: str,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "RqliteStore":
        """Build a store, retrying until rqlite is up and has elected a leader (a
        worker starting alongside rqlite in compose waits rather than crashing)."""
        import time

        import requests

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                store = cls(url, prefix=prefix, create_schema=create_schema)
                if not create_schema:
                    store._query("SELECT 1", ())  # nothing created: still wait for a leader
                return store
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
        rows = self._query(f"SELECT data FROM {self._n.executions} WHERE id = ?", (execution_id,))
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
        # distributed SQLite: the same json_extract projection as SqliteStore, over _query
        off = _decode_offset(cursor)
        sql, params = _listing_sql_sqlite(self._n.executions, status, definition_id, roots_only, limit, off)
        return _listing_page(self._query(sql, params), limit, off)

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
        step: Optional[Step] = None,
    ) -> list[int]:
        old = exe.version
        exe.version = old + 1  # bump BEFORE dumping so the stored JSON carries the new version
        new = exe.version
        data = exe.model_dump_json()
        # the CAS write: a brand-new Execution (old == 0) inserts only if the id is free; any
        # other version updates only WHERE version=old — never an insert, so a stale copy of
        # a purged Execution can't recreate it. Every other write is guarded on the row
        # holding our `data`, so a CAS miss leaves the whole txn a no-op.
        statements: list = [
            [
                f"INSERT OR IGNORE INTO {self._n.executions} (id, definition_id, data, version) VALUES (?, ?, ?, ?)",
                exe.id,
                exe.definition_id,
                data,
                new,
            ]
            if old == 0
            else [
                f"UPDATE {self._n.executions} SET data = ?, version = ? WHERE id = ? AND version = ?",
                data,
                new,
                exe.id,
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
                    f"INSERT INTO {self._n.outbox} (target_id, event) SELECT ?, ? "
                    f"WHERE EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
                    target_id,
                    event.model_dump_json(),
                    exe.id,
                    data,
                ]
            )
        if processed_event_id is not None:
            statements.append(
                [
                    f"INSERT OR IGNORE INTO {self._n.processed_events} (execution_id, event_id) SELECT ?, ? "
                    f"WHERE EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
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
                    f"DELETE FROM {self._n.timers} WHERE execution_id = ? AND path = ? "
                    f"AND EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
                    exe.id,
                    op.path,
                    exe.id,
                    data,
                ]
            )
            if op.action == "schedule":
                statements.append(
                    [
                        f"INSERT INTO {self._n.timers} (execution_id, path, fire_at) SELECT ?, ?, ? "
                        f"WHERE EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
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
                    f"INSERT INTO {self._n.spawns} (parent_id, child_id, root_path, context) SELECT ?, ?, ?, ? "
                    f"WHERE EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
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
                    f"INSERT INTO {self._n.trace} (execution_id, idx, entry) "
                    f"SELECT ?, COALESCE((SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?), -1) + 1, ? "
                    f"WHERE EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
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
                        f"DELETE FROM {self._n.trace} WHERE execution_id = ? AND idx <= "
                        f"(SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?) - ? "
                        f"AND EXISTS (SELECT 1 FROM {self._n.executions} WHERE id = ? AND data = ?)",
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
            found = self._query(f"SELECT version FROM {self._n.executions} WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)
        # success: exe.version is already `new`; the outbox inserts are statements 1..len(emits)
        return [int(r["last_insert_id"]) for r in results[1 : 1 + len(emits)]]

    def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = self._query(
            f"SELECT 1 FROM {self._n.processed_events} WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    def append_trace(self, execution_id: str, entry: dict) -> None:
        """The demo/test seam (unguarded): append a step with idx = MAX+1, then cap."""
        self._execute(
            [
                [
                    f"INSERT INTO {self._n.trace} (execution_id, idx, entry) "
                    f"SELECT ?, COALESCE((SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?), -1) + 1, ?",
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
                        f"DELETE FROM {self._n.trace} WHERE execution_id = ? AND idx <= "
                        f"(SELECT MAX(idx) FROM {self._n.trace} WHERE execution_id = ?) - ?",
                        execution_id,
                        execution_id,
                        self.trace_max,
                    ]
                ]
            )

    def read_trace(self, execution_id: str) -> list[dict]:
        rows = self._query(
            f"SELECT idx, entry FROM {self._n.trace} WHERE execution_id = ? ORDER BY idx", (execution_id,)
        )
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    def pending_outbox(self) -> list[OutboxEntry]:
        rows = self._query(f"SELECT seq, target_id, event FROM {self._n.outbox} ORDER BY seq", ())
        return [
            OutboxEntry(seq, target_id, Event.model_validate_json(event)) for seq, target_id, event in rows
        ]

    def ack_outbox(self, seq: int) -> None:
        self._execute([[f"DELETE FROM {self._n.outbox} WHERE seq = ?", seq]])

    def pending_spawns(self) -> list[SpawnEntry]:
        rows = self._query(
            f"SELECT seq, parent_id, child_id, root_path, context FROM {self._n.spawns} ORDER BY seq", ()
        )
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    def ack_spawn(self, seq: int) -> None:
        self._execute([[f"DELETE FROM {self._n.spawns} WHERE seq = ?", seq]])

    def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = self._query(
            f"SELECT execution_id, path, fire_at FROM {self._n.timers} WHERE fire_at <= ? ORDER BY fire_at",
            (now,),
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._execute(
            [
                [
                    f"DELETE FROM {self._n.timers} WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    def ids_with_prefix(self, prefix: str) -> list[str]:
        return [r[0] for r in self._query(self._n.sql(_IDS_WITH_PREFIX_SQL), (_like_prefix(prefix),))]

    def purge(self, execution_id: str, expected_version: int) -> bool:
        results = self._execute(_purge_statements(self._n, execution_id, expected_version), transaction=True)
        return results[0].get("rows_affected", 0) == 1

    def close(self) -> None:
        self._session.close()


def _purge_statements(names: Names, execution_id: str, expected_version: int) -> list:
    """One transactional request (rqlite has no interactive transactions): the CAS delete,
    then every companion delete guarded on the Execution now being absent — so a version
    mismatch leaves the whole request a no-op, and an already-purged id is still swept."""
    gone = f" AND NOT EXISTS (SELECT 1 FROM {names.executions} WHERE id = ?)"
    return [
        [f"DELETE FROM {names.executions} WHERE id = ? AND version = ?", execution_id, expected_version],
        *[[sql + gone, execution_id, execution_id] for sql in map(names.sql, _PURGE_COMPANIONS_SQL)],
    ]
