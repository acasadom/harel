"""AsyncRqliteStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.schema import DEFAULT_PREFIX, Names, store_schema
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.engine.store._base import (
    _IDS_WITH_PREFIX_SQL,
    DEFAULT_TRACE_MAX,
    _decode_offset,
    _like_prefix,
    _listing_page,
    _listing_sql_sqlite,
)
from harel.engine.store.rqlite import _purge_statements
from harel.spec.states import Event


class AsyncRqliteStore:
    """Async mirror of `RqliteStore` over `httpx.AsyncClient`: the same guarded-upsert
    CAS (no interactive transactions — all writes in one transactional request, each
    side-write conditioned on the Execution row holding our exact `data`) with every
    HTTP call awaited. Build with `await AsyncRqliteStore.from_url(url)`."""

    def __init__(
        self, client: Any, base_url: str, timeout: float = 10.0, *, prefix: str = DEFAULT_PREFIX
    ) -> None:
        self._n = Names(prefix)
        self._client = client
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self.trace_max = DEFAULT_TRACE_MAX

    @classmethod
    async def from_url(
        cls,
        url: str,
        timeout: float = 10.0,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "AsyncRqliteStore":
        """Connect, retrying until rqlite is up and has elected a leader; `prefix` names its
        tables (see `harel.engine.schema`), and with `create_schema=False` they must already
        exist."""
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                store = cls(client, url, timeout, prefix=prefix)
                if create_schema:
                    await store._execute(store_schema("sqlite", prefix))
                else:
                    await store._query("SELECT 1", ())  # nothing created: still wait for a leader
                return store
            except Exception as exc:  # noqa: BLE001
                await client.aclose()
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    async def _execute(self, statements: list, transaction: bool = False) -> list:
        params = {"transaction": ""} if transaction else {}
        resp = await self._client.post(
            f"{self._base}/db/execute", params=params, json=statements, timeout=self._timeout
        )
        resp.raise_for_status()
        results = resp.json()["results"]
        for res in results:
            if "error" in res:
                raise RuntimeError(f"rqlite execute error: {res['error']}")
        return results

    async def _query(self, sql: str, params: tuple) -> list:
        resp = await self._client.post(
            f"{self._base}/db/query",
            params={"level": "strong"},
            json=[[sql, *params]],
            timeout=self._timeout,
        )
        resp.raise_for_status()
        result = resp.json()["results"][0]
        if "error" in result:
            raise RuntimeError(f"rqlite query error: {result['error']}")
        return result.get("values") or []

    async def load(self, execution_id: str) -> Optional[Execution]:
        rows = await self._query(f"SELECT data FROM {self._n.executions} WHERE id = ?", (execution_id,))
        return Execution.model_validate_json(rows[0][0]) if rows else None

    async def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage:
        """See `RqliteStore.list_executions`."""
        off = _decode_offset(cursor)
        sql, params = _listing_sql_sqlite(self._n.executions, status, definition_id, roots_only, limit, off)
        return _listing_page(await self._query(sql, params), limit, off)

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one HTTP request (one SELECT with an EXISTS subquery)."""
        rows = await self._query(
            f"SELECT data, EXISTS(SELECT 1 FROM {self._n.processed_events} WHERE execution_id = ? AND event_id = ?) "
            f"FROM {self._n.executions} WHERE id = ?",
            (execution_id, event_id, execution_id),
        )
        if not rows:
            return None, False
        return Execution.model_validate_json(rows[0][0]), bool(rows[0][1])

    async def save(self, exe: Execution) -> None:
        await self.commit(exe, [])

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
    ) -> list[int]:
        old = exe.version
        exe.version = old + 1
        new = exe.version
        data = exe.model_dump_json()
        # insert only a brand-new Execution, otherwise CAS-update — see RqliteStore.commit
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
        results = await self._execute(statements, transaction=True)
        if results[0].get("rows_affected", 0) == 0:
            exe.version = old
            found = await self._query(f"SELECT version FROM {self._n.executions} WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)
        # success: exe.version is already `new`; the outbox inserts are statements 1..len(emits)
        return [int(r["last_insert_id"]) for r in results[1 : 1 + len(emits)]]

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = await self._query(
            f"SELECT 1 FROM {self._n.processed_events} WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    async def append_trace(self, execution_id: str, entry: dict) -> None:
        await self._execute(
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
            await self._execute(
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

    async def read_trace(self, execution_id: str) -> list[dict]:
        rows = await self._query(
            f"SELECT idx, entry FROM {self._n.trace} WHERE execution_id = ? ORDER BY idx", (execution_id,)
        )
        return [{**json.loads(entry), "index": idx} for idx, entry in rows]

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._query(f"SELECT seq, target_id, event FROM {self._n.outbox} ORDER BY seq", ())
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        await self._execute([[f"DELETE FROM {self._n.outbox} WHERE seq = ?", seq]])

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._query(
            f"SELECT seq, parent_id, child_id, root_path, context FROM {self._n.spawns} ORDER BY seq", ()
        )
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        await self._execute([[f"DELETE FROM {self._n.spawns} WHERE seq = ?", seq]])

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._query(
            f"SELECT execution_id, path, fire_at FROM {self._n.timers} WHERE fire_at <= ? ORDER BY fire_at",
            (now,),
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        await self._execute(
            [
                [
                    f"DELETE FROM {self._n.timers} WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    async def ids_with_prefix(self, prefix: str) -> list[str]:
        return [r[0] for r in await self._query(self._n.sql(_IDS_WITH_PREFIX_SQL), (_like_prefix(prefix),))]

    async def purge(self, execution_id: str, expected_version: int) -> bool:
        results = await self._execute(
            _purge_statements(self._n, execution_id, expected_version), transaction=True
        )
        return results[0].get("rows_affected", 0) == 1

    async def close(self) -> None:
        await self._client.aclose()
