"""AsyncRqliteStore — an async ExecutionStore backend."""

from __future__ import annotations

import json
from typing import Any, Optional

from harel.engine.execution import Execution
from harel.engine.store import OutboxEntry, SpawnEntry, StoreConflict, TimerOp
from harel.spec.states import Event


class AsyncRqliteStore:
    """Async mirror of `RqliteStore` over `httpx.AsyncClient`: the same guarded-upsert
    CAS (no interactive transactions — all writes in one transactional request, each
    side-write conditioned on the Execution row holding our exact `data`) with every
    HTTP call awaited. Build with `await AsyncRqliteStore.from_url(url)`."""

    def __init__(self, client: Any, base_url: str, timeout: float = 10.0) -> None:
        self._client = client
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    @classmethod
    async def from_url(
        cls,
        url: str,
        timeout: float = 10.0,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncRqliteStore":
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                store = cls(client, url, timeout)
                await store._execute(
                    [
                        "CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, "
                        "definition_id TEXT NOT NULL, data TEXT NOT NULL, version INTEGER NOT NULL)",
                        "CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "target_id TEXT, event TEXT NOT NULL)",
                        "CREATE TABLE IF NOT EXISTS processed_events "
                        "(execution_id TEXT NOT NULL, event_id TEXT NOT NULL, "
                        "PRIMARY KEY (execution_id, event_id))",
                        "CREATE TABLE IF NOT EXISTS timers (execution_id TEXT NOT NULL, "
                        "path TEXT NOT NULL, fire_at REAL NOT NULL, "
                        "PRIMARY KEY (execution_id, path))",
                        "CREATE TABLE IF NOT EXISTS spawns (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
                        "parent_id TEXT NOT NULL, child_id TEXT NOT NULL, "
                        "root_path TEXT NOT NULL, context TEXT NOT NULL)",
                    ]
                )
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
        rows = await self._query("SELECT data FROM executions WHERE id = ?", (execution_id,))
        return Execution.model_validate_json(rows[0][0]) if rows else None

    async def load_for_event(self, execution_id: str, event_id: str) -> tuple[Optional[Execution], bool]:
        """Load + dedupe-check in one HTTP request (one SELECT with an EXISTS subquery)."""
        rows = await self._query(
            "SELECT data, EXISTS(SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?) "
            "FROM executions WHERE id = ?",
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
    ) -> None:
        old = exe.version
        exe.version = old + 1
        new = exe.version
        data = exe.model_dump_json()
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
        results = await self._execute(statements, transaction=True)
        if results[0].get("rows_affected", 0) == 0:
            exe.version = old
            found = await self._query("SELECT version FROM executions WHERE id = ?", (exe.id,))
            raise StoreConflict(exe.id, expected=old, found=found[0][0] if found else None)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        rows = await self._query(
            "SELECT 1 FROM processed_events WHERE execution_id = ? AND event_id = ?",
            (execution_id, event_id),
        )
        return bool(rows)

    async def pending_outbox(self) -> list[OutboxEntry]:
        rows = await self._query("SELECT seq, target_id, event FROM outbox ORDER BY seq", ())
        return [OutboxEntry(seq, tid, Event.model_validate_json(ev)) for seq, tid, ev in rows]

    async def ack_outbox(self, seq: int) -> None:
        await self._execute([["DELETE FROM outbox WHERE seq = ?", seq]])

    async def pending_spawns(self) -> list[SpawnEntry]:
        rows = await self._query(
            "SELECT seq, parent_id, child_id, root_path, context FROM spawns ORDER BY seq", ()
        )
        return [SpawnEntry(seq, pid, cid, rp, json.loads(ctx)) for seq, pid, cid, rp, ctx in rows]

    async def ack_spawn(self, seq: int) -> None:
        await self._execute([["DELETE FROM spawns WHERE seq = ?", seq]])

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        rows = await self._query(
            "SELECT execution_id, path, fire_at FROM timers WHERE fire_at <= ? ORDER BY fire_at", (now,)
        )
        return [(eid, path, float(fa)) for eid, path, fa in rows]

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        await self._execute(
            [
                [
                    "DELETE FROM timers WHERE execution_id = ? AND path = ? AND fire_at = ?",
                    execution_id,
                    path,
                    fire_at,
                ]
            ]
        )

    async def close(self) -> None:
        await self._client.aclose()
