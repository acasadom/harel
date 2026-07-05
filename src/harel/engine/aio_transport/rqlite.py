"""AsyncRqliteTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.transport import _PARKED, Lease
from harel.spec.states import Event


class AsyncRqliteTransport:
    """Async mirror of `RqliteTransport` over `httpx.AsyncClient`: the same claim
    strategy (one serialized UPDATE leases the oldest deliverable message with a unique
    token, raft ensures sequential consistency) with every HTTP call awaited.
    Build with `await AsyncRqliteTransport.from_url(url)`."""

    def __init__(
        self, client: Any, base_url: str, timeout: float = 10.0, clock: Callable[[], float] = time.time
    ) -> None:
        self._client = client
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._clock = clock

    @classmethod
    async def from_url(
        cls,
        url: str,
        timeout: float = 10.0,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
    ) -> "AsyncRqliteTransport":
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                transport = cls(client, url, timeout)
                await transport._execute(
                    [
                        "CREATE TABLE IF NOT EXISTS messages "
                        "(seq INTEGER PRIMARY KEY AUTOINCREMENT, group_id TEXT NOT NULL, "
                        "event TEXT NOT NULL, locked_by TEXT, lock_expiry REAL)",
                        "CREATE TABLE IF NOT EXISTS groups "
                        "(group_id TEXT PRIMARY KEY, last_claimed_at REAL NOT NULL DEFAULT 0.0, "
                        "priority INT NOT NULL DEFAULT 0)",
                        "INSERT OR IGNORE INTO groups (group_id) SELECT DISTINCT group_id FROM messages",
                    ]
                )
                return transport
            except Exception as exc:  # noqa: BLE001
                await client.aclose()
                last = exc
                await anyio.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    async def _execute(self, statements: list, transaction: bool = False) -> list:
        url = f"{self._base}/db/execute" + ("?transaction" if transaction else "")
        resp = await self._client.post(url, json=statements, timeout=self._timeout)
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

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        await self._execute(
            [
                ["INSERT INTO messages (group_id, event) VALUES (?, ?)", group_id, event.model_dump_json()],
                ["INSERT OR IGNORE INTO groups (group_id, priority) VALUES (?, ?)", group_id, priority],
            ],
            transaction=True,
        )

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        results = await self._execute(
            [
                [
                    "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ("
                    "  SELECT m.seq FROM messages m "
                    "  JOIN groups g ON g.group_id = m.group_id "
                    "  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                    "    AND m.group_id NOT IN ("
                    "      SELECT group_id FROM messages WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                    "    ) AND g.priority >= ?"
                    "  ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1)",
                    token,
                    now + visibility,
                    now,
                    now,
                    min_priority,
                ]
            ]
        )
        if results[0].get("rows_affected", 0) == 0:
            return None
        rows = await self._query("SELECT seq, group_id, event FROM messages WHERE locked_by = ?", (token,))
        seq, group_id, event = rows[0]
        await self._execute([["UPDATE groups SET last_claimed_at = ? WHERE group_id = ?", now, group_id]])
        return Lease(seq, group_id, Event.model_validate_json(event), token=token)

    async def ack(self, lease: Lease) -> None:
        await self._execute(
            [
                ["DELETE FROM messages WHERE seq = ?", lease.seq],
                [
                    "DELETE FROM groups WHERE group_id = ? AND NOT EXISTS "
                    "(SELECT 1 FROM messages WHERE group_id = ?)",
                    lease.group_id,
                    lease.group_id,
                ],
            ],
            transaction=True,
        )

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            await self._execute(
                [
                    [
                        "UPDATE messages SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                        _PARKED,
                        self._clock() + delay,
                        lease.seq,
                    ]
                ]
            )
        else:
            await self._execute(
                [["UPDATE messages SET locked_by = NULL, lock_expiry = 0 WHERE seq = ?", lease.seq]]
            )

    async def close(self) -> None:
        await self._client.aclose()
