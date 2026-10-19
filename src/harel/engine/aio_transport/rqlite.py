"""AsyncRqliteTransport — an async Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Optional

from harel.engine.schema import DEFAULT_PREFIX, Names, sqlite_transport_upgrade, transport_schema
from harel.engine.transport import _PARKED, Backlog, Lease
from harel.engine.transport._base import _SQLITE_BACKLOG_SQL, _backlog, _routing_filter
from harel.spec.states import Event


class AsyncRqliteTransport:
    """Async mirror of `RqliteTransport` over `httpx.AsyncClient`: the same claim
    strategy (one serialized UPDATE leases the oldest deliverable message with a unique
    token, raft ensures sequential consistency) with every HTTP call awaited.
    Build with `await AsyncRqliteTransport.from_url(url)`."""

    def __init__(
        self,
        client: Any,
        base_url: str,
        timeout: float = 10.0,
        clock: Callable[[], float] = time.time,
        *,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        self._n = Names(prefix)
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
        *,
        clock: Callable[[], float] = time.time,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "AsyncRqliteTransport":
        """Connect, retrying until rqlite is up and has elected a leader; `prefix` names its
        tables (see `harel.engine.schema`), and with `create_schema=False` they must already
        exist. `clock` is what its leases and parks are measured by."""
        import anyio
        import httpx

        last: Exception | None = None
        for _ in range(connect_retries):
            client = httpx.AsyncClient()
            try:
                transport = cls(client, url, timeout, clock, prefix=prefix)
                if create_schema:
                    statements = transport_schema("sqlite", prefix)
                    await transport._execute([q for q in statements if q.startswith("CREATE TABLE")])
                    names = Names(prefix)
                    existing = {
                        table: [
                            row[1]
                            for row in await transport._query(
                                f"PRAGMA table_info({getattr(names, table)})", ()
                            )
                        ]
                        for table in ("groups", "messages")
                    }
                    upgrade = sqlite_transport_upgrade(existing, prefix)
                    await transport._execute(
                        upgrade + [q for q in statements if not q.startswith("CREATE TABLE")]
                    )
                else:
                    await transport._query("SELECT 1", ())  # nothing created: still wait for a leader
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

    async def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        await self._execute(
            [
                [
                    f"INSERT INTO {self._n.messages} (group_id, event, published_at) VALUES (?, ?, ?)",
                    group_id,
                    event.model_dump_json(),
                    self._clock(),
                ],
                [
                    f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority, routing_key) VALUES (?, ?, ?)",
                    group_id,
                    priority,
                    routing_key,
                ],
            ],
            transaction=True,
        )

    async def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        routing, keys = _routing_filter(routing_keys, "g.routing_key")
        results = await self._execute(
            [
                [
                    f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ("
                    f"  SELECT m.seq FROM {self._n.messages} m "
                    f"  JOIN {self._n.groups} g ON g.group_id = m.group_id "
                    "  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                    "    AND m.group_id NOT IN ("
                    f"      SELECT group_id FROM {self._n.messages} WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
                    f"    ) AND g.priority >= ?{routing}"
                    "  ORDER BY g.last_claimed_at ASC, m.seq ASC LIMIT 1)",
                    token,
                    now + visibility,
                    now,
                    now,
                    min_priority,
                    *keys,
                ]
            ]
        )
        if results[0].get("rows_affected", 0) == 0:
            return None
        rows = await self._query(
            f"SELECT seq, group_id, event FROM {self._n.messages} WHERE locked_by = ?", (token,)
        )
        seq, group_id, event = rows[0]
        await self._execute(
            [[f"UPDATE {self._n.groups} SET last_claimed_at = ? WHERE group_id = ?", now, group_id]]
        )
        return Lease(seq, group_id, Event.model_validate_json(event), token=token)

    async def ack(self, lease: Lease) -> None:
        await self._execute(
            [
                [f"DELETE FROM {self._n.messages} WHERE seq = ?", lease.seq],
                [
                    f"DELETE FROM {self._n.groups} WHERE group_id = ? AND NOT EXISTS "
                    f"(SELECT 1 FROM {self._n.messages} WHERE group_id = ?)",
                    lease.group_id,
                    lease.group_id,
                ],
            ],
            transaction=True,
        )

    async def backlog(self) -> Backlog:
        return _backlog(await self._query(self._n.sql(_SQLITE_BACKLOG_SQL), (self._clock(),)))

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            await self._execute(
                [
                    [
                        f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ?",
                        _PARKED,
                        self._clock() + delay,
                        lease.seq,
                    ]
                ]
            )
        else:
            await self._execute(
                [
                    [
                        f"UPDATE {self._n.messages} SET locked_by = NULL, lock_expiry = 0 WHERE seq = ?",
                        lease.seq,
                    ]
                ]
            )

    async def close(self) -> None:
        await self._client.aclose()
