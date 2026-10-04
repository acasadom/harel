"""RqliteTransport — a Transport backend."""

from __future__ import annotations

import time
import uuid
from typing import Callable, Optional

from harel.engine.schema import DEFAULT_PREFIX, Names, transport_schema
from harel.engine.transport._base import _PARKED, Lease
from harel.spec.states import Event


class RqliteTransport:
    """`Transport` over rqlite — a multi-machine queue on distributed SQLite. rqlite
    serializes all writes through the Raft leader, so the per-group exclusivity
    selection is race-free in a single statement (like SQLite's write-lock). `claim`
    leases the oldest deliverable message with a unique token in one UPDATE, then
    reads that row back by token. Lease times are the client clock. The base URL is
    injected, so `requests` is an optional extra."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        clock: Callable[[], float] = time.time,
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
        self._clock = clock
        self._session = requests.Session()
        if create_schema:
            self._execute(transport_schema("sqlite", prefix))

    @classmethod
    def from_url(
        cls,
        url: str,
        connect_retries: int = 30,
        retry_delay: float = 1.0,
        *,
        prefix: str = DEFAULT_PREFIX,
        create_schema: bool = True,
    ) -> "RqliteTransport":
        import time as _time

        import requests

        last: Exception | None = None
        for _ in range(connect_retries):
            try:
                transport = cls(url, prefix=prefix, create_schema=create_schema)
                if not create_schema:
                    transport._query("SELECT 1", ())  # nothing created: still wait for a leader
                return transport
            except requests.exceptions.RequestException as exc:
                last = exc
                _time.sleep(retry_delay)
        raise last if last is not None else RuntimeError("rqlite connect failed")

    def _execute(self, statements: list, transaction: bool = False) -> list:
        url = f"{self._base}/db/execute" + ("?transaction" if transaction else "")
        resp = self._session.post(url, json=statements, timeout=self._timeout)
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

    def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        self._execute(
            [
                [
                    f"INSERT INTO {self._n.messages} (group_id, event) VALUES (?, ?)",
                    group_id,
                    event.model_dump_json(),
                ],
                [
                    f"INSERT OR IGNORE INTO {self._n.groups} (group_id, priority) VALUES (?, ?)",
                    group_id,
                    priority,
                ],
            ],
            transaction=True,
        )

    def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        token = f"{worker_id}:{uuid.uuid4().hex}"
        results = self._execute(
            [
                [
                    f"UPDATE {self._n.messages} SET locked_by = ?, lock_expiry = ? WHERE seq = ("
                    f"  SELECT m.seq FROM {self._n.messages} m "
                    f"  JOIN {self._n.groups} g ON g.group_id = m.group_id "
                    "  WHERE (m.locked_by IS NULL OR m.lock_expiry < ?) "
                    "    AND m.group_id NOT IN ("
                    f"      SELECT group_id FROM {self._n.messages} WHERE locked_by IS NOT NULL AND lock_expiry >= ?"
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
        rows = self._query(
            f"SELECT seq, group_id, event FROM {self._n.messages} WHERE locked_by = ?", (token,)
        )
        seq, group_id, event = rows[0]
        self._execute(
            [[f"UPDATE {self._n.groups} SET last_claimed_at = ? WHERE group_id = ?", now, group_id]]
        )
        return Lease(seq, group_id, Event.model_validate_json(event), token=token)

    def ack(self, lease: Lease) -> None:
        self._execute(
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

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        if delay > 0:
            self._execute(
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
            self._execute(
                [
                    [
                        f"UPDATE {self._n.messages} SET locked_by = NULL, lock_expiry = 0 WHERE seq = ?",
                        lease.seq,
                    ]
                ]
            )

    def close(self) -> None:
        self._session.close()
