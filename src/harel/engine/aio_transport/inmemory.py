"""AsyncInMemoryTransport — an async Transport backend."""

from __future__ import annotations

import time
from typing import Callable, Optional

from harel.engine.transport import _PARKED, Lease
from harel.spec.states import Event


class AsyncInMemoryTransport:
    """Same-process async `Transport`: a faithful async mirror of `InMemoryTransport`
    (lease/visibility via `lock_expiry`, `_PARKED` parking for `nack(delay)`). No lock —
    a single event loop serializes the (await-free) critical sections, doing what the
    sync transport's `threading.Lock` does across threads."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._groups: dict[str, dict] = {}  # group_id → {last_claimed_at, priority}
        self._clock = clock

    async def publish(self, group_id: str, event: Event, priority: int = 0) -> None:
        self._seq += 1
        self._messages.append(
            {
                "seq": self._seq,
                "group_id": group_id,
                "event": event,
                "locked_by": None,
                "lock_expiry": 0.0,
            }
        )
        self._groups.setdefault(group_id, {"last_claimed_at": 0.0, "priority": priority})

    async def claim(self, worker_id: str, visibility: float, min_priority: int = 0) -> Optional[Lease]:
        now = self._clock()
        in_flight = {
            m["group_id"] for m in self._messages if m["locked_by"] is not None and m["lock_expiry"] >= now
        }
        for m in sorted(
            self._messages,
            key=lambda m: (self._groups.get(m["group_id"], {}).get("last_claimed_at", 0.0), m["seq"]),
        ):
            available = m["locked_by"] is None or m["lock_expiry"] < now
            if not available or m["group_id"] in in_flight:
                continue
            if self._groups.get(m["group_id"], {}).get("priority", 0) < min_priority:
                continue
            m["locked_by"] = worker_id
            m["lock_expiry"] = now + visibility
            if m["group_id"] in self._groups:
                self._groups[m["group_id"]]["last_claimed_at"] = now
            return Lease(m["seq"], m["group_id"], m["event"])
        return None

    async def ack(self, lease: Lease) -> None:
        self._messages = [m for m in self._messages if m["seq"] != lease.seq]
        if not any(m["group_id"] == lease.group_id for m in self._messages):
            self._groups.pop(lease.group_id, None)

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        for m in self._messages:
            if m["seq"] == lease.seq:
                if delay > 0:
                    m["locked_by"] = _PARKED
                    m["lock_expiry"] = self._clock() + delay
                else:
                    m["locked_by"] = None
                    m["lock_expiry"] = 0.0

    async def close(self) -> None:
        pass
