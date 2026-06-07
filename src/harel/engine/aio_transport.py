"""Async `Transport` — the async sibling of `harel.engine.transport`.

Same contract (single-active-consumer per group, FIFO within a group, lease/visibility,
`nack(delay)` parking), every method `async def`. The sync transports stay untouched.

Holds the `AsyncTransport` Protocol + `AsyncInMemoryTransport`. The networked async
backends (sqlite/redis/postgres) are added in later phases. `Lease` is reused as-is.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, Protocol, runtime_checkable

from harel.engine.transport import _PARKED, Lease
from harel.spec.states import Event


@runtime_checkable
class AsyncTransport(Protocol):
    """Async mirror of `Transport`: identical per-group-exclusivity semantics, awaited IO."""

    async def publish(self, group_id: str, event: Event) -> None: ...

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]: ...

    async def ack(self, lease: Lease) -> None: ...

    async def nack(self, lease: Lease, delay: float = 0.0) -> None: ...

    async def close(self) -> None: ...


class AsyncInMemoryTransport:
    """Same-process async `Transport`: a faithful async mirror of `InMemoryTransport`
    (lease/visibility via `lock_expiry`, `_PARKED` parking for `nack(delay)`). No lock —
    a single event loop serializes the (await-free) critical sections, doing what the
    sync transport's `threading.Lock` does across threads."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._clock = clock

    async def publish(self, group_id: str, event: Event) -> None:
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

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        in_flight = {
            m["group_id"] for m in self._messages if m["locked_by"] is not None and m["lock_expiry"] >= now
        }
        for m in sorted(self._messages, key=lambda m: m["seq"]):
            available = m["locked_by"] is None or m["lock_expiry"] < now
            if available and m["group_id"] not in in_flight:
                m["locked_by"] = worker_id
                m["lock_expiry"] = now + visibility
                return Lease(m["seq"], m["group_id"], m["event"])
        return None

    async def ack(self, lease: Lease) -> None:
        self._messages = [m for m in self._messages if m["seq"] != lease.seq]

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
