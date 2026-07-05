"""InMemoryTransport — a Transport backend."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from harel.engine.transport._base import _PARKED, Lease
from harel.spec.states import Event


class InMemoryTransport:
    """Same-process `Transport`: a list guarded by a lock. The lock serializes
    `claim` (so the per-group exclusivity check is race-free), mirroring what the
    SQLite write-lock does across processes."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._last_claimed: dict[str, float] = {}  # group_id → last claim timestamp
        self._lock = threading.Lock()
        self._clock = clock

    def publish(self, group_id: str, event: Event) -> None:
        with self._lock:
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
            self._last_claimed.setdefault(group_id, 0.0)

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        now = self._clock()
        with self._lock:
            in_flight = {
                m["group_id"]
                for m in self._messages
                if m["locked_by"] is not None and m["lock_expiry"] >= now
            }
            # pick the head message of the group with the oldest last_claimed_at (round-robin)
            for m in sorted(
                self._messages, key=lambda m: (self._last_claimed.get(m["group_id"], 0.0), m["seq"])
            ):
                available = m["locked_by"] is None or m["lock_expiry"] < now
                if available and m["group_id"] not in in_flight:
                    m["locked_by"] = worker_id
                    m["lock_expiry"] = now + visibility
                    self._last_claimed[m["group_id"]] = now
                    return Lease(m["seq"], m["group_id"], m["event"])
            return None

    def ack(self, lease: Lease) -> None:
        with self._lock:
            self._messages = [m for m in self._messages if m["seq"] != lease.seq]
            if not any(m["group_id"] == lease.group_id for m in self._messages):
                self._last_claimed.pop(lease.group_id, None)

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        with self._lock:
            for m in self._messages:
                if m["seq"] == lease.seq:
                    if delay > 0:
                        m["locked_by"] = _PARKED
                        m["lock_expiry"] = self._clock() + delay
                    else:
                        m["locked_by"] = None
                        m["lock_expiry"] = 0.0

    def close(self) -> None:
        pass  # nothing to release; the list lives with the process
