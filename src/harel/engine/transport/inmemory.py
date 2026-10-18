"""InMemoryTransport — a Transport backend."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from harel.engine.transport._base import _PARKED, Backlog, Lease, _backlog
from harel.spec.states import Event


class InMemoryTransport:
    """Same-process `Transport`: a list guarded by a lock. The lock serializes
    `claim` (so the per-group exclusivity check is race-free), mirroring what the
    SQLite write-lock does across processes."""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._messages: list[dict] = []
        self._seq = 0
        self._groups: dict[str, dict] = {}  # group_id → {last_claimed_at, priority, routing_key}
        self._lock = threading.Lock()
        self._clock = clock

    def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        with self._lock:
            self._seq += 1
            self._messages.append(
                {
                    "seq": self._seq,
                    "group_id": group_id,
                    "event": event,
                    "locked_by": None,
                    "lock_expiry": 0.0,
                    "published_at": self._clock(),
                }
            )
            # the publish creating the group sets its priority and routing key; later ones don't
            self._groups.setdefault(
                group_id, {"last_claimed_at": 0.0, "priority": priority, "routing_key": routing_key}
            )

    def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        now = self._clock()
        with self._lock:
            in_flight = {
                m["group_id"]
                for m in self._messages
                if m["locked_by"] is not None and m["lock_expiry"] >= now
            }
            for m in sorted(
                self._messages,
                key=lambda m: (self._groups.get(m["group_id"], {}).get("last_claimed_at", 0.0), m["seq"]),
            ):
                available = m["locked_by"] is None or m["lock_expiry"] < now
                if not available or m["group_id"] in in_flight:
                    continue
                group = self._groups.get(m["group_id"], {})
                if group.get("priority", 0) < min_priority:
                    continue
                if routing_keys is not None and group.get("routing_key") not in routing_keys:
                    continue
                m["locked_by"] = worker_id
                m["lock_expiry"] = now + visibility
                if m["group_id"] in self._groups:
                    self._groups[m["group_id"]]["last_claimed_at"] = now
                return Lease(m["seq"], m["group_id"], m["event"])
            return None

    def ack(self, lease: Lease) -> None:
        with self._lock:
            self._messages = [m for m in self._messages if m["seq"] != lease.seq]
            if not any(m["group_id"] == lease.group_id for m in self._messages):
                self._groups.pop(lease.group_id, None)

    def backlog(self) -> Backlog:
        with self._lock:
            now = self._clock()
            rows: dict = {}
            for m in self._messages:
                key = self._groups.get(m["group_id"], {}).get("routing_key")
                row = rows.setdefault(key, {"messages": 0, "groups": set(), "held": set(), "oldest": None})
                row["messages"] += 1
                row["groups"].add(m["group_id"])
                if m["locked_by"] is not None and m["lock_expiry"] >= now:
                    row["held"].add(m["group_id"])
                at = m.get("published_at")
                if at is not None and (row["oldest"] is None or at < row["oldest"]):
                    row["oldest"] = at
            return _backlog(
                (k, r["messages"], len(r["groups"]), len(r["held"]), r["oldest"]) for k, r in rows.items()
            )

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
