"""Shared transport contract: the `Transport` Protocol, the `Lease` dataclass, and
the `_PARKED` sentinel. Concrete backends live in sibling modules; the package
`__init__` re-exports them so existing imports keep working."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

from harel.spec.states import Event

# sentinel `locked_by` for a message parked by `nack(delay>0)`: non-null (so the
# claim's "available"/in-flight checks skip it) until its `lock_expiry` passes.
_PARKED = "__parked__"


@dataclass
class Lease:
    """A claimed message: the `group_id` it belongs to and the `event`, plus the
    backend's handle to identify it on ack/nack — `seq` (the row/message id, for
    the in-memory and sqlite backends) or `token` (the Redis group-lock fencing
    token). Held until `ack` (delivered) or `nack`/expiry (re-deliver)."""

    seq: int
    group_id: str
    event: Event
    token: str = ""


@runtime_checkable
class Transport(Protocol):
    def publish(self, group_id: str, event: Event) -> None:
        """Enqueue `event` in `group_id`'s FIFO."""
        ...

    def claim(self, worker_id: str, visibility: float) -> Optional[Lease]:
        """Lease the oldest message of some group that has nothing in-flight, for
        `visibility` seconds; None if there is nothing deliverable right now."""
        ...

    def ack(self, lease: Lease) -> None:
        """The message was handled: remove it, freeing its group."""
        ...

    def nack(self, lease: Lease, delay: float = 0.0) -> None:
        """Return the message to the queue. With `delay=0` it is immediately
        claimable again (retry now); with `delay>0` it is *parked* — not claimable
        (and its group stays blocked) until `delay` seconds pass. Parking lets a
        worker bounce a suspended group's message without spinning on it."""
        ...

    def close(self) -> None:
        """Release any backend resources (connection/client/session)."""
        ...
