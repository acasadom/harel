"""Shared async-transport contract: the `AsyncTransport` Protocol. The concrete
async backends live in sibling modules and are re-exported by the package `__init__`.
The `_PARKED`/`Lease` primitives are reused from the sync `harel.engine.transport`."""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

from harel.engine.transport import Lease
from harel.spec.states import Event


@runtime_checkable
class AsyncTransport(Protocol):
    """Async mirror of `Transport`: identical per-group-exclusivity semantics, awaited IO."""

    async def publish(self, group_id: str, event: Event) -> None: ...

    async def claim(self, worker_id: str, visibility: float) -> Optional[Lease]: ...

    async def ack(self, lease: Lease) -> None: ...

    async def nack(self, lease: Lease, delay: float = 0.0) -> None: ...

    async def close(self) -> None: ...
