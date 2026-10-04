"""Shared async-store contract: the `AsyncExecutionStore` Protocol. The concrete
async backends live in sibling modules and are re-exported by the package `__init__`.
The outbox/spawn/timer dataclasses are reused from the sync `harel.engine.store`."""

from __future__ import annotations

from typing import Iterable, Optional, Protocol, runtime_checkable

from harel.engine.execution import Execution, ExecutionPage, Status
from harel.engine.store import OutboxEntry, SpawnEntry, Step, TimerOp
from harel.spec.states import Event


@runtime_checkable
class AsyncExecutionStore(Protocol):
    """Async mirror of `ExecutionStore`: identical semantics, awaited IO. Backend-agnostic,
    so the deferred backends (rqlite/sqs/mongo/dynamo) slot in unchanged later."""

    async def load(self, execution_id: str) -> Optional[Execution]: ...

    async def list_executions(
        self,
        *,
        status: Optional[Iterable[Status]] = None,
        definition_id: Optional[str] = None,
        roots_only: bool = False,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> ExecutionPage: ...

    async def save(self, exe: Execution) -> None: ...

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: "tuple[TimerOp, ...]" = (),
        spawns: "tuple[tuple[str, str, dict], ...]" = (),
        trace: "Optional[dict]" = None,
        step: "Optional[Step]" = None,
    ) -> list[int]: ...

    async def is_processed(self, execution_id: str, event_id: str) -> bool: ...

    async def read_trace(self, execution_id: str) -> list[dict]: ...

    async def pending_outbox(self) -> list[OutboxEntry]: ...

    async def ack_outbox(self, seq: int) -> None: ...

    async def pending_spawns(self) -> "list[SpawnEntry]": ...

    async def ack_spawn(self, seq: int) -> None: ...

    async def due_timers(self, now: float) -> "list[tuple[str, str, float]]": ...

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None: ...

    async def ids_with_prefix(self, prefix: str) -> list[str]: ...

    async def purge(self, execution_id: str, expected_version: int) -> bool: ...

    async def close(self) -> None: ...
