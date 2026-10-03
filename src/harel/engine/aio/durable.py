"""Async durable host: the durable runner's logic (`harel.engine.hosting.DurableLogic`) run
with coroutines, over an `AsyncExecutionStore`.

Drives bare Executions through the async engine, checkpointing at every event boundary.
Same contract as the sync `DurableRunner`, every public method `async def`. The sync
`DurableRunner` runs on this with `execution="background"` (the default).
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

from harel.definition.model import Definition
from harel.engine.aio import control
from harel.engine.execution import Execution
from harel.engine.flow import Flow, run_async
from harel.engine.hosting import ControlPort, DurableLogic
from harel.engine.resolve import MachineResolver
from harel.spec.states import Event


class AsyncDurableRunner(DurableLogic):
    def __init__(
        self,
        store: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        on_action_error: str = "fail",
    ) -> None:
        super().__init__(definitions, clock, resolver, trace, on_action_error)
        self.store = store

    async def _serve(self, flow: Flow) -> Any:
        return await run_async(flow, {"store": self.store, "control": ControlPort(control, self.store)})

    async def create(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
    ) -> Execution:
        return await self._serve(self.create_flow(definition_id, context, execution_id, priority))

    async def process(self, execution_id: str, event: Event) -> Execution:
        return await self._serve(self.process_flow(execution_id, event))

    async def recover(self, definition_id: str) -> None:
        await self._serve(self.recover_flow(definition_id))

    async def fire_due_timers(self) -> int:
        return await self._serve(self.fire_due_timers_flow())

    # --- control plane ------------------------------------------------------
    async def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> Execution:
        return await self._serve(self.cancel_flow(execution_id, reason))

    async def terminate(self, execution_id: str) -> Execution:
        return await self._serve(self.terminate_flow(execution_id))

    async def suspend(self, execution_id: str) -> Execution:
        return await self._serve(self.suspend_flow(execution_id))

    async def resume(self, execution_id: str) -> Execution:
        return await self._serve(self.resume_flow(execution_id))

    async def purge(self, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
        """Permanently delete a finished execution tree, archiving it first if `archive`
        is given — see `control.purge`. False if it no longer exists."""
        return await self._serve(self.purge_flow(execution_id, archive))

    async def redrive(self, execution_id: str, target_path: str) -> Execution:
        """Force a FAILED `execution_id` back to RUNNING at `target_path` (a leaf
        the caller picks — see `control.redrive`). Use once the bug that dead-
        lettered it is fixed; context is untouched."""
        return await self._serve(self.redrive_flow(execution_id, target_path))
