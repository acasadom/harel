"""The async driver: the engine's driver logic (`harel.engine.driving`) run with coroutines.

`AsyncDriver` is `DriverLogic` served by the coroutine interpreter (`flow.run_async`): every
IO request its flows yield — a store call, an action, independent work in parallel — is
awaited. A coroutine action is awaited; a plain sync action runs in the default thread pool
(`run_in_executor`) so a blocking sync action doesn't freeze the loop; parallel work (child
creations, a broadcast to regions) overlaps on the loop. The engine generator itself stays
synchronous — `next(gen)`/`gen.send(...)` are CPU between awaits.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from harel.definition.model import Definition
from harel.engine.driving import DriverLogic, FailOnActionError
from harel.engine.execution import Execution
from harel.engine.flow import Flow, run_async
from harel.spec.states import Event

logger = logging.getLogger(__name__)


class AsyncDriver(DriverLogic):
    """Async in-memory runtime: drives Executions through the pure engine over an async
    store. The bare driver propagates action errors (so parity tests surface bugs)."""

    def __init__(
        self,
        defn: Definition,
        store: Any,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
        trace: bool = False,
    ) -> None:
        super().__init__(defn, clock, definitions, resolve_machine, trace)
        self.store = store

    def _ports(self) -> dict[str, Any]:
        """What this driver's flows do IO on."""
        return {"store": self.store}

    async def _serve(self, flow: Flow) -> Any:
        return await run_async(flow, self._ports())

    # --- the flows, awaited --------------------------------------------------
    async def _run(
        self, exe: Execution, gen, event_id: Optional[str] = None, event: Optional[Event] = None
    ) -> bool:
        return await self._serve(self._run_flow(exe, gen, event_id, event))

    async def _drive(self, exe: Execution, gen, original_exc: Optional[Exception] = None):
        return await self._serve(self._drive_flow(exe, gen, original_exc))

    async def _create_spawn(self, entry) -> None:
        await self._serve(self._create_spawn_flow(entry))

    async def _flush(self) -> None:
        await self._serve(self._flush_flow())

    async def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        await self._serve(self._deliver_timeout_flow(execution_id, event))

    async def _rearm_ttl(self, exe: Execution, event: Event) -> None:
        await self._serve(self._rearm_ttl_flow(exe, event))

    async def fire_due_timers(self) -> int:
        """Deliver every timer due now (a `Timeout` to its execution) and remove it.
        Returns how many fired."""
        return await self._serve(self.fire_due_timers_flow())

    # --- public API --------------------------------------------------------
    async def recover(self) -> None:
        await self._flush()

    async def start(self, exe: Execution) -> None:
        await self._serve(self.start_flow(exe))

    async def inject(self, exe: Execution, event: Event) -> None:
        await self._serve(self.inject_flow(exe, event))


class _AsyncRuntimeDriver(FailOnActionError, AsyncDriver):
    """The production driver: `AsyncDriver` with the dead-letter policy (`FailOnActionError`).
    Used by AsyncDurableRunner and AsyncWorker."""
