"""A headless, durable host for state-machine executions (sync API).

`DurableRunner` is the synchronous public façade over the async core
(`harel.engine.aio.durable.AsyncDurableRunner`). It drives bare Executions through the
pure engine over a persistent `ExecutionStore`, checkpointing at every event boundary;
the `Execution` is the single source of truth, so a run created in one process resumes in
another. Each sync method bridges to the async runner via the shared anyio portal (one
background loop) — see `harel.engine.aio.facade`. A sync store passed here (the common
case, e.g. `SqliteStore("x.db")`) is adapted so the async engine can await it while the
caller keeps introspecting the same store object.

For async callers, use `AsyncDurableRunner` directly (calling this sync façade from inside
a running event loop is refused with a clear error).
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from harel.definition.model import Definition
from harel.engine.aio import facade
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.execution import Execution
from harel.engine.resolve import MachineResolver
from harel.engine.store import ExecutionStore
from harel.spec.states import Event


class DurableRunner:
    def __init__(
        self,
        store: ExecutionStore,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
    ) -> None:
        self.store = store  # kept so callers can introspect the same store object
        self.definitions = definitions
        self.resolver = resolver
        self._clock = clock
        # build the async runner ON the shared portal loop (so async backends, later, bind
        # their connection pools to that loop); a sync store is adapted to the async interface.
        self._async: AsyncDurableRunner = facade.run(self._build, store, definitions, clock, resolver, trace)

    @staticmethod
    async def _build(store, definitions, clock, resolver, trace=False) -> AsyncDurableRunner:
        return AsyncDurableRunner(facade.as_async_store(store), definitions, clock, resolver, trace=trace)

    def create(self, definition_id: str, context: Optional[dict] = None) -> Execution:
        """Create, start and persist a new Execution; return its committed state."""
        return facade.run(self._async.create, definition_id, context)

    def process(self, execution_id: str, event: Event) -> Execution:
        """Load a persisted Execution, feed it one event, return the committed state."""
        return facade.run(self._async.process, execution_id, event)

    def recover(self, definition_id: str) -> None:
        """Drain the durable outbox for `definition_id`'s Executions (relay on restart)."""
        return facade.run(self._async.recover, definition_id)

    def fire_due_timers(self) -> int:
        """Deliver every timer due now inline; returns how many fired."""
        return facade.run(self._async.fire_due_timers)

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> Execution:
        """Cancel (cooperative if the model has `on: Cancel`, else forceful); the
        cooperative cleanup runs inline. `reason` is an opaque payload on the `Cancel`."""
        return facade.run(self._async.cancel, execution_id, reason=reason)

    def terminate(self, execution_id: str) -> Execution:
        """Forcefully cancel `execution_id` now (no cleanup, no hooks)."""
        return facade.run(self._async.terminate, execution_id)

    def suspend(self, execution_id: str) -> Execution:
        """Pause `execution_id` (reversible; state and backlog preserved)."""
        return facade.run(self._async.suspend, execution_id)

    def resume(self, execution_id: str) -> Execution:
        """Resume a suspended `execution_id`, continuing where it stopped."""
        return facade.run(self._async.resume, execution_id)
