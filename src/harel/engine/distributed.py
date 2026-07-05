"""Distributed execution (sync API): stateless workers drive Executions off a `Transport`.

`DistributedRunner` and `Worker` are now thin **synchronous facades** over the async core
(`harel.engine.aio.distributed`), bridged by the shared anyio portal (one background loop —
see `harel.engine.aio.facade`). The pure engine is unchanged; this only changes *how events
move*: a `Worker` loops `claim`→`load`→dedupe→`route`→`ack`; the transport guarantees one
in-flight message per group, so each Execution is driven by at most one worker at a time.

The facade `Worker.run(stop)` is a plain sync loop over `step()` (each `step` bridges to the
async worker via the portal), so it runs in the caller's thread and honours a `threading.Event`
without any threading↔asyncio event translation. For native async concurrency (many events in
flight on one loop) use `harel.engine.aio.distributed.AsyncWorker` directly.

A sync store/transport passed here is adapted to the async interface (delegating to the same
object); pass async backends directly for the native path. Calling the sync facade from inside
a running event loop is refused (use the async API).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Optional

from harel.definition.model import Definition
from harel.engine.execution import Execution
from harel.engine.resolve import MachineResolver, ResolveError
from harel.spec.states import Event


def _register_submachines(definitions: dict) -> None:
    """Fold every registered Definition's inline `invoke` targets into `definitions`
    by id (= their synthetic FQN), so they resolve without an external resolver."""
    for defn in list(definitions.values()):
        definitions.update({s.id: s for s in defn.submachines.values()})


def _resolve_machine(definitions: dict, resolver: Optional[MachineResolver], fqn: str) -> Definition:
    """Resolve a submachine FQN and register it in `definitions` (so the child then
    routes by its own id). Inline targets (id == FQN) are already registered; an
    external FQN goes through the resolver. Raises if neither has it."""
    if fqn in definitions:  # an inline submachine (id == synthetic FQN)
        return definitions[fqn]
    if resolver is None:
        raise ResolveError(f"invoke {fqn!r} but this runner has no machine resolver")
    defn = resolver.resolve(fqn)
    definitions[defn.id] = defn
    return defn


def _defn_for(definitions: dict, resolver: Optional[MachineResolver], exe: Execution) -> Definition:
    """The Definition to drive `exe`: from the registry, or — for a submachine child
    whose Definition this worker has not built yet — lazily resolved by its persisted
    FQN (the spawning worker may be a different process)."""
    defn = definitions.get(exe.definition_id)
    if defn is None and exe.definition_fqn is not None:
        defn = _resolve_machine(definitions, resolver, exe.definition_fqn)
    if defn is None:
        raise KeyError(exe.definition_id)
    return defn


class Worker:
    """Sync facade over `aio.distributed.AsyncWorker` (same constructor as the old sync
    Worker, so direct construction keeps working): `step()` bridges one claim→route→ack to
    the async worker; `run(stop)` is a plain sync loop over `step()`/`fire_due_timers()`
    honouring a `threading.Event` (so it runs in a thread without event translation). A sync
    store/transport is adapted to the async interface (delegating to the same object)."""

    def __init__(
        self,
        store: Any,
        transport: Any,
        definitions: dict[str, Definition],
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        concurrency: int = 256,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        self.worker_id = worker_id
        self._async = self._portal_build(
            store,
            transport,
            definitions,
            worker_id,
            visibility,
            suspend_recheck,
            clock,
            resolver,
            concurrency,
            high_ratio,
            priority_threshold,
        )

    @staticmethod
    def _portal_build(
        store,
        transport,
        definitions,
        worker_id,
        visibility,
        suspend_recheck,
        clock,
        resolver,
        concurrency,
        high_ratio=0.0,
        priority_threshold=1,
    ):
        from harel.engine.aio import facade

        async def build():
            from harel.engine.aio.distributed import AsyncWorker

            return AsyncWorker(
                facade.as_async_store(store),
                facade.as_async_transport(transport),
                definitions,
                worker_id,
                visibility,
                suspend_recheck,
                clock,
                resolver,
                concurrency,
                high_ratio=high_ratio,
                priority_threshold=priority_threshold,
            )

        return facade.run(build)

    def step(self) -> bool:
        from harel.engine.aio import facade

        return facade.run(self._async.step)

    def fire_due_timers(self) -> int:
        from harel.engine.aio import facade

        return facade.run(self._async.fire_due_timers)

    def run(self, stop: threading.Event, idle_sleep: float = 0.005) -> None:
        while not stop.is_set():
            if self.step():
                continue
            if self.fire_due_timers() == 0:
                stop.wait(idle_sleep)


class DistributedRunner:
    """Sync facade over `aio.distributed.AsyncDistributedRunner`."""

    def __init__(
        self,
        store: Any,
        transport: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        self.resolver = resolver
        self._clock = clock
        self._async = self._portal_build(store, transport, definitions, clock, resolver, trace)

    @staticmethod
    def _portal_build(store, transport, definitions, clock, resolver, trace=False):
        from harel.engine.aio import facade

        async def build():
            from harel.engine.aio.distributed import AsyncDistributedRunner

            return AsyncDistributedRunner(
                facade.as_async_store(store),
                facade.as_async_transport(transport),
                definitions,
                clock,
                resolver,
                trace=trace,
            )

        return facade.run(build)

    def create(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
    ) -> Execution:
        from harel.engine.aio import facade

        return facade.run(self._async.create, definition_id, context, execution_id, priority)

    def send(self, execution_id: str, event: Event) -> None:
        from harel.engine.aio import facade

        facade.run(self._async.send, execution_id, event)

    def worker(
        self,
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
        concurrency: int = 256,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
    ) -> Worker:
        return Worker(
            self.store,
            self.transport,
            self.definitions,
            worker_id,
            visibility,
            suspend_recheck,
            clock or self._clock,
            self.resolver,
            concurrency,
            high_ratio=high_ratio,
            priority_threshold=priority_threshold,
        )

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> None:
        from harel.engine.aio import facade

        facade.run(self._async.cancel, execution_id, reason=reason)

    def terminate(self, execution_id: str) -> None:
        from harel.engine.aio import facade

        facade.run(self._async.terminate, execution_id)

    def suspend(self, execution_id: str) -> None:
        from harel.engine.aio import facade

        facade.run(self._async.suspend, execution_id)

    def resume(self, execution_id: str) -> None:
        from harel.engine.aio import facade

        facade.run(self._async.resume, execution_id)
