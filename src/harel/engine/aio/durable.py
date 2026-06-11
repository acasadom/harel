"""Async durable host — the async mirror of `harel.engine.durable.DurableRunner`.

Drives bare Executions through the async engine over an `AsyncExecutionStore`,
checkpointing at every event boundary. Same contract as the sync `DurableRunner`, every
public method `async def`. The sync `DurableRunner` is a thin anyio facade over this.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine.aio import control
from harel.engine.aio.driver import _AsyncRuntimeDriver
from harel.engine.execution import Execution
from harel.engine.resolve import MachineResolver, ResolveError
from harel.spec.states import Event


class AsyncDurableRunner:
    def __init__(
        self,
        store: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
    ) -> None:
        self.store = store
        self.definitions = definitions
        for defn in list(definitions.values()):
            self.definitions.update({s.id: s for s in defn.submachines.values()})
        self._clock = clock
        self.resolver = resolver
        self._trace = trace

    def _resolve_machine(self, fqn: str) -> Definition:
        if fqn in self.definitions:
            return self.definitions[fqn]
        if self.resolver is None:
            raise ResolveError(f"invoke {fqn!r} but AsyncDurableRunner has no resolver")
        defn = self.resolver.resolve(fqn)
        self.definitions[defn.id] = defn
        return defn

    def _driver(self, definition_id: str) -> _AsyncRuntimeDriver:
        return _AsyncRuntimeDriver(
            self.definitions[definition_id],
            store=self.store,
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=self._resolve_machine,
            trace=self._trace,
        )

    async def create(self, definition_id: str, context: Optional[dict] = None) -> Execution:
        exe = Execution(definition_id=definition_id, context=dict(context or {}))
        await self._driver(definition_id).start(exe)
        loaded = await self.store.load(exe.id)
        assert loaded is not None
        return loaded

    async def process(self, execution_id: str, event: Event) -> Execution:
        exe = await self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        await self._driver(exe.definition_id).inject(exe, event)
        loaded = await self.store.load(execution_id)
        assert loaded is not None
        return loaded

    async def recover(self, definition_id: str) -> None:
        await self._driver(definition_id).recover()

    async def fire_due_timers(self) -> int:
        fired = 0
        for execution_id, path, fire_at in await self.store.due_timers(self._clock()):
            exe = await self.store.load(execution_id)
            if exe is not None and exe.definition_id in self.definitions:
                event = engine.timeout_event(execution_id, path, fire_at)
                await self._driver(exe.definition_id)._deliver_timeout(execution_id, event)
            await self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    # --- control plane ------------------------------------------------------
    async def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> Execution:
        exe = await self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver(exe.definition_id)
        await control.cancel(self.store, driver.defn, execution_id, reason=reason)
        await driver.recover()  # deliver the injected Cancel inline (runs the cleanup)
        loaded = await self.store.load(execution_id)
        assert loaded is not None
        return loaded

    async def terminate(self, execution_id: str) -> Execution:
        await control.terminate(self.store, execution_id)
        loaded = await self.store.load(execution_id)
        assert loaded is not None
        return loaded

    async def suspend(self, execution_id: str) -> Execution:
        await control.suspend(self.store, execution_id)
        loaded = await self.store.load(execution_id)
        assert loaded is not None
        return loaded

    async def resume(self, execution_id: str) -> Execution:
        await control.resume(self.store, execution_id)
        loaded = await self.store.load(execution_id)
        assert loaded is not None
        return loaded
