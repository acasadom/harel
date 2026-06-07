"""Async distributed execution — the async mirror of `harel.engine.distributed`.

`AsyncTransportDriver` reuses the async `AsyncDriver` but routes deferred effects through
an (async) `Transport` (publish the outbox; fan a domain event out to region groups).
`AsyncWorker` loops claim→load→dedupe→route→ack; its production `run()` drives up to
`concurrency` events in flight at once on one loop (the concurrency win), with per-group
exclusivity inherited from the transport's claim and `StoreConflict`→nack the CAS fence.
`AsyncDistributedRunner` is the façade (create/send/worker + control plane).

Reuses the pure dict helpers from the sync module (`_defn_for`/`_register_submachines`/
`_resolve_machine`) — they do no IO. The control plane is the async one (`aio.control`).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine.aio import control
from harel.engine.aio.driver import _AsyncRuntimeDriver
from harel.engine.distributed import _defn_for, _register_submachines, _resolve_machine
from harel.engine.execution import Execution, Status
from harel.engine.resolve import MachineResolver
from harel.engine.runtime import _CONTROL
from harel.engine.store import StoreConflict
from harel.spec.states import Event


class AsyncTransportDriver(_AsyncRuntimeDriver):
    """Async `Driver` whose deferred effects flow through a `Transport`: `_flush` publishes
    the outbox; `route` fans a domain event out to the regions' groups (or runs the engine
    when there are no live regions)."""

    def __init__(
        self,
        defn: Definition,
        store: Any,
        transport: Any,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
    ) -> None:
        super().__init__(defn, store, clock, definitions=definitions, resolve_machine=resolve_machine)
        self.transport = transport

    async def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        await self.transport.publish(execution_id, event)

    async def _flush(self) -> None:
        while True:
            progressed = False
            for spawn in await self.store.pending_spawns():
                await self._create_spawn(spawn)
                await self.store.ack_spawn(spawn.seq)
                progressed = True
            for entry in await self.store.pending_outbox():
                if entry.target_id is not None:
                    await self.transport.publish(entry.target_id, entry.event)
                await self.store.ack_outbox(entry.seq)
                progressed = True
            if not progressed:
                return

    async def route(self, exe: Execution, event: Event) -> None:
        live = [
            child
            for cid, cs in exe.children.items()
            if not cs.finished and not cs.submachine and (child := await self.store.load(cid)) is not None
        ]
        if event.kind not in _CONTROL and live:
            for child in live:
                await self.transport.publish(child.id, event)
            await self.store.commit(exe, [], processed_event_id=event.id)
        else:
            await self._run(exe, engine.process(self.defn, exe, event), event_id=event.id)
        await self._flush()


class AsyncWorker:
    """Async event loop over a store + transport. `step()` processes at most one message
    (mirrors the sync `Worker.step`); `run()` drives up to `concurrency` in flight at once."""

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
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self.worker_id = worker_id
        self.visibility = visibility
        self.suspend_recheck = suspend_recheck
        self._clock = clock
        self.concurrency = concurrency

    def _driver(self, exe: Execution) -> AsyncTransportDriver:
        return AsyncTransportDriver(
            _defn_for(self.definitions, self.resolver, exe),
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
        )

    async def _handle(self, lease) -> bool:
        exe = await self.store.load(lease.group_id)
        if exe is None or await self.store.is_processed(exe.id, lease.event.id):
            await self.transport.ack(lease)
            return True
        if exe.status is Status.CANCELLED:
            await self.transport.ack(lease)
            return True
        if exe.status is Status.SUSPENDED:
            await self.transport.nack(lease, delay=self.suspend_recheck)
            return True
        if exe.status is Status.CANCELLING and lease.event.kind != "Cancel":
            await self.transport.ack(lease)
            return True
        try:
            await self._driver(exe).route(exe, lease.event)
        except StoreConflict:
            await self.transport.nack(lease)
            return True
        await self.transport.ack(lease)
        return True

    async def step(self) -> bool:
        """Process at most one message. Returns False if nothing was claimable."""
        lease = await self.transport.claim(self.worker_id, self.visibility)
        if lease is None:
            return False
        return await self._handle(lease)

    async def fire_due_timers(self) -> int:
        fired = 0
        for execution_id, path, fire_at in await self.store.due_timers(self._clock()):
            await self.transport.publish(execution_id, engine.timeout_event(execution_id, path, fire_at))
            await self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    async def run(self, stop: asyncio.Event, idle_sleep: float = 0.005) -> None:
        """Loop until `stop` is set, driving up to `concurrency` events in flight at once.
        Per-group exclusivity (one in-flight per group) is the transport's claim; the
        semaphore caps total concurrency. When the queue is empty, sweep due timers."""
        sem = asyncio.Semaphore(self.concurrency)
        pending: set[asyncio.Task] = set()

        async def _run_one(lease) -> None:
            try:
                await self._handle(lease)
            finally:
                sem.release()

        while not stop.is_set():
            await sem.acquire()
            lease = await self.transport.claim(self.worker_id, self.visibility)
            if lease is None:
                sem.release()
                if await self.fire_due_timers() == 0:
                    if pending:
                        await asyncio.wait(pending, timeout=idle_sleep, return_when=asyncio.FIRST_COMPLETED)
                    else:
                        try:
                            await asyncio.wait_for(_wait_event(stop), timeout=idle_sleep)
                        except asyncio.TimeoutError:
                            pass
                continue
            task = asyncio.create_task(_run_one(lease))
            pending.add(task)
            task.add_done_callback(pending.discard)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _wait_event(stop: asyncio.Event) -> None:
    await stop.wait()


class AsyncDistributedRunner:
    """Façade over an async store + transport + Definition registry."""

    def __init__(
        self,
        store: Any,
        transport: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self._clock = clock

    def _transport_driver(self, defn: Definition) -> AsyncTransportDriver:
        return AsyncTransportDriver(
            defn,
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
        )

    async def create(self, definition_id: str, context: Optional[dict] = None) -> Execution:
        exe = Execution(definition_id=definition_id, context=dict(context or {}))
        await self._transport_driver(self.definitions[definition_id]).start(exe)
        loaded = await self.store.load(exe.id)
        assert loaded is not None
        return loaded

    async def send(self, execution_id: str, event: Event) -> None:
        await self.transport.publish(execution_id, event)

    def worker(
        self,
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
        concurrency: int = 256,
    ) -> AsyncWorker:
        return AsyncWorker(
            self.store,
            self.transport,
            self.definitions,
            worker_id,
            visibility,
            suspend_recheck,
            clock or self._clock,
            resolver=self.resolver,
            concurrency=concurrency,
        )

    # --- control plane ------------------------------------------------------
    async def _driver_for(self, execution_id: str) -> AsyncTransportDriver:
        exe = await self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        return self._transport_driver(_defn_for(self.definitions, self.resolver, exe))

    async def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> None:
        driver = await self._driver_for(execution_id)
        await control.cancel(self.store, driver.defn, execution_id, reason=reason)
        await driver._flush()  # publish the injected Cancel (if any) to the transport

    async def terminate(self, execution_id: str) -> None:
        await control.terminate(self.store, execution_id)

    async def suspend(self, execution_id: str) -> None:
        await control.suspend(self.store, execution_id)

    async def resume(self, execution_id: str) -> None:
        await control.resume(self.store, execution_id)
