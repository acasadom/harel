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
import random
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
from harel.engine.transport import Lease
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
        trace: bool = False,
    ) -> None:
        super().__init__(
            defn, store, clock, definitions=definitions, resolve_machine=resolve_machine, trace=trace
        )
        self.transport = transport

    async def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        exe = await self.store.load(execution_id)
        priority = exe.priority if exe is not None else 0
        await self.transport.publish(execution_id, event, priority=priority)

    async def start(self, exe: Execution) -> None:
        await self._run(exe, engine.core.start(self.defn, exe))
        await self._flush(primary_priority={exe.id: exe.priority})

    async def _flush(self, primary_priority: Optional[dict[str, int]] = None) -> None:
        while True:
            progressed = False
            for spawn in await self.store.pending_spawns():
                await self._create_spawn(spawn)
                await self.store.ack_spawn(spawn.seq)
                progressed = True
            for entry in await self.store.pending_outbox():
                if entry.target_id is not None:
                    # self-targeted re-publish uses this exe's priority (primary_priority);
                    # a cross-execution emit (e.g. a region's Finished -> parent) uses the
                    # TARGET's own priority, not 0, so it doesn't pin the target's group.
                    prio = (primary_priority or {}).get(entry.target_id)
                    if prio is None:
                        target = await self.store.load(entry.target_id)
                        prio = target.priority if target is not None else 0
                    await self.transport.publish(entry.target_id, entry.event, priority=prio)
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
                await self.transport.publish(child.id, event, priority=child.priority)
            await self.store.commit(exe, [], processed_event_id=event.id)
            enqueued = False  # broadcast went straight to the transport; nothing in the outbox
        else:
            enqueued = await self._run(exe, engine.process(self.defn, exe, event), event_id=event.id)
        # only run the relay (its HGETALL round-trips) when this event actually enqueued
        # outbox/spawn work — most events emit nothing. Orphans from a crash are still drained
        # by the next emitting event's relay and by `recover()` on startup (the idle loop never
        # flushed either, so this does not change the at-least-once guarantee).
        if enqueued:
            await self._flush(primary_priority={exe.id: exe.priority})


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
        trace: bool = False,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
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
        self._trace = trace  # opt-in execution timeline, threaded to each per-execution driver
        self.high_ratio = high_ratio
        self.priority_threshold = priority_threshold

    def _driver(self, exe: Execution) -> AsyncTransportDriver:
        return AsyncTransportDriver(
            _defn_for(self.definitions, self.resolver, exe),
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )

    async def _load_for_event(self, execution_id: str, event_id: str) -> tuple[Any, bool]:
        """Load the Execution and its dedupe flag. One round-trip if the store offers
        `load_for_event`; otherwise fall back to `load` + `is_processed` (two round-trips)."""
        combined = getattr(self.store, "load_for_event", None)
        if combined is not None:
            return await combined(execution_id, event_id)
        exe = await self.store.load(execution_id)
        processed = exe is not None and await self.store.is_processed(execution_id, event_id)
        return exe, processed

    async def _handle(self, lease) -> bool:
        exe, processed = await self._load_for_event(lease.group_id, lease.event.id)
        if exe is None or processed:
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

    async def _claim(self) -> Optional[Lease]:
        """Claim one message, applying the high_ratio/priority_threshold policy.
        When high_ratio>0, tries priority>=threshold first; falls back to any priority
        so the worker isn't idle when no high-priority work is available."""
        if self.high_ratio > 0 and random.random() < self.high_ratio:
            lease = await self.transport.claim(self.worker_id, self.visibility, self.priority_threshold)
            if lease is not None:
                return lease
        return await self.transport.claim(self.worker_id, self.visibility)

    async def step(self) -> bool:
        """Process at most one message. Returns False if nothing was claimable."""
        lease = await self._claim()
        if lease is None:
            return False
        return await self._handle(lease)

    async def fire_due_timers(self) -> int:
        fired = 0
        for execution_id, path, fire_at in await self.store.due_timers(self._clock()):
            # publish at the execution's own priority: for a machine that parks on a
            # `timeout:` state, this Timeout is the FIRST publish to its group, so it
            # sets the group's priority — dropping it here would pin the group to 0.
            exe = await self.store.load(execution_id)
            priority = exe.priority if exe is not None else 0
            await self.transport.publish(
                execution_id, engine.timeout_event(execution_id, path, fire_at), priority=priority
            )
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
            lease = await self._claim()
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
        trace: bool = False,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self._clock = clock
        self._trace = trace

    def _transport_driver(self, defn: Definition) -> AsyncTransportDriver:
        return AsyncTransportDriver(
            defn,
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )

    async def create(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
    ) -> Execution:
        if execution_id is not None and await self.store.load(execution_id) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        exe = Execution(
            definition_id=definition_id,
            context=dict(context or {}),
            priority=priority,
            **({"id": execution_id} if execution_id is not None else {}),
        )
        await self._transport_driver(self.definitions[definition_id]).start(exe)
        loaded = await self.store.load(exe.id)
        assert loaded is not None
        return loaded

    async def send(self, execution_id: str, event: Event) -> None:
        exe = await self.store.load(execution_id)
        priority = exe.priority if exe is not None else 0
        await self.transport.publish(execution_id, event, priority=priority)

    def worker(
        self,
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
        concurrency: int = 256,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
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
            trace=self._trace,
            high_ratio=high_ratio,
            priority_threshold=priority_threshold,
        )

    # --- control plane ------------------------------------------------------
    async def _driver_for(self, execution_id: str) -> tuple[AsyncTransportDriver, "Execution"]:
        exe = await self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        return self._transport_driver(_defn_for(self.definitions, self.resolver, exe)), exe

    async def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> None:
        driver, exe = await self._driver_for(execution_id)
        await control.cancel(self.store, driver.defn, execution_id, reason=reason)
        await driver._flush(primary_priority={execution_id: exe.priority})

    async def terminate(self, execution_id: str) -> None:
        await control.terminate(self.store, execution_id)

    async def suspend(self, execution_id: str) -> None:
        await control.suspend(self.store, execution_id)

    async def resume(self, execution_id: str) -> None:
        await control.resume(self.store, execution_id)
