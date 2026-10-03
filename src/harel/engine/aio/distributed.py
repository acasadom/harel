"""Async distributed execution — the distributed logic run with coroutines.

The logic lives in `harel.engine.driving` (`TransportDriverLogic`: the relay publishes the
outbox; `route` fans a domain event out to region groups) and `harel.engine.hosting`
(`SenderLogic`: create/start/send; `WorkerLogic`: claim→load→dedupe→route→ack). Here it runs
over an async store + transport: `AsyncTransportDriver`, `AsyncWorker` — whose `run()`
drives up to `concurrency` events in flight at once on one loop, with per-group exclusivity
from the transport's claim and `StoreConflict`→nack as the CAS fence — and
`AsyncDistributedRunner`, the façade (create/send/worker + control plane, `aio.control`).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Optional

from harel.definition.model import Definition
from harel.engine.aio import control
from harel.engine.aio.driver import _AsyncRuntimeDriver
from harel.engine.distributed import _defn_for, _resolve_machine
from harel.engine.driving import TransportDriverLogic
from harel.engine.execution import Execution
from harel.engine.flow import Flow, run_async
from harel.engine.hosting import ControlPort, SenderLogic, WorkerLogic
from harel.engine.resolve import MachineResolver
from harel.engine.transport import Lease
from harel.spec.states import Event

logger = logging.getLogger(__name__)


class AsyncTransportDriver(TransportDriverLogic, _AsyncRuntimeDriver):
    """The transport driver's logic (`TransportDriverLogic`: the relay publishes the outbox;
    `route` fans a domain event out to the regions' groups, or runs the engine when there
    are no live regions) run with coroutines, over an async store and transport."""

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

    def _ports(self) -> dict[str, Any]:
        return {"store": self.store, "transport": self.transport}

    async def _flush(self, primary_priority: Optional[dict[str, int]] = None) -> None:
        await self._serve(self._flush_flow(primary_priority))

    async def route(self, exe: Execution, event: Event) -> None:
        await self._serve(self.route_flow(exe, event))


class AsyncWorker(WorkerLogic):
    """The worker's logic (`harel.engine.hosting.WorkerLogic`) run with coroutines, over an
    async store and transport. `step()` processes at most one message; `run()` drives up to
    `concurrency` in flight at once."""

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
        super().__init__(
            definitions,
            worker_id,
            visibility,
            suspend_recheck,
            clock,
            resolver,
            trace=trace,
            high_ratio=high_ratio,
            priority_threshold=priority_threshold,
        )
        self.store = store
        self.transport = transport
        self.concurrency = concurrency

    async def _serve(self, flow: Flow) -> Any:
        return await run_async(flow, {"store": self.store, "transport": self.transport})

    async def _handle(self, lease) -> bool:
        return await self._serve(
            self.handle_flow(lease, combined_load=getattr(self.store, "load_for_event", None) is not None)
        )

    async def _claim(self) -> Optional[Lease]:
        return await self._serve(self.claim_flow())

    async def step(self) -> bool:
        """Process at most one message. Returns False if nothing was claimable."""
        return await self._serve(
            self.step_flow(combined_load=getattr(self.store, "load_for_event", None) is not None)
        )

    async def fire_due_timers(self) -> int:
        return await self._serve(self.fire_due_timers_flow())

    async def run(self, stop: asyncio.Event, idle_sleep: float = 0.005) -> None:
        """Loop until `stop` is set, driving up to `concurrency` events in flight at once.
        Per-group exclusivity (one in-flight per group) is the transport's claim; the
        semaphore caps total concurrency. When the queue is empty, sweep due timers.

        A message whose handling raises (a store or transport outage, an engine bug — an
        action's own error is the driver's to route, not this) is logged with its traceback
        and nacked to come back after `suspend_recheck` seconds: soon enough for a transient
        outage, without spinning on a failure that persists. If the nack fails too, the
        message comes back when its lease expires."""
        sem = asyncio.Semaphore(self.concurrency)
        pending: set[asyncio.Task] = set()

        async def _run_one(lease) -> None:
            try:
                await self._handle(lease)
            except Exception:
                logger.exception(
                    "worker %s failed handling %s event %s for execution %s; retrying in %ss",
                    self.worker_id,
                    lease.event.kind,
                    lease.event.id,
                    lease.group_id,
                    self.suspend_recheck,
                )
                try:
                    await self.transport.nack(lease, delay=self.suspend_recheck)
                except Exception:
                    logger.exception(
                        "worker %s could not nack %s event %s for execution %s; it is "
                        "redelivered when its lease expires",
                        self.worker_id,
                        lease.event.kind,
                        lease.event.id,
                        lease.group_id,
                    )
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


class AsyncDistributedRunner(SenderLogic):
    """The distributed runner's writing side (`harel.engine.hosting.SenderLogic`) run with
    coroutines, over an async store + transport; `worker()` builds an `AsyncWorker`."""

    def __init__(
        self,
        store: Any,
        transport: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
    ) -> None:
        super().__init__(definitions, clock, resolver, trace)
        self.store = store
        self.transport = transport

    async def _serve(self, flow: Flow) -> Any:
        ports = {
            "store": self.store,
            "transport": self.transport,
            "control": ControlPort(control, self.store),
        }
        return await run_async(flow, ports)

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
        start_on_create: bool = True,
    ) -> Execution:
        """Create an Execution — never runs its actions on this caller: they run on
        whichever worker claims its `Start`. Creating is not starting (unlike
        `AsyncDurableRunner.create`, which runs the initial step inline — that is what a
        single-process host is for).

        `start_on_create` (default `True`) commits that `Start` in this same call, so the
        returned Execution is `PENDING` only until a worker picks it up. That narrows — a
        delivery is still a separate, best-effort step (see `_persist_start_flow`) — a real
        race: with the record persisted but no `Start` queued, a domain event addressed to
        it can't be queued or deferred (only a `RUNNING` execution processes domain events)
        and parking it would deadlock its group behind the missing `Start`. With
        `start_on_create=False` that window is open on purpose: anything sent before your
        own `start(execution_id)` is discarded on arrival, not queued. `create()` and
        `start()` are the only ways to produce a `Start` — `send()` refuses the kind."""
        return await self._serve(
            self.create_flow(definition_id, context, execution_id, priority, start_on_create)
        )

    async def _persist_start(self, exe: Execution, data: Optional[dict] = None) -> None:
        await self._serve(self._persist_start_flow(exe, data))

    async def start(self, execution_id: str, data: Optional[dict] = None) -> None:
        """Publish a `Start` for `execution_id` — a worker claims it and runs the start
        sequence (its actions run there, not on this caller). A no-op, with a warning, if it
        has already started: the engine only acts on a `Start` while `status` is `PENDING`,
        which is what holds under a race; this check is for the common double call.

        `data`, when given, seeds the context before the machine runs (start-with-
        parameters: reserve an id with `create(execution_id=..., start_on_create=False)`,
        then `start(id, data={...})` once they are known). It is the only way to attach a
        payload to `Start`: `send()` refuses the kind, so nobody can race this one with a
        differently-parametrized `Start`."""
        await self._serve(self.start_flow(execution_id, data))

    async def send(self, execution_id: str, event: Event) -> None:
        """Publish a domain event through the transport. Refuses `Start` (raises
        `ValueError`): a status check can't tell a legitimate caller from an unauthorized
        one, so only rejecting the kind closes the gap — `create()`/`start()` are the only
        ways to begin an execution, since only the first `Start` a worker claims counts."""
        await self._serve(self.send_flow(execution_id, event))

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
        await self._serve(self.cancel_flow(execution_id, reason))

    async def terminate(self, execution_id: str) -> None:
        await self._serve(self.terminate_flow(execution_id))

    async def suspend(self, execution_id: str) -> None:
        await self._serve(self.suspend_flow(execution_id))

    async def resume(self, execution_id: str) -> None:
        await self._serve(self.resume_flow(execution_id))

    async def purge(self, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
        """Permanently delete a finished execution tree, archiving it first if `archive`
        is given — see `control.purge`. False if it no longer exists."""
        return await self._serve(self.purge_flow(execution_id, archive))

    async def redrive(self, execution_id: str, target_path: str) -> None:
        """Force a FAILED `execution_id` back to RUNNING at `target_path` (a leaf
        the caller picks — see `control.redrive`)."""
        await self._serve(self.redrive_flow(execution_id, target_path))
