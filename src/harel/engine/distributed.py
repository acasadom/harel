"""Distributed execution (sync API): stateless workers drive Executions off a `Transport`.

The pure engine is unchanged; this only changes *how events move*: a `Worker` loops
`claim`→`load`→dedupe→`route`→`ack`; the transport guarantees one in-flight message per
group, so each Execution is driven by at most one worker at a time. `DistributedRunner` is the
sending side — create/start/send and the control plane — and never runs a machine's actions.

Their logic is `harel.engine.hosting.SenderLogic` / `WorkerLogic`; `execution=` picks how it
runs:

- `"background"` (the default) — on the shared background event loop (`harel.engine.aio.
  facade`), as `AsyncDistributedRunner` / `AsyncWorker`: each call blocks until the loop has
  done it. A sync store/transport is adapted to the async interface (delegating to the same
  object); async backends may be passed directly.
- `"inline"` — in the caller's own thread, with no event loop: every store and transport call
  (and, in a worker, every action) runs right there, inside the caller's transaction. It takes
  a sync store and transport, and refuses a coroutine action.

`Worker.run(stop)` is a plain sync loop over `step()` in either mode, so it runs in the
caller's thread and honours a `threading.Event`. For native async concurrency (many events in
flight on one loop) use `harel.engine.aio.distributed.AsyncWorker` directly. Calling the sync
facade from inside a running event loop is refused (use the async API).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Iterable, Optional

from harel.definition.model import Definition
from harel.engine.execution import Execution
from harel.engine.flow import Flow, run_inline
from harel.engine.hosting import (  # noqa: F401 (re-exported)
    SenderLogic,
    WorkerLogic,
    _register_submachines,
    _resolve_machine,
    check_execution,
    log_handling_failed,
    log_loop_step_failed,
    log_nack_failed,
)
from harel.engine.resolve import MachineResolver
from harel.engine.transport import Lease
from harel.spec.states import Event


class Worker:
    """A sync worker: `step()` handles one claim→route→ack — by `WorkerLogic`, inline or on
    the background loop as `AsyncWorker` (see the module docstring); `run(stop)` is a plain
    sync loop over `step()`/`fire_due_timers()` honouring a `threading.Event`, one message
    at a time (`concurrency` is `AsyncWorker.run`'s; this loop doesn't use it)."""

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
        trace: bool = False,
        *,
        execution: str = "background",
        machines: Optional[Iterable[str]] = None,
        action_modules: Iterable[str] = (),
        actions: Optional[dict] = None,
        guards: Optional[dict] = None,
    ) -> None:
        """`machines`, when given, are the only machines this worker claims work for (see
        `WorkerLogic`)."""
        check_execution(execution, ("store", store), ("transport", transport))
        self.store = store
        self.transport = transport
        self.definitions = definitions
        self.worker_id = worker_id
        self.suspend_recheck = suspend_recheck
        self.execution = execution
        if execution == "inline":
            self._inline: Optional[WorkerLogic] = WorkerLogic(
                definitions,
                worker_id,
                visibility,
                suspend_recheck,
                clock,
                resolver,
                trace=trace,
                high_ratio=high_ratio,
                priority_threshold=priority_threshold,
                machines=machines,
                action_modules=action_modules,
                actions=actions,
                guards=guards,
            )
            self._ports = {"store": store, "transport": transport}
            return
        self._inline = None
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
            trace,
            tuple(action_modules),
            actions,
            guards,
            machines,
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
        trace=False,
        action_modules=(),
        actions=None,
        guards=None,
        machines=None,
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
                trace=trace,
                high_ratio=high_ratio,
                priority_threshold=priority_threshold,
                machines=machines,
                action_modules=action_modules,
                actions=actions,
                guards=guards,
            )

        return facade.run(build)

    def _serve_inline(self, flow: Flow) -> Any:
        from harel.engine.aio import facade

        facade._guard_no_running_loop()
        return run_inline(flow, self._ports)

    def step(self) -> bool:
        """Process at most one message. Returns False if nothing was claimable."""
        if self._inline is not None:
            combined = getattr(self.store, "load_for_event", None) is not None
            return self._serve_inline(self._inline.step_flow(combined_load=combined))
        from harel.engine.aio import facade

        return facade.run(self._async.step)

    def fire_due_timers(self) -> int:
        if self._inline is not None:
            return self._serve_inline(self._inline.fire_due_timers_flow())
        from harel.engine.aio import facade

        return facade.run(self._async.fire_due_timers)

    def _claim(self) -> Optional[Lease]:
        if self._inline is not None:
            return self._serve_inline(self._inline.claim_flow())
        from harel.engine.aio import facade

        return facade.run(self._async._claim)

    def _handle(self, lease: Lease) -> None:
        if self._inline is not None:
            combined = getattr(self.store, "load_for_event", None) is not None
            self._serve_inline(self._inline.handle_flow(lease, combined_load=combined))
            return
        from harel.engine.aio import facade

        facade.run(self._async._handle, lease)

    def _nack(self, lease: Lease, delay: float) -> None:
        if self._inline is not None:
            self.transport.nack(lease, delay=delay)
            return
        from harel.engine.aio import facade

        facade.run(self._async.transport.nack, lease, delay=delay)

    def run(self, stop: threading.Event, idle_sleep: float = 0.005) -> None:
        """Loop until `stop` is set, one message at a time; when the queue is empty, sweep the
        due timers. Like `AsyncWorker.run`: a message whose handling raises is logged with its
        traceback and nacked to come back after `suspend_recheck` seconds (if the nack fails
        too, it comes back when its lease expires), and a claim or a timer sweep that raises is
        logged and the loop carries on after `suspend_recheck` seconds — nothing stops it but
        `stop`."""
        while not stop.is_set():
            try:
                lease = self._claim()
            except Exception:
                log_loop_step_failed(self.worker_id, "claim", self.suspend_recheck)
                stop.wait(self.suspend_recheck)
                continue
            if lease is None:
                try:
                    fired = self.fire_due_timers()
                except Exception:
                    log_loop_step_failed(self.worker_id, "fire the due timers", self.suspend_recheck)
                    stop.wait(self.suspend_recheck)
                    continue
                if fired == 0:
                    stop.wait(idle_sleep)
                continue
            try:
                self._handle(lease)
            except Exception:
                log_handling_failed(self.worker_id, lease, self.suspend_recheck)
                try:
                    self._nack(lease, self.suspend_recheck)
                except Exception:
                    log_nack_failed(self.worker_id, lease)


class DistributedRunner:
    """The sync sending side: `SenderLogic`, inline or on the background loop as
    `AsyncDistributedRunner` (see the module docstring)."""

    def __init__(
        self,
        store: Any,
        transport: Any,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        *,
        execution: str = "background",
        action_modules: Iterable[str] = (),
        actions: Optional[dict] = None,
        guards: Optional[dict] = None,
    ) -> None:
        """`execution` is `"background"` (the shared background event loop, the default)
        or `"inline"` (the caller's own thread, no event loop: create/start/send and the
        control plane write to the store and transport right there, inside the caller's
        transaction, over sync backends) — see the module docstring. Its `worker()` uses the
        same execution model."""
        check_execution(execution, ("store", store), ("transport", transport))
        self.store = store
        self.transport = transport
        self.definitions = definitions
        self.resolver = resolver
        self._clock = clock
        self._trace = trace
        self.execution = execution
        self.action_modules = tuple(action_modules)
        self.actions, self.guards = actions, guards
        if execution == "inline":
            self._inline: Optional[SenderLogic] = SenderLogic(
                definitions,
                clock,
                resolver,
                trace,
                shares_store_transaction=getattr(transport, "shares_store_transaction", False),
                action_modules=self.action_modules,
                actions=self.actions,
                guards=self.guards,
            )
            self._ports = {"store": store, "transport": transport}
            return
        self._inline = None
        self._async = self._portal_build(
            store, transport, definitions, clock, resolver, trace, self.action_modules, actions, guards
        )

    @staticmethod
    def _portal_build(
        store,
        transport,
        definitions,
        clock,
        resolver,
        trace=False,
        action_modules=(),
        actions=None,
        guards=None,
    ):
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
                action_modules=action_modules,
                actions=actions,
                guards=guards,
            )

        return facade.run(build)

    def _do(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Run operation `name` — `SenderLogic.<name>_flow` inline, or the async runner's
        `<name>` on the background loop."""
        from harel.engine.aio import facade

        if self._inline is not None:
            facade._guard_no_running_loop()
            return run_inline(getattr(self._inline, f"{name}_flow")(*args, **kwargs), self._ports)
        return facade.run(getattr(self._async, name), *args, **kwargs)

    def create(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
        start_on_create: bool = True,
        *,
        version: Optional[int] = None,
    ) -> Execution:
        """Create an Execution — never runs its actions on this caller (see
        `AsyncDistributedRunner.create`). By default (`start_on_create=True`) this
        also publishes `Start`, so a worker picks it up right away; pass `False`
        to defer that and call `start(execution_id)` yourself later. `version` runs that
        version of the machine (it must be loaded) instead of the highest."""
        return self._do(
            "create", definition_id, context, execution_id, priority, start_on_create, version=version
        )

    def start(self, execution_id: str, data: Optional[dict] = None) -> None:
        """Publish a `Start` for `execution_id` — a worker runs it, not this caller.
        No-op (logged) if it's already been started. `data`, when given, seeds the
        context before the machine runs (start-with-parameters) — the only
        sanctioned way to attach a payload to `Start`; `send()` refuses the kind
        outright."""
        self._do("start", execution_id, data)

    def send(self, execution_id: str, event: Event) -> None:
        """Publish a domain event — a worker processes it, not this caller. Refuses
        a caller-supplied `Start` event (raises `ValueError`): use `create()` or
        `start(execution_id, data=...)` instead."""
        self._do("send", execution_id, event)

    def worker(
        self,
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Optional[Callable[[], float]] = None,
        concurrency: int = 256,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
        *,
        machines: Optional[Iterable[str]] = None,
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
            trace=self._trace,
            execution=self.execution,
            machines=machines,
            action_modules=self.action_modules,
            actions=self.actions,
            guards=self.guards,
        )

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> None:
        self._do("cancel", execution_id, reason=reason)

    def terminate(self, execution_id: str) -> None:
        self._do("terminate", execution_id)

    def suspend(self, execution_id: str) -> None:
        self._do("suspend", execution_id)

    def resume(self, execution_id: str) -> None:
        self._do("resume", execution_id)

    def purge(self, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
        """Permanently delete a finished execution tree (root, regions, invokes and all
        their store rows), passing it to `archive` first if given. Refuses a child or a
        tree with a member not DONE/CANCELLED. False if it no longer exists."""
        return self._do("purge", execution_id, archive=archive)

    def redrive(self, execution_id: str, target_path: str) -> None:
        """Force a dead-lettered (FAILED) `execution_id` back to RUNNING at
        `target_path` (a leaf state you choose). No-op if not FAILED."""
        self._do("redrive", execution_id, target_path)
