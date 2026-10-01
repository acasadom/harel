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
import logging
import random
import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.events import check_context
from harel.definition.model import Definition
from harel.engine.aio import control
from harel.engine.aio.driver import _AsyncRuntimeDriver
from harel.engine.distributed import _defn_for, _register_submachines, _resolve_machine
from harel.engine.execution import Execution, Status, stamp
from harel.engine.resolve import MachineResolver
from harel.engine.runtime import _CONTROL
from harel.engine.store import StoreConflict, TimerOp
from harel.engine.transport import Lease
from harel.spec.states import Event

logger = logging.getLogger(__name__)


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

    async def _flush(self, primary_priority: Optional[dict[str, int]] = None) -> None:
        while True:
            spawns = await self.store.pending_spawns()
            outbox = await self.store.pending_outbox()
            if not spawns and not outbox:
                return
            if spawns:
                # Each spawn targets a different child_id → independent store rows → safe to
                # run concurrently. For the transport driver, _create_spawn only writes the
                # child record; the initial event is published via the outbox in the next pass.
                await asyncio.gather(*[self._create_spawn(s) for s in spawns])
                await asyncio.gather(*[self.store.ack_spawn(s.seq) for s in spawns])
            for entry in outbox:
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

    async def route(self, exe: Execution, event: Event) -> None:
        live = [
            child
            for cid, cs in exe.children.items()
            if not cs.finished and not cs.submachine and (child := await self.store.load(cid)) is not None
        ]
        if event.kind not in _CONTROL and live:
            for child in live:
                await self.transport.publish(child.id, event, priority=child.priority)
            timers: tuple[TimerOp, ...] = ()
            delay = engine.ttl_delay(self.defn, exe)  # the broadcast is activity for exe's `ttl`
            if delay is not None:
                exe.expires_at = self._clock() + delay
                timers = (TimerOp("schedule", engine.TTL_PATH, exe.expires_at),)
            stamp(exe, self._clock())
            await self.store.commit(exe, [], processed_event_id=event.id, timers=timers)
            enqueued = False  # broadcast went straight to the transport; nothing in the outbox
        else:
            enqueued = await self._run(
                exe, engine.process(self.defn, exe, event), event_id=event.id, event=event
            )
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
        if exe.status is Status.PENDING and lease.event.kind != "Start":
            # not started yet. Only a RUNNING execution processes domain events —
            # this is discarded, not parked: parking would hold this message's
            # group "in flight" (the single-active-consumer lock), blocking the
            # Start itself from ever being claimed behind it — a self-deadlock,
            # not a fix. `create()` defaults to `start_on_create=True`, publishing
            # Start immediately, so this is normally unreachable; it mainly fires
            # for a caller that opted into `start_on_create=False` and got a
            # domain event delivered before its own explicit `start()` call — a
            # risk that choice already accepts (see `AsyncDistributedRunner.create`)
            # — but can also fire, rarely, if Start's own best-effort delivery was
            # delayed by a transient publish failure (`_persist_start`) and a
            # fast-following domain event reached the transport first. Logged
            # (not just silently ack'd) because this discard is otherwise
            # invisible: nothing else records that the event was ever dropped.
            logger.warning(
                "discarding %s event for execution %s: still PENDING (not yet "
                "started) — the sender should retry once it is RUNNING",
                lease.event.kind,
                lease.group_id,
            )
            await self.transport.ack(lease)
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
        due = await self.store.due_timers(self._clock())
        if not due:
            return 0

        async def _fire_one(execution_id: str, path: str, fire_at: float) -> None:
            # publish at the execution's own priority: for a machine that parks on a
            # `timeout:` state, this Timeout is the FIRST publish to its group, so it
            # sets the group's priority — dropping it here would pin the group to 0.
            exe = await self.store.load(execution_id)
            priority = exe.priority if exe is not None else 0
            await self.transport.publish(
                execution_id, engine.timeout_event(execution_id, path, fire_at), priority=priority
            )
            await self.store.delete_timer(execution_id, path, fire_at)

        await asyncio.gather(*[_fire_one(eid, p, fa) for eid, p, fa in due])
        return len(due)

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
        start_on_create: bool = True,
    ) -> Execution:
        """Create an Execution — never runs its actions on this caller. Creating is
        not starting: unlike `AsyncDurableRunner.create` (a synchronous, single-
        process host, where running the initial `on enter` inline is simply what
        it's for), running a machine's actions inline here would defeat the entire
        point of `DistributedRunner` — the work wouldn't be distributed to a worker
        at all, it'd run on whoever called `create`. Either way its actions run on
        whichever worker claims the published `Start`, never on this caller.

        `start_on_create` (default `True`) commits that `Start` immediately, in this
        same call, so the returned Execution is `PENDING` only as a transient detail
        — a worker picks it up right away in the common case. This also narrows (it
        cannot fully close — delivery is still a separate, best-effort step; see
        `_persist_start`) a genuine race: with the record persisted but no `Start`
        ever queued, a domain event addressed to it can't be queued or deferred
        (only a `RUNNING` execution processes domain events — see
        `AsyncWorker._handle`'s `PENDING` branch) and a naive "park it and retry"
        would deadlock (parking holds the execution's single-active-consumer group
        lock, blocking the `Start` behind it from ever being claimed). Passing
        `start_on_create=False` reopens that window deliberately — use it only if
        you have a reason to delay starting past `create()`, and be aware that
        anything you send in the meantime is discarded on arrival, not queued: call
        `start(execution_id)` yourself when ready. `create()` and `start()` are the
        only way to produce a `Start` — `send()` refuses the kind outright, so
        nothing else can race your own delayed start with a differently-
        parametrized one."""
        if execution_id is not None and await self.store.load(execution_id) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        if definition_id not in self.definitions:
            # fail now, synchronously, in the caller's own stack: a bad id must not
            # silently persist a PENDING record that later dies (or loops) on
            # whichever worker eventually claims its Start. Checked after the
            # execution_id collision so a caller retrying with a bad definition_id
            # still learns about an id collision first, not masked by this check.
            raise KeyError(f"unknown definition_id {definition_id!r}")
        # a required field may still come with `start(data=...)` when the start is deferred
        check_context(
            self.definitions[definition_id].context_schema,
            dict(context or {}),
            check_required=start_on_create,
        )
        exe = Execution(
            definition_id=definition_id,
            context=dict(context or {}),
            priority=priority,
            **({"id": execution_id} if execution_id is not None else {}),
        )
        if start_on_create:
            await self._persist_start(exe)
        else:
            stamp(exe, self._clock())
            await self.store.save(exe)
        return exe

    async def _persist_start(self, exe: Execution, data: Optional[dict] = None) -> None:
        """Commit `exe`'s `Start` into the durable outbox — the same atomic write as
        `exe`'s own persistence, exactly how every other emit in this engine reaches
        the transport (see `control.cancel`'s injected `Cancel`) — then attempt a
        direct, best-effort publish of just that one entry.

        Deliberately does NOT call the generic `_flush()`: that drains the store's
        ENTIRE pending outbox/spawns, fleet-wide. Piggybacking on that scan is fine
        when it's already happening for other reasons (`route()`, `cancel()`); here
        it would be pure waste (we already know the one entry we want delivered) and
        actively misleading (a failure publishing some UNRELATED execution's
        backlog, encountered first in that scan, would abort before this exe's own
        entry is even attempted, and get logged as *this* exe's Start failing).

        Once published, the entry is acked, as `_flush` does with each one it
        delivers — otherwise it would sit in the outbox and be published again by the
        next flush.

        A failed direct publish must not raise: `exe` is already durably committed
        above, so the Start stays queued for the next flush anywhere in the fleet to
        pick up (see `route`'s comment on orphan draining) — raising here would cost
        the caller the very id they'd need to retry via `start(execution_id)`. A
        failed ack doesn't raise either: the entry is published again by a later
        flush, and the copy is dropped by the dedupe on the Start's event id."""
        event = Event(kind="Start", data=dict(data or {}))
        stamp(exe, self._clock())
        seqs = await self.store.commit(exe, [(exe.id, event)])
        try:
            await self.transport.publish(exe.id, event, priority=exe.priority)
        except Exception:
            logger.warning(
                "could not immediately publish the Start for execution %s; it is "
                "durably queued and will be delivered by a later flush",
                exe.id,
                exc_info=True,
            )
            return
        try:
            for seq in seqs:
                await self.store.ack_outbox(seq)
        except Exception:
            logger.warning(
                "published the Start for execution %s but could not ack its outbox "
                "entry; a later flush publishes it again, and the copy is dropped",
                exe.id,
                exc_info=True,
            )

    async def start(self, execution_id: str, data: Optional[dict] = None) -> None:
        """Publish a `Start` for `execution_id` — a worker claims it and runs the
        actual start sequence (its actions run there, not on this caller). No-op if
        it's already been started: the engine only ever acts on `Start` while
        `status` is still `PENDING` (see `core.process`), so a redelivered or
        duplicate `Start` can never re-run/reset a live or finished Execution — this
        just warns eagerly, before publishing, for the common case of a caller
        starting the same execution twice; the engine's own check is what actually
        holds under a race (two `start()` calls, or a redelivery).

        `data`, when given, seeds the context before the machine runs (start-with-
        parameters — e.g. `create(execution_id=..., start_on_create=False)` reserves
        an id before the real parameters are known, and `start(id, data={...})`
        supplies them once they are). This is the *only* sanctioned way to attach
        a payload to `Start`: `send()` refuses the kind outright, precisely so a
        caller can't race this one with their own, differently-parametrized
        `Start`."""
        exe = await self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if exe.status is not Status.PENDING:
            logger.warning(
                "start() called on execution %s which is already %s; ignoring",
                execution_id,
                exe.status.value,
            )
            return
        defn = _defn_for(self.definitions, self.resolver, exe)
        check_context(defn.context_schema, {**exe.context, **(data or {})})
        await self._persist_start(exe, data)

    async def send(self, execution_id: str, event: Event) -> None:
        """Publish a domain event through the transport. Refuses `Start` (raises
        `ValueError`): a state check (`status is PENDING?`) can't tell a
        legitimate caller from an unauthorized one — both see the same status, so
        both would pass — only rejecting the *kind* outright, regardless of
        state, closes the gap. `create()`/`start()` are the only sanctioned way
        to begin an execution (optionally with its own `data`); a caller-supplied
        `Start` racing a legitimate one could otherwise seed the wrong context
        into a still-`PENDING` execution, since only the first `Start` any
        worker claims ever counts (see `core.process`)."""
        if event.kind == "Start":
            raise ValueError(
                "send() refuses a caller-supplied Start event — use create() or "
                "start(execution_id, data=...) instead, the only sanctioned way to "
                "begin an execution (optionally with its parameters)"
            )
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
        await control.cancel(self.store, driver.defn, execution_id, reason=reason, clock=self._clock)
        await driver._flush(primary_priority={execution_id: exe.priority})

    async def terminate(self, execution_id: str) -> None:
        await control.terminate(self.store, execution_id, clock=self._clock)

    async def suspend(self, execution_id: str) -> None:
        await control.suspend(self.store, execution_id, clock=self._clock)

    async def resume(self, execution_id: str) -> None:
        await control.resume(self.store, execution_id, clock=self._clock)

    async def purge(self, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
        """Permanently delete a finished execution tree, archiving it first if `archive`
        is given — see `control.purge`. False if it no longer exists."""
        return await control.purge(self.store, execution_id, archive=archive)

    async def redrive(self, execution_id: str, target_path: str) -> None:
        """Force a FAILED `execution_id` back to RUNNING at `target_path` (a leaf
        the caller picks — see `control.redrive`)."""
        driver, _exe = await self._driver_for(execution_id)
        await control.redrive(self.store, driver.defn, execution_id, target_path, clock=self._clock)
