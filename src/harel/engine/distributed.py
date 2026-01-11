"""Distributed execution: stateless workers drive Executions off a `Transport`.

This wires the durable store (state + outbox + dedupe) to the event transport
(single-active-consumer per group). The pure engine is unchanged; this only
changes *how events move*:

- `TransportDriver` reuses the synchronous `Driver` to run one event, but instead
  of delivering the outbox inline (`_flush`) it **publishes** each emitted event
  to the transport (a worker delivers it later). And instead of broadcasting a
  domain event to an orthogonal parent's regions inline, it **publishes** the
  event to each region's group (`route`).
- A `Worker` loops: `claim` the next deliverable message, load its Execution,
  drop it if already processed (dedupe), else `route` it (run-or-fan-out), then
  `ack`. The transport guarantees one in-flight message per group, so each
  Execution is driven by at most one worker at a time — the single-writer the
  store's CAS assumes. N workers run different groups concurrently.
- `DistributedRunner` is the façade: `create` (start an Execution, inline), `send`
  (publish an event), `worker` (a Worker handle to run a loop), plus the control
  plane (`cancel`/`terminate`/`suspend`/`resume`) — lifecycle commands that act on
  the Execution record (via `engine.control`) so they take effect at the next event
  boundary instead of waiting behind the FIFO backlog. The worker honours them:
  CANCELLED drains the backlog as no-ops, SUSPENDED parks it (`nack(delay)`) and
  CANCELLING drains until the injected cooperative `Cancel` reaches the machine.

Single-process today (workers = threads against one sqlite file); the multi-
process variant is the same code with workers in separate processes. Deferred:
the `pending` mid-event resume.

Orthogonal fork is crash-safe via the **spawn-outbox**: the parent commits its
advance + join expectations + the pending child creations atomically; the relay
(`_flush`) creates the children idempotently afterwards (and here publishes their
emits to the transport). So a crash mid-fork neither loses a region's `Finished`
nor hits a re-spawn CAS conflict.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine import control
from harel.engine.execution import Execution, Status
from harel.engine.resolve import MachineResolver, ResolveError
from harel.engine.runtime import _CONTROL, _RuntimeDriver
from harel.engine.store import ExecutionStore, StoreConflict
from harel.engine.transport import Transport
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


class TransportDriver(_RuntimeDriver):
    """A `Driver` whose deferred effects flow through a `Transport` instead of
    inline: `_flush` publishes the outbox; `route` fans a domain event out to the
    regions' groups (or runs the engine when there are no live regions)."""

    def __init__(
        self,
        defn: Definition,
        store: ExecutionStore,
        transport: Transport,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
    ) -> None:
        super().__init__(defn, store, clock, definitions=definitions, resolve_machine=resolve_machine)
        self.transport = transport

    def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        # a fired timer's Timeout is published to its group (a worker runs it),
        # not delivered inline like the base driver.
        self.transport.publish(execution_id, event)

    def _flush(self) -> None:
        # create pending children inline (a fork's children — their own emits go to
        # the outbox), then PUBLISH the outbox to the transport (a worker delivers it
        # later) instead of delivering inline. Loops for nested forks. Any worker may
        # relay any entry; a child re-created is skipped, an event re-published deduped.
        while True:
            progressed = False
            for spawn in self.store.pending_spawns():
                self._create_spawn(spawn)
                self.store.ack_spawn(spawn.seq)
                progressed = True
            for entry in self.store.pending_outbox():
                if entry.target_id is not None:
                    self.transport.publish(entry.target_id, entry.event)
                self.store.ack_outbox(entry.seq)
                progressed = True
            if not progressed:
                return

    def route(self, exe: Execution, event: Event) -> None:
        """Drive one claimed event: a domain event for a parent with live regions
        is fanned out to the region groups (the old broadcast, now via publish);
        otherwise the engine runs it here. Then publish the resulting outbox."""
        # broadcast to live orthogonal regions; a submachine `invoke` child is
        # black-box (never fan a parent domain event into it)
        live = [
            child
            for cid, cs in exe.children.items()
            if not cs.finished and not cs.submachine and (child := self.store.load(cid)) is not None
        ]
        if event.kind not in _CONTROL and live:
            for child in live:
                self.transport.publish(child.id, event)
            self.store.commit(exe, [], processed_event_id=event.id)  # mark the parent routed it
        else:
            self._run(exe, engine.process(self.defn, exe, event), event_id=event.id)
        self._flush()


class Worker:
    """A stateless event-processing loop over a store + transport. Claims one
    message at a time; the transport's per-group exclusivity makes it the sole
    writer of that Execution while it holds the lease."""

    def __init__(
        self,
        store: ExecutionStore,
        transport: Transport,
        definitions: dict[str, Definition],
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver  # FQN -> Definition for submachine `invoke` (optional)
        self.worker_id = worker_id
        self.visibility = visibility
        # how long to park a suspended group's message before re-checking it: long
        # enough not to spin a worker, short enough to bound resume latency.
        self.suspend_recheck = suspend_recheck
        self._clock = clock

    def step(self) -> bool:
        """Process at most one message. Returns False if there was nothing to do
        (the caller may then back off)."""
        lease = self.transport.claim(self.worker_id, self.visibility)
        if lease is None:
            return False
        exe = self.store.load(lease.group_id)
        if exe is None or self.store.is_processed(exe.id, lease.event.id):
            self.transport.ack(lease)  # unknown target or duplicate: nothing to do
            return True
        # control-plane lifecycle (set out-of-band by cancel/suspend/...):
        if exe.status is Status.CANCELLED:
            self.transport.ack(lease)  # terminal: drain the backlog as no-ops
            return True
        if exe.status is Status.SUSPENDED:
            # paused: leave the message queued (FIFO preserved for resume) and park
            # it for a while so a suspended group does not spin a worker.
            self.transport.nack(lease, delay=self.suspend_recheck)
            return True
        if exe.status is Status.CANCELLING and lease.event.kind != "Cancel":
            self.transport.ack(lease)  # cooperative cancel in flight: drain until the Cancel
            return True
        driver = TransportDriver(
            _defn_for(self.definitions, self.resolver, exe),
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
        )
        try:
            driver.route(exe, lease.event)
        except StoreConflict:
            # another writer (a control-plane command, or a worker whose lease we
            # raced) advanced this Execution since we loaded it: our work is stale.
            # Redeliver so we retry against the fresh state (where we may instead
            # find a lifecycle status to honour). The version/CAS is the fence.
            self.transport.nack(lease)
            return True
        self.transport.ack(lease)
        return True

    def fire_due_timers(self) -> int:
        """Publish a `Timeout` event for every timer due now, then remove it (a
        worker then drives it like any event). The Timeout id is stable so a timer
        swept by two workers dedupes to one effect. Returns how many fired."""
        fired = 0
        for execution_id, path, fire_at in self.store.due_timers(self._clock()):
            self.transport.publish(execution_id, engine.timeout_event(execution_id, path, fire_at))
            self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    def run(self, stop: threading.Event, idle_sleep: float = 0.005) -> None:
        """Loop until `stop` is set. When the queue is empty, sweep due timers
        (publishing their Timeout events) before backing off."""
        while not stop.is_set():
            if self.step():
                continue
            if self.fire_due_timers() == 0:
                stop.wait(idle_sleep)


class DistributedRunner:
    """Façade over a store + transport + Definition registry."""

    def __init__(
        self,
        store: ExecutionStore,
        transport: Transport,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
    ) -> None:
        self.store = store
        self.transport = transport
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver  # FQN -> Definition for submachine `invoke` (optional)
        self._clock = clock  # used when arming timers (create/control); tests inject it

    def _transport_driver(self, defn: Definition) -> TransportDriver:
        return TransportDriver(
            defn,
            self.store,
            self.transport,
            self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
        )

    def create(self, definition_id: str, context: Optional[dict] = None) -> Execution:
        """Start a new Execution (inline) and return its committed state. The
        initial configuration — including an orthogonal fork's regions — is
        persisted; subsequent events flow through the transport."""
        exe = Execution(definition_id=definition_id, context=dict(context or {}))
        self._transport_driver(self.definitions[definition_id]).start(exe)
        loaded = self.store.load(exe.id)
        assert loaded is not None
        return loaded

    def send(self, execution_id: str, event: Event) -> None:
        """Publish an event to an Execution's group (a worker delivers it)."""
        self.transport.publish(execution_id, event)

    def worker(
        self,
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Callable[[], float] = time.time,
    ) -> Worker:
        return Worker(
            self.store,
            self.transport,
            self.definitions,
            worker_id,
            visibility,
            suspend_recheck,
            clock,
            resolver=self.resolver,
        )

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def _driver(self, execution_id: str) -> TransportDriver:
        exe = self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        return self._transport_driver(_defn_for(self.definitions, self.resolver, exe))

    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> None:
        """Cancel `execution_id`: cooperative if it models `on: Cancel` (the
        machine cleans up; a worker drains the backlog until the injected Cancel),
        forceful terminate otherwise. The injected Cancel is published here.
        `reason` is an opaque payload carried on the `Cancel` event for the model."""
        driver = self._driver(execution_id)
        control.cancel(self.store, driver.defn, execution_id, reason=reason)
        driver._flush()  # publish the injected Cancel (if any) to the transport

    def terminate(self, execution_id: str) -> None:
        """Forcefully cancel `execution_id` now (no cleanup); the backlog drains."""
        control.terminate(self.store, execution_id)

    def suspend(self, execution_id: str) -> None:
        """Pause `execution_id` (reversible; state and queued backlog preserved)."""
        control.suspend(self.store, execution_id)

    def resume(self, execution_id: str) -> None:
        """Resume a suspended `execution_id`, continuing where it stopped."""
        control.resume(self.store, execution_id)
