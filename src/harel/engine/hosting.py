"""The runners' logic — the durable host, the distributed sender, the worker — as flows (see
`harel.engine.flow`), written once for every execution model.

`DurableLogic` runs an execution's events inline (`create`, `process`, timers, the control
plane): the caller gets back the execution as it is once the event has been processed — a
synchronous semantics, whatever the execution model. `SenderLogic` is the distributed
runner's side that only writes: create an execution and queue its `Start`, send an event;
a worker processes them later — an asynchronous semantics. `WorkerLogic` is what a worker
does with one claimed message. A concrete runner runs these flows with an interpreter and
gives them their ports: the `store` and the `transport`. The control plane is
`harel.engine.control`'s flows, over the same store.
"""

from __future__ import annotations

import inspect
import logging
import random
import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.events import check_context, check_event, with_defaults
from harel.definition.model import Definition
from harel.engine import control
from harel.engine.driving import DriverLogic, FailOnActionError, TransportDriverLogic, store, transport
from harel.engine.execution import Execution, Status, stamp
from harel.engine.flow import Flow, parallel
from harel.engine.resolve import MachineResolver, ResolveError
from harel.engine.store import StoreConflict
from harel.spec.states import Event

logger = logging.getLogger(__name__)


EXECUTION_MODELS = ("background", "inline")


def check_execution(execution: str, *ports: tuple[str, Any]) -> None:
    """Check an `execution=` choice; `"inline"` runs in the caller's thread without an
    event loop, so every port it is given must be sync (a `(name, object)` pair each)."""
    if execution not in EXECUTION_MODELS:
        raise ValueError(f"execution must be one of {EXECUTION_MODELS}, got {execution!r}")
    if execution == "inline":
        for name, port in ports:
            if any(inspect.iscoroutinefunction(getattr(port, m, None)) for m in ("load", "claim", "publish")):
                raise TypeError(
                    f"execution='inline' runs in the caller's thread, without an event loop: "
                    f"it needs a sync {name}, got the async {type(port).__name__}"
                )


class _HostedDriverLogic(FailOnActionError, DriverLogic):
    """The driver's logic with the dead-letter policy, as the durable host runs it."""


class _HostedTransportDriverLogic(FailOnActionError, TransportDriverLogic):
    """The transport driver's logic with the dead-letter policy, as a worker runs it."""


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


ACTION_ERROR_POLICIES = ("fail", "raise")


class DurableLogic:
    """The durable host: drives an Execution's events inline, checkpointing at every event
    boundary. An action error nothing in the model handles is, by `on_action_error`:

    - `"fail"` (the default) — the execution fails terminally (`FAILED`, the dead letter),
      and that step is committed;
    - `"raise"` — the exception reaches the caller and that step is not committed. Inside a
      caller's transaction (a web request's), the caller's own writes and the machine's
      advance then roll back together. (Steps the same call committed before — an earlier
      region of a broadcast — stay, unless that transaction rolls them back.)"""

    def __init__(
        self,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        on_action_error: str = "fail",
    ) -> None:
        if on_action_error not in ACTION_ERROR_POLICIES:
            raise ValueError(
                f"on_action_error must be one of {ACTION_ERROR_POLICIES}, got {on_action_error!r}"
            )
        self.definitions = definitions
        _register_submachines(self.definitions)
        self._clock = clock
        self.resolver = resolver
        self._trace = trace
        self._driver_class = _HostedDriverLogic if on_action_error == "fail" else DriverLogic

    def _resolve_machine(self, fqn: str) -> Definition:
        return _resolve_machine(self.definitions, self.resolver, fqn)

    def _driver_logic(self, definition_id: str) -> DriverLogic:
        return self._driver_class(
            self.definitions[definition_id],
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=self._resolve_machine,
            trace=self._trace,
        )

    def _loaded_flow(self, execution_id: str) -> Flow:
        loaded = yield from store("load", execution_id)
        assert loaded is not None
        return loaded

    def create_flow(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
    ) -> Flow:
        if execution_id is not None and (yield from store("load", execution_id)) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        driver = self._driver_logic(definition_id)
        context = with_defaults(driver.defn.context_schema, dict(context or {}))
        check_context(driver.defn.context_schema, context)
        exe = Execution(
            definition_id=definition_id,
            context=context,
            priority=priority,
            **({"id": execution_id} if execution_id is not None else {}),
        )
        yield from driver.start_flow(exe)
        return (yield from self._loaded_flow(exe.id))

    def process_flow(self, execution_id: str, event: Event) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver_logic(exe.definition_id)
        check_event(driver.defn.events, event.kind, event.data)  # before anything runs
        yield from driver.inject_flow(exe, event)
        return (yield from self._loaded_flow(execution_id))

    def recover_flow(self, definition_id: str) -> Flow:
        yield from self._driver_logic(definition_id)._flush_flow()

    def fire_due_timers_flow(self) -> Flow:
        # one at a time: delivering a timeout commits a CAS write on the execution, and two
        # timers of the same execution (nested composites) must not race
        fired = 0
        for execution_id, path, fire_at in (yield from store("due_timers", self._clock())):
            exe = yield from store("load", execution_id)
            if exe is not None and exe.status is Status.SUSPENDED:
                continue  # left armed: it fires once resumed (the worker path parks it the same way)
            if exe is not None and exe.definition_id in self.definitions:
                event = engine.timeout_event(execution_id, path, fire_at)
                yield from self._driver_logic(exe.definition_id)._deliver_timeout_flow(execution_id, event)
            yield from store("delete_timer", execution_id, path, fire_at)
            fired += 1
        return fired

    # --- control plane ------------------------------------------------------
    def cancel_flow(self, execution_id: str, reason: Optional[dict] = None) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver_logic(exe.definition_id)
        yield from control.cancel_flow(driver.defn, execution_id, reason=reason, clock=self._clock)
        yield from driver._flush_flow()  # deliver the injected Cancel inline (runs the cleanup)
        return (yield from self._loaded_flow(execution_id))

    def terminate_flow(self, execution_id: str) -> Flow:
        yield from control.terminate_flow(execution_id, clock=self._clock)
        return (yield from self._loaded_flow(execution_id))

    def suspend_flow(self, execution_id: str) -> Flow:
        yield from control.suspend_flow(execution_id, clock=self._clock)
        return (yield from self._loaded_flow(execution_id))

    def resume_flow(self, execution_id: str) -> Flow:
        yield from control.resume_flow(execution_id, clock=self._clock)
        return (yield from self._loaded_flow(execution_id))

    def purge_flow(self, execution_id: str, archive: Optional[Callable[[dict], Any]] = None) -> Flow:
        return (yield from control.purge_flow(execution_id, archive=archive))

    def redrive_flow(self, execution_id: str, target_path: str) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver_logic(exe.definition_id)
        yield from control.redrive_flow(driver.defn, execution_id, target_path, clock=self._clock)
        return (yield from self._loaded_flow(execution_id))


class SenderLogic:
    """The distributed runner's writing side: create an Execution and queue its `Start`,
    start a deferred one, send an event, and the control plane. It never runs a machine's
    actions — a worker does, when it claims what this queued."""

    def __init__(
        self,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        shares_store_transaction: bool = False,
    ) -> None:
        """`shares_store_transaction`: the transport writes in the store's transaction (see
        `Transport`), so a failed `Start` publish reaches the caller."""
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self._clock = clock
        self._trace = trace
        self._shares_store_transaction = shares_store_transaction

    def _transport_logic(self, defn: Definition) -> TransportDriverLogic:
        return _HostedTransportDriverLogic(
            defn,
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )

    def create_flow(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
        start_on_create: bool = True,
    ) -> Flow:
        if execution_id is not None and (yield from store("load", execution_id)) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        if definition_id not in self.definitions:
            # fail now, in the caller's own stack: a bad id must not persist a PENDING record
            # that later dies (or loops) on whichever worker claims its Start. Checked after
            # the execution_id collision, so a caller retrying with a bad definition_id still
            # learns about an id collision first.
            raise KeyError(f"unknown definition_id {definition_id!r}")
        # a required field may still come with `start(data=...)` when the start is deferred
        schema = self.definitions[definition_id].context_schema
        context = with_defaults(schema, dict(context or {}))
        check_context(schema, context, check_required=start_on_create)
        exe = Execution(
            definition_id=definition_id,
            context=context,
            priority=priority,
            **({"id": execution_id} if execution_id is not None else {}),
        )
        if start_on_create:
            yield from self._persist_start_flow(exe)
        else:
            stamp(exe, self._clock())
            yield from store("save", exe)
        return exe

    def _persist_start_flow(self, exe: Execution, data: Optional[dict] = None) -> Flow:
        """Commit `exe`'s `Start` into the durable outbox, in the same atomic write as `exe`
        itself, then publish just that entry and ack it. Not the generic relay, which would
        drain every pending entry fleet-wide. A failed publish or ack doesn't raise: the
        entry stays queued, so a later flush delivers it (a copy is dropped by the dedupe on
        the Start's id) — and the caller keeps the id it needs to retry with `start()`. Unless
        the transport shares the store's transaction: then the failure is the caller's, whose
        transaction can't commit, and it is raised."""
        event = Event(kind="Start", data=dict(data or {}))
        stamp(exe, self._clock())
        seqs = yield from store("commit", exe, [(exe.id, event)])
        if self._shares_store_transaction:
            yield from transport("publish", exe.id, event, priority=exe.priority)
            for seq in seqs:
                yield from store("ack_outbox", seq)
            return
        try:
            yield from transport("publish", exe.id, event, priority=exe.priority)
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
                yield from store("ack_outbox", seq)
        except Exception:
            logger.warning(
                "published the Start for execution %s but could not ack its outbox "
                "entry; a later flush publishes it again, and the copy is dropped",
                exe.id,
                exc_info=True,
            )

    def start_flow(self, execution_id: str, data: Optional[dict] = None) -> Flow:
        exe = yield from store("load", execution_id)
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
        yield from self._persist_start_flow(exe, data)

    def send_flow(self, execution_id: str, event: Event) -> Flow:
        if event.kind == "Start":
            raise ValueError(
                "send() refuses a caller-supplied Start event — use create() or "
                "start(execution_id, data=...) instead, the only sanctioned way to "
                "begin an execution (optionally with its parameters)"
            )
        exe = yield from store("load", execution_id)
        if exe is not None:  # checked in the caller's stack: a malformed event never queues
            check_event(_defn_for(self.definitions, self.resolver, exe).events, event.kind, event.data)
        priority = exe.priority if exe is not None else 0
        yield from transport("publish", execution_id, event, priority=priority)

    # --- control plane ------------------------------------------------------
    def _logic_for_flow(self, execution_id: str) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        return self._transport_logic(_defn_for(self.definitions, self.resolver, exe)), exe

    def cancel_flow(self, execution_id: str, reason: Optional[dict] = None) -> Flow:
        logic, exe = yield from self._logic_for_flow(execution_id)
        yield from control.cancel_flow(logic.defn, execution_id, reason=reason, clock=self._clock)
        yield from logic._flush_flow(primary_priority={execution_id: exe.priority})

    def terminate_flow(self, execution_id: str) -> Flow:
        yield from control.terminate_flow(execution_id, clock=self._clock)

    def suspend_flow(self, execution_id: str) -> Flow:
        yield from control.suspend_flow(execution_id, clock=self._clock)

    def resume_flow(self, execution_id: str) -> Flow:
        yield from control.resume_flow(execution_id, clock=self._clock)

    def purge_flow(self, execution_id: str, archive: Optional[Callable[[dict], Any]] = None) -> Flow:
        return (yield from control.purge_flow(execution_id, archive=archive))

    def redrive_flow(self, execution_id: str, target_path: str) -> Flow:
        logic, _exe = yield from self._logic_for_flow(execution_id)
        yield from control.redrive_flow(logic.defn, execution_id, target_path, clock=self._clock)


class WorkerLogic:
    """What a worker does with the transport: claim a message (applying the priority policy)
    and handle it — load, dedupe, route by status, run the engine, ack or nack — and fire
    the due timers by publishing them."""

    def __init__(
        self,
        definitions: dict[str, Definition],
        worker_id: str = "worker",
        visibility: float = 30.0,
        suspend_recheck: float = 5.0,
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        high_ratio: float = 0.0,
        priority_threshold: int = 1,
    ) -> None:
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self.worker_id = worker_id
        self.visibility = visibility
        self.suspend_recheck = suspend_recheck
        self._clock = clock
        self._trace = trace  # opt-in execution timeline, threaded to each per-execution driver
        self.high_ratio = high_ratio
        self.priority_threshold = priority_threshold

    def _transport_logic(self, exe: Execution) -> TransportDriverLogic:
        return _HostedTransportDriverLogic(
            _defn_for(self.definitions, self.resolver, exe),
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )

    def _load_for_event_flow(self, execution_id: str, event_id: str, combined: bool) -> Flow:
        """Load the Execution and its dedupe flag: one round-trip if the store offers
        `load_for_event` (`combined`), else `load` + `is_processed`."""
        if combined:
            return (yield from store("load_for_event", execution_id, event_id))
        exe = yield from store("load", execution_id)
        processed = exe is not None and (yield from store("is_processed", execution_id, event_id))
        return exe, processed

    def handle_flow(self, lease, combined_load: bool) -> Flow:
        exe, processed = yield from self._load_for_event_flow(lease.group_id, lease.event.id, combined_load)
        if exe is None or processed:
            yield from transport("ack", lease)
            return True
        if exe.status is Status.CANCELLED:
            yield from transport("ack", lease)
            return True
        if exe.status is Status.SUSPENDED:
            yield from transport("nack", lease, delay=self.suspend_recheck)
            return True
        if exe.status is Status.PENDING and lease.event.kind != "Start":
            # not started yet: only a RUNNING execution processes domain events. Discarded,
            # not parked — parking would hold this group's single-active-consumer lock and
            # block the Start itself behind it (a self-deadlock). Normally unreachable
            # (create() publishes the Start first); it fires for a caller that deferred the
            # start and sent an event before start(), or, rarely, when the Start's direct
            # publish failed and a fast-following event reached the transport first. Logged,
            # since nothing else records that the event was dropped.
            logger.warning(
                "discarding %s event for execution %s: still PENDING (not yet "
                "started) — the sender should retry once it is RUNNING",
                lease.event.kind,
                lease.group_id,
            )
            yield from transport("ack", lease)
            return True
        if exe.status is Status.CANCELLING and lease.event.kind != "Cancel":
            yield from transport("ack", lease)
            return True
        try:
            yield from self._transport_logic(exe).route_flow(exe, lease.event)
        except StoreConflict:
            yield from transport("nack", lease)
            return True
        yield from transport("ack", lease)
        return True

    def claim_flow(self) -> Flow:
        """Claim one message, applying the high_ratio/priority_threshold policy: with
        high_ratio>0, try priority>=threshold first, falling back to any priority so the
        worker isn't idle when no high-priority work is available."""
        if self.high_ratio > 0 and random.random() < self.high_ratio:
            lease = yield from transport("claim", self.worker_id, self.visibility, self.priority_threshold)
            if lease is not None:
                return lease
        return (yield from transport("claim", self.worker_id, self.visibility))

    def step_flow(self, combined_load: bool) -> Flow:
        """Process at most one message. Returns False if nothing was claimable."""
        lease = yield from self.claim_flow()
        if lease is None:
            return False
        return (yield from self.handle_flow(lease, combined_load))

    def fire_due_timers_flow(self) -> Flow:
        due = yield from store("due_timers", self._clock())
        if not due:
            return 0

        def fire_one(execution_id: str, path: str, fire_at: float) -> Flow:
            # publish at the execution's own priority: for a machine that parks on a
            # `timeout:` state, this Timeout is the FIRST publish to its group, so it sets
            # the group's priority — dropping it here would pin the group to 0
            exe = yield from store("load", execution_id)
            priority = exe.priority if exe is not None else 0
            event = engine.timeout_event(execution_id, path, fire_at)
            yield from transport("publish", execution_id, event, priority=priority)
            yield from store("delete_timer", execution_id, path, fire_at)

        # each only publishes (the CAS happens later, when a worker routes the Timeout)
        yield from parallel([fire_one(eid, p, fa) for eid, p, fa in due])
        return len(due)
