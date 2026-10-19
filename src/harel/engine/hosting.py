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
from typing import Any, Callable, Iterable, Optional

from harel import engine
from harel.definition.events import check_context, check_event, with_defaults
from harel.definition.model import Definition
from harel.engine import control
from harel.engine.driving import DriverLogic, FailOnActionError, TransportDriverLogic, store, transport
from harel.engine.execution import Execution, Status, routing_key, stamp
from harel.engine.flow import Flow, parallel
from harel.engine.resolve import (
    DefinitionNotLoaded,
    MachineResolver,
    ResolveError,
    find_definition,
    register_definition,
)
from harel.engine.store import Step, StoreConflict, WorkerRecord
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
    by id (= their synthetic FQN) and version, so they resolve without an external resolver."""
    for defn in list(definitions.values()):
        for sub in defn.submachines.values():
            register_definition(definitions, sub)


def _resolve_machine(definitions: dict, resolver: Optional[MachineResolver], fqn: str) -> Definition:
    """Resolve a submachine FQN and register it in `definitions` (so the child then
    routes by its own id). Inline targets (id == FQN) are already registered; an
    external FQN goes through the resolver. Raises if neither has it."""
    if fqn in definitions:  # an inline submachine (id == synthetic FQN)
        return definitions[fqn]
    if resolver is None:
        raise ResolveError(f"invoke {fqn!r} but this runner has no machine resolver")
    defn = resolver.resolve(fqn)
    register_definition(definitions, defn)
    return defn


def _find_local(
    definitions: dict, resolver: Optional[MachineResolver], exe: Execution
) -> Optional[Definition]:
    """The Definition of `exe`'s version this runner holds: in the registry, or — for a submachine
    child whose Definition this worker has not built yet — resolved by its persisted FQN (the
    spawning worker may be a different process). None when it has none."""
    version = exe.definition_version or 1
    if find_definition(definitions, exe.definition_id, version) is None and exe.definition_fqn is not None:
        try:
            _resolve_machine(definitions, resolver, exe.definition_fqn)
        except ResolveError:
            return None
    return find_definition(definitions, exe.definition_id, version)


class _DefinitionLookup:
    """How a runner finds the Definition an execution runs on (`_defn_flow`): its own — the
    registry it was given — when that is the version and source the execution started on, else
    the version published in the store (see `harel.published`), which then may call only the
    action modules the runner's own definitions call, plus `action_modules` — with its handlers and
    guards bound by `actions` and `guards`, as `definition_from_dsl` binds them."""

    definitions: dict[str, Definition]
    resolver: Optional[MachineResolver]
    action_modules: tuple[str, ...] = ()
    actions: Optional[dict] = None
    guards: Optional[dict] = None
    _published: Any = None

    def _published_definitions(self) -> Any:
        if self._published is None:
            from harel.published import PublishedDefinitions, allowed_action_modules

            own = allowed_action_modules(list(self.definitions.values()), self.action_modules)
            self._published = PublishedDefinitions(own, actions=self.actions, guards=self.guards)
        return self._published

    def _defn_flow(self, exe: Execution) -> Flow:
        """The Definition to drive `exe` with. Raises `DefinitionNotLoaded` when neither this
        runner nor the store has its version."""
        version = exe.definition_version or 1
        local = _find_local(self.definitions, self.resolver, exe)
        if local is not None and (local.digest is None or exe.definition_digest in (None, local.digest)):
            return local
        published = yield from self._published_flow(exe, version)
        if published is not None and exe.definition_digest in (None, published.digest):
            seen = self._published_definitions().warned
            if local is not None and (exe.definition_id, version) not in seen:
                seen.add((exe.definition_id, version))
                logger.warning(
                    "machine %r version %d here differs from the published one, which execution %s "
                    "started on: running it on the published one",
                    exe.definition_id,
                    version,
                    exe.id,
                )
            return published
        if local is not None:
            return local  # a source the execution didn't start on: `definition_problem` says so
        raise DefinitionNotLoaded(
            f"execution {exe.id!r} runs machine {exe.definition_id!r} version {version}, which this "
            f"runner hasn't loaded and the store hasn't published"
        )

    def _published_flow(self, exe: Execution, version: int) -> Flow:
        """`exe`'s version as published: a machine's own record, or — for a submachine child —
        what its parent's Definition holds under the child's FQN."""
        if exe.definition_fqn is not None:
            if exe.parent_id is None:
                return None
            parent = yield from store("load", exe.parent_id)
            if parent is None:
                return None
            sub = (yield from self._defn_flow(parent)).submachines.get(exe.definition_fqn)
            return sub if sub is not None and sub.version == version else None
        published = self._published_definitions()
        built = published.get(exe.definition_id, version)
        if built is not None:
            return built
        record = yield from store("load_definition", exe.definition_id, version)
        if record is None:
            return None
        try:
            return published.build(record)
        except Exception as exc:
            raise DefinitionNotLoaded(
                f"execution {exe.id!r} runs machine {exe.definition_id!r} version {version}, whose "
                f"published definition this runner can't use: {exc}"
            ) from exc


def _new_defn(definitions: dict, definition_id: str, version: Optional[int]) -> Definition:
    """The Definition a new execution of `definition_id` runs: `version`, else the highest."""
    defn = (
        definitions.get(definition_id)
        if version is None
        else find_definition(definitions, definition_id, version)
    )
    if defn is None:
        which = repr(definition_id) if version is None else f"{definition_id!r} version {version}"
        raise KeyError(f"unknown definition_id {which}")
    return defn


ACTION_ERROR_POLICIES = ("fail", "raise")


class DurableLogic(_DefinitionLookup):
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
        *,
        action_modules: Iterable[str] = (),
        actions: Optional[dict] = None,
        guards: Optional[dict] = None,
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
        self.action_modules = tuple(action_modules)
        self.actions, self.guards = actions, guards
        self._driver_class = _HostedDriverLogic if on_action_error == "fail" else DriverLogic

    def _resolve_machine(self, fqn: str) -> Definition:
        return _resolve_machine(self.definitions, self.resolver, fqn)

    def _driver_logic(self, defn: Definition) -> DriverLogic:
        driver = self._driver_class(
            defn,
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=self._resolve_machine,
            trace=self._trace,
        )
        driver.definition_flow = self._defn_flow
        return driver

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
        version: Optional[int] = None,
    ) -> Flow:
        if execution_id is not None and (yield from store("load", execution_id)) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        driver = self._driver_logic(_new_defn(self.definitions, definition_id, version))
        context = with_defaults(driver.defn.context_schema, dict(context or {}))
        check_context(driver.defn.context_schema, context)
        exe = Execution(
            definition_id=definition_id,
            definition_version=driver.defn.version,
            definition_digest=driver.defn.digest,
            routing_key=definition_id,
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
        driver = self._driver_logic((yield from self._defn_flow(exe)))
        check_event(driver.defn.events, event.kind, event.data)  # before anything runs
        yield from driver.inject_flow(exe, event)
        return (yield from self._loaded_flow(execution_id))

    def recover_flow(self, definition_id: str) -> Flow:
        yield from self._driver_logic(self.definitions[definition_id])._flush_flow()

    def fire_due_timers_flow(self) -> Flow:
        # one at a time: delivering a timeout commits a CAS write on the execution, and two
        # timers of the same execution (nested composites) must not race
        fired = 0
        for execution_id, path, fire_at in (yield from store("due_timers", self._clock())):
            exe = yield from store("load", execution_id)
            if exe is not None and exe.status is Status.SUSPENDED:
                continue  # left armed: it fires once resumed (the worker path parks it the same way)
            if exe is not None and exe.definition_id in self.definitions:
                try:
                    defn = yield from self._defn_flow(exe)
                except DefinitionNotLoaded:
                    # left armed for a runner that has its version; the other timers still fire
                    logger.warning("timer %s of execution %s not fired", path, execution_id, exc_info=True)
                    continue
                event = engine.timeout_event(execution_id, path, fire_at)
                yield from self._driver_logic(defn)._deliver_timeout_flow(execution_id, event)
            yield from store("delete_timer", execution_id, path, fire_at)
            fired += 1
        return fired

    # --- control plane ------------------------------------------------------
    def cancel_flow(self, execution_id: str, reason: Optional[dict] = None) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver_logic((yield from self._defn_flow(exe)))
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
        driver = self._driver_logic((yield from self._defn_flow(exe)))
        yield from control.redrive_flow(driver.defn, execution_id, target_path, clock=self._clock)
        return (yield from self._loaded_flow(execution_id))


class SenderLogic(_DefinitionLookup):
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
        *,
        action_modules: Iterable[str] = (),
        actions: Optional[dict] = None,
        guards: Optional[dict] = None,
    ) -> None:
        """`shares_store_transaction`: the transport writes in the store's transaction (see
        `Transport`), so a failed `Start` publish reaches the caller."""
        self.definitions = definitions
        _register_submachines(self.definitions)
        self.resolver = resolver
        self._clock = clock
        self._trace = trace
        self._shares_store_transaction = shares_store_transaction
        self.action_modules = tuple(action_modules)
        self.actions, self.guards = actions, guards

    def _transport_logic(self, defn: Definition) -> TransportDriverLogic:
        logic = _HostedTransportDriverLogic(
            defn,
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )
        logic.definition_flow = self._defn_flow
        return logic

    def create_flow(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
        start_on_create: bool = True,
        version: Optional[int] = None,
    ) -> Flow:
        if execution_id is not None and (yield from store("load", execution_id)) is not None:
            from harel.engine.store import ExecutionAlreadyExists

            raise ExecutionAlreadyExists(execution_id)
        # fail now, in the caller's own stack: a bad id must not persist a PENDING record
        # that later dies (or loops) on whichever worker claims its Start. Checked after
        # the execution_id collision, so a caller retrying with a bad definition_id still
        # learns about an id collision first.
        defn = _new_defn(self.definitions, definition_id, version)
        # a required field may still come with `start(data=...)` when the start is deferred
        schema = defn.context_schema
        context = with_defaults(schema, dict(context or {}))
        check_context(schema, context, check_required=start_on_create)
        exe = Execution(
            definition_id=definition_id,
            definition_version=defn.version,
            definition_digest=defn.digest,
            routing_key=definition_id,
            context=context,
            priority=priority,
            **({"id": execution_id} if execution_id is not None else {}),
        )
        if start_on_create:
            yield from self._persist_start_flow(exe, cause="create")
        else:
            stamp(exe, self._clock())
            yield from store("save", exe)
        return exe

    def _persist_start_flow(self, exe: Execution, data: Optional[dict] = None, *, cause: str) -> Flow:
        """Commit `exe`'s `Start` into the durable outbox, in the same atomic write as `exe`
        itself, then publish just that entry and ack it. Not the generic relay, which would
        drain every pending entry fleet-wide. A failed publish or ack doesn't raise: the
        entry stays queued, so a later flush delivers it (a copy is dropped by the dedupe on
        the Start's id) — and the caller keeps the id it needs to retry with `start()`. Unless
        the transport shares the store's transaction: then the failure is the caller's, whose
        transaction can't commit, and it is raised."""
        event = Event(kind="Start", data=dict(data or {}))
        stamp(exe, self._clock())
        step = Step(
            cause=cause,
            from_status=exe.status,
            to_status=exe.status,  # still PENDING: a worker runs the Start
            event_kind=event.kind,
            event_id=event.id,
        )
        seqs = yield from store("commit", exe, [(exe.id, event)], step=step)
        key = routing_key(exe)
        if self._shares_store_transaction:
            yield from transport("publish", exe.id, event, priority=exe.priority, routing_key=key)
            for seq in seqs:
                yield from store("ack_outbox", seq)
            return
        try:
            yield from transport("publish", exe.id, event, priority=exe.priority, routing_key=key)
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
        defn = yield from self._defn_flow(exe)
        check_context(defn.context_schema, {**exe.context, **(data or {})})
        yield from self._persist_start_flow(exe, data, cause="start")

    def send_flow(self, execution_id: str, event: Event) -> Flow:
        if event.kind == "Start":
            raise ValueError(
                "send() refuses a caller-supplied Start event — use create() or "
                "start(execution_id, data=...) instead, the only sanctioned way to "
                "begin an execution (optionally with its parameters)"
            )
        exe = yield from store("load", execution_id)
        if exe is not None:  # checked in the caller's stack: a malformed event never queues
            check_event((yield from self._defn_flow(exe)).events, event.kind, event.data)
        priority = exe.priority if exe is not None else 0
        yield from transport("publish", execution_id, event, priority=priority, routing_key=routing_key(exe))

    # --- control plane ------------------------------------------------------
    def _logic_for_flow(self, execution_id: str) -> Flow:
        exe = yield from store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        return self._transport_logic((yield from self._defn_flow(exe))), exe

    def cancel_flow(self, execution_id: str, reason: Optional[dict] = None) -> Flow:
        logic, exe = yield from self._logic_for_flow(execution_id)
        yield from control.cancel_flow(logic.defn, execution_id, reason=reason, clock=self._clock)
        yield from logic._flush_flow(primary={execution_id: exe})

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


def log_handling_failed(worker_id: str, lease: Any, retry_in: float) -> None:
    """A worker's loop failed handling `lease` (a store or transport outage, a version it can't
    run, an engine bug — an action's own error is the driver's to route, not this)."""
    logger.exception(
        "worker %s failed handling %s event %s for execution %s; retrying in %ss",
        worker_id,
        lease.event.kind,
        lease.event.id,
        lease.group_id,
        retry_in,
    )


def log_nack_failed(worker_id: str, lease: Any) -> None:
    logger.exception(
        "worker %s could not nack %s event %s for execution %s; it is redelivered when its lease expires",
        worker_id,
        lease.event.kind,
        lease.event.id,
        lease.group_id,
    )


def log_loop_step_failed(worker_id: str, what: str, retry_in: Optional[float]) -> None:
    """A worker's loop failed a step of its own — to claim, to sweep the due timers, to record
    its heartbeat: it carries on, trying again after `retry_in` seconds (None: it won't)."""
    if retry_in is None:
        logger.exception("worker %s could not %s", worker_id, what)
    else:
        logger.exception("worker %s could not %s; retrying in %ss", worker_id, what, retry_in)


class WorkerLogic(_DefinitionLookup):
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
        *,
        machines: Optional[Iterable[str]] = None,
        heartbeat: float = 10.0,
        action_modules: Iterable[str] = (),
        actions: Optional[dict] = None,
        guards: Optional[dict] = None,
    ) -> None:
        """`machines`, when given, are the only machines this worker claims work for — the
        executions whose tree started from one of them (their `routing_key`); None claims all.
        A running worker records a heartbeat in the store every `heartbeat` seconds (its
        machines and when it was last seen), which a monitor reads to tell which machines no
        live worker covers."""
        self.definitions = definitions
        self.machines = frozenset(machines) if machines is not None else None
        if self.machines is not None and not self.machines:
            raise ValueError("machines names no machine: pass None to claim them all")
        self.action_modules = tuple(action_modules)
        self.actions, self.guards = actions, guards
        _register_submachines(self.definitions)
        self.resolver = resolver
        self.worker_id = worker_id
        self.visibility = visibility
        self.suspend_recheck = suspend_recheck
        self._clock = clock
        self._trace = trace  # opt-in execution timeline, threaded to each per-execution driver
        self.high_ratio = high_ratio
        self.priority_threshold = priority_threshold
        self.heartbeat = heartbeat
        self._started_at: Optional[float] = None
        self._next_beat = 0.0

    def heartbeat_due(self) -> bool:
        return self._clock() >= self._next_beat

    def heartbeat_failed(self) -> None:
        """A heartbeat that failed is tried again a whole interval later, not at every turn."""
        self._next_beat = self._clock() + self.heartbeat

    def heartbeat_flow(self) -> Flow:
        """Record this worker's heartbeat: its machines, when it started and now."""
        now = self._clock()
        if self._started_at is None:
            self._started_at = now
        machines = tuple(sorted(self.machines)) if self.machines is not None else None
        yield from store(
            "record_worker", WorkerRecord(self.worker_id, machines, self._started_at, now, self.heartbeat)
        )
        self._next_beat = now + self.heartbeat

    def forget_flow(self) -> Flow:
        """Remove this worker's heartbeat (it is stopping)."""
        yield from store("forget_worker", self.worker_id)

    def _transport_logic_flow(self, exe: Execution) -> Flow:
        logic = _HostedTransportDriverLogic(
            (yield from self._defn_flow(exe)),
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=lambda fqn: _resolve_machine(self.definitions, self.resolver, fqn),
            trace=self._trace,
        )
        logic.definition_flow = self._defn_flow
        return logic

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
            logic = yield from self._transport_logic_flow(exe)
            yield from logic.route_flow(exe, lease.event)
        except StoreConflict:
            yield from transport("nack", lease)
            return True
        yield from transport("ack", lease)
        return True

    def claim_flow(self) -> Flow:
        """Claim one message, applying the high_ratio/priority_threshold policy: with
        high_ratio>0, try priority>=threshold first, falling back to any priority so the
        worker isn't idle when no high-priority work is available. A worker given `machines`
        claims only their messages."""
        # an unfiltered worker claims as before, so a transport without routing still serves it
        routed = {"routing_keys": self.machines} if self.machines is not None else {}
        if self.high_ratio > 0 and random.random() < self.high_ratio:
            lease = yield from transport(
                "claim", self.worker_id, self.visibility, self.priority_threshold, **routed
            )
            if lease is not None:
                return lease
        return (yield from transport("claim", self.worker_id, self.visibility, **routed))

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
            yield from transport(
                "publish", execution_id, event, priority=priority, routing_key=routing_key(exe)
            )
            yield from store("delete_timer", execution_id, path, fire_at)

        # each only publishes (the CAS happens later, when a worker routes the Timeout)
        yield from parallel([fire_one(eid, p, fa) for eid, p, fa in due])
        return len(due)
