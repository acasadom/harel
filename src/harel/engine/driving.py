"""What drives the engine over Executions, written once as flows (see `harel.engine.flow`).

`DriverLogic` holds the driver's logic — run the engine for one event and commit, call the
actions, route their errors, create spawned children, relay the outbox, deliver timeouts,
broadcast to regions — as generator methods (`*_flow`) that yield IO requests instead of
doing IO. A concrete driver adds the execution model: `AsyncDriver` runs these flows with
coroutines. Subclasses override the hooks (`_on_action_error`, ...) and, to change what IO
happens, the flows themselves (the transport driver publishes where this one runs inline).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.events import with_defaults
from harel.definition.model import Definition
from harel.engine.execution import Execution, Status, stamp
from harel.engine.flow import CallAction, Flow, call, parallel, run_inline
from harel.engine.resolve import ResolveError
from harel.engine.runtime import _CONTROL, _action_name, _Proxy, _resolve, _trace_step
from harel.engine.store import TimerOp
from harel.spec.states import Event

logger = logging.getLogger(__name__)


def store(method: str, *args: Any, **kwargs: Any) -> Flow:
    """`yield from store("load", eid)`: a call on the store, from inside a flow."""
    return (yield from call("store", method, *args, **kwargs))


def transport(method: str, *args: Any, **kwargs: Any) -> Flow:
    """`yield from transport("publish", gid, ev)`: a call on the transport, from a flow."""
    return (yield from call("transport", method, *args, **kwargs))


class DriverLogic:
    """The driver's logic over one Definition (and, for a submachine `invoke`, the
    others in `definitions`). The bare driver propagates action errors (so parity tests
    surface bugs); the production policy (fail the execution) is a subclass's."""

    def __init__(
        self,
        defn: Definition,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
        trace: bool = False,
    ) -> None:
        self.defn = defn
        self._clock = clock
        self._definitions = definitions
        self.resolve_machine = resolve_machine
        self._trace_enabled = trace  # opt-in execution timeline (off => no per-event step)

    # --- hooks -------------------------------------------------------------
    def _definition_for(self, exe: Execution) -> Definition:
        if self._definitions is not None:
            return self._definitions.get(exe.definition_id, self.defn)
        return self.defn

    def _proxy(self, exe: Execution) -> Any:
        return _Proxy(exe.context)

    def _before_action(self, exe: Execution, node) -> None:
        pass

    def _on_action_error(self, exe: Execution, exc: Exception) -> None:
        raise exc

    # --- core --------------------------------------------------------------
    def _run_flow(
        self, exe: Execution, gen, event_id: Optional[str] = None, event: Optional[Event] = None
    ) -> Flow:
        """Drive the engine's effects for one event and commit. Returns whether the commit
        enqueued anything for the relay (outbox emits or child spawns), so the caller can skip
        the relay round-trips when there is nothing to deliver."""
        from_path = exe.active_path
        emits, timer_ops, spawns, actions, assigned = yield from self._drive_flow(exe, gen)
        step = (
            _trace_step(event, from_path, exe, actions, self._clock(), assigned)
            if self._trace_enabled
            else None
        )
        stamp(exe, self._clock())
        yield from store(
            "commit",
            exe,
            emits,
            processed_event_id=event_id,
            timers=tuple(timer_ops),
            spawns=tuple(spawns),
            trace=step,
        )
        return bool(emits or spawns)

    def _expression_error_flow(
        self, exe: Execution, exc: Exception, original_exc: Optional[Exception]
    ) -> Flow:
        """The engine couldn't evaluate a model expression (a `set`): routed exactly like an
        action error — to an `on error` in scope (`_error` + the error event carry it),
        else the runner's policy. It is raised before the transition leaves any state, so
        recovery starts from where the execution was. Partial effects are dropped."""
        if original_exc is not None:  # already recovering: no second attempt
            exc.__cause__ = original_exc
            self._on_action_error(exe, exc)
            return [], [], [], [], {}
        defn = self._definition_for(exe)
        ev = engine.error_event(exc)
        if engine.has_error_handler(defn, exe, ev):
            exe.context["_error"] = dict(ev.data)
            return (yield from self._drive_flow(exe, engine.process(defn, exe, ev), original_exc=exc))
        self._on_action_error(exe, exc)
        return [], [], [], [], {}

    def _drive_flow(self, exe: Execution, gen, original_exc: Optional[Exception] = None) -> Flow:
        """Run the engine generator `gen` to its end for `exe`, serving its effects: call the
        actions (routing a raise to `on error` or the runner's policy) and collect what the
        step commits — emits, timer ops, spawns, the actions run, what a `set` wrote."""
        emits: list[tuple[Optional[str], Event]] = []
        timer_ops: list[TimerOp] = []
        spawns: list[tuple[str, str, dict]] = []
        actions: list[str] = []
        assigned: dict = {}  # context values a `set` wrote this step (for the trace)
        proxy = self._proxy(exe)
        action_index = 0  # per-event counter -> a deterministic, replay-stable idempotency key
        try:
            effect = next(gen)
            while True:
                if isinstance(effect, (engine.RunAction, engine.RunSelector)):
                    self._before_action(exe, effect.node)
                    action = (
                        effect.selector.action if isinstance(effect, engine.RunSelector) else effect.action
                    )
                    # version is the pre-commit value (a failed attempt didn't bump it),
                    # so the key is identical across an at-least-once redelivery
                    proxy.idempotency_key = f"{exe.id}:{exe.version}:{action_index}"
                    action_index += 1
                    actions.append(_action_name(action))
                    try:
                        ret = yield CallAction(_resolve(action), proxy, effect.event, dict(action.inputs))
                    except Exception as exc:
                        gen.close()
                        if original_exc is not None:
                            # already recovering (an `on error` handler's own action just
                            # raised): no second attempt — chain explicitly (`__cause__`),
                            # since the implicit context Python normally sets is thread-local
                            # and an action may run in a thread pool, so it isn't reliably kept.
                            exc.__cause__ = original_exc
                            self._on_action_error(exe, exc)
                            return [], [], [], [], {}
                        if isinstance(effect, engine.RunAction) and effect.hook is engine.Hook.EXIT:
                            # `on_exit` must always succeed: leaving a state applies real,
                            # un-undoable side effects (releasing a lock, cancelling regions,
                            # disarming a timer), and routing away from it would have to
                            # re-run this very hook to reach anywhere outside the state's own
                            # subtree — re-triggering the same failure. So a raise here is
                            # always a bug: no `on error` lookup, straight to the runner policy.
                            self._on_action_error(exe, exc)
                            return [], [], [], [], {}
                        # if the model has an `on error` transition for the current config,
                        # route to it (exception in context._error + the error event data);
                        # else fall back to the runner's policy (fail the exe / re-raise).
                        # Partial effects are dropped either way.
                        defn = self._definition_for(exe)
                        ev = engine.error_event(exc)
                        if engine.has_error_handler(defn, exe, ev):
                            exe.context["_error"] = dict(ev.data)
                            return (
                                yield from self._drive_flow(
                                    exe, engine.process(defn, exe, ev), original_exc=exc
                                )
                            )
                        self._on_action_error(exe, exc)  # base: re-raises; production: fails the exe
                        return [], [], [], [], {}
                    effect = gen.send(engine.ActionResult(value=ret))
                elif isinstance(effect, engine.SpawnChildren):
                    # the fork's children are enqueued (committed atomically with the
                    # parent's join expectations), then created by the relay
                    spawns.extend((s.child_id, s.root_path, dict(s.context)) for s in effect.specs)
                    effect = gen.send(None)
                elif isinstance(effect, engine.Emit):
                    emits.append((effect.to, effect.event))
                    effect = gen.send(None)
                elif isinstance(effect, engine.ScheduleTimer):
                    # delay is either literal or read from context (a dynamic/backoff
                    # value the state's on_enter just computed, run above this effect)
                    delay = (
                        effect.delay
                        if effect.delay is not None
                        else float(exe.context.get(effect.context_key, 0.0))
                    )
                    fire_at = self._clock() + delay
                    timer_ops.append(TimerOp("schedule", effect.path, fire_at))
                    effect = gen.send(fire_at)
                elif isinstance(effect, engine.Assigned):
                    assigned.update(effect.values)
                    effect = gen.send(None)
                elif isinstance(effect, engine.CancelTimer):
                    timer_ops.append(TimerOp("cancel", effect.path))
                    effect = gen.send(None)
                else:
                    effect = gen.send(None)
        except StopIteration:
            pass
        except engine.ExpressionError as exc:
            return (yield from self._expression_error_flow(exe, exc, original_exc))
        return emits, timer_ops, spawns, actions, assigned

    def _create_spawn_flow(self, entry) -> Flow:
        """Create and start one pending child Execution, idempotently: if the child already
        exists (a crash-and-retry re-runs the fork), skip — its progress is kept. An
        orthogonal region shares this driver's Definition; a submachine `invoke` child runs
        another, named by the `__invoke_fqn__` riding in its context."""
        if (yield from store("load", entry.child_id)) is not None:
            return
        context = dict(entry.context)
        fqn = context.pop("__invoke_fqn__", None)
        if fqn is not None:
            if self.resolve_machine is None:
                raise ResolveError(f"invoke {fqn!r} but this runner has no machine resolver")
            child_defn = self.resolve_machine(fqn)
            # an invoked machine starts with its own context defaults; a region (the same
            # machine) starts with only what the fork passes down
            context = with_defaults(child_defn.context_schema, context)
        else:
            child_defn = self.defn
        # a child (orthogonal region / invoke / fan-out instance) inherits the parent's
        # priority — a high-priority workflow's regions carry the actual work, so they must
        # be claimed at that priority too
        parent = (yield from store("load", entry.parent_id)) if entry.parent_id is not None else None
        child = Execution(
            id=entry.child_id,
            definition_id=child_defn.id,
            definition_fqn=fqn,  # persisted so any worker can (re)resolve a submachine child
            root_path=entry.root_path,
            context=context,
            parent_id=entry.parent_id,
            child_id=entry.child_id,
            priority=parent.priority if parent is not None else 0,
        )
        yield from self._run_flow(child, engine.start(child_defn, child))

    def _flush_flow(self) -> Flow:
        """Drive deferred work to quiescence: create the pending children (the spawn
        outbox) and deliver the pending outbox events. Reads the durable store, so a crash
        mid-relay re-runs on restart (children are idempotent, events deduped)."""
        while True:
            spawns = yield from store("pending_spawns")
            outbox = yield from store("pending_outbox")
            if not spawns and not outbox:
                return
            if spawns:
                # each spawn targets a different child_id → independent store rows → safe to
                # run concurrently (their actions overlap where the interpreter allows)
                yield from parallel([self._create_spawn_flow(s) for s in spawns])
                yield from parallel([store("ack_spawn", s.seq) for s in spawns])
            for entry in outbox:
                target = (yield from store("load", entry.target_id)) if entry.target_id is not None else None
                if target is not None and not (yield from store("is_processed", target.id, entry.event.id)):
                    yield from self._run_flow(
                        target,
                        engine.process(self._definition_for(target), target, entry.event),
                        event_id=entry.event.id,
                        event=entry.event,
                    )
                yield from store("ack_outbox", entry.seq)

    def _deliver_timeout_flow(self, execution_id: str, event: Event) -> Flow:
        """Deliver a fired timer's `Timeout` event inline (like the outbox relay). The
        transport driver overrides this to publish it instead."""
        target = yield from store("load", execution_id)
        if target is not None and not (yield from store("is_processed", target.id, event.id)):
            yield from self._run_flow(
                target,
                engine.process(self._definition_for(target), target, event),
                event_id=event.id,
                event=event,
            )
            yield from self._flush_flow()

    def fire_due_timers_flow(self) -> Flow:
        """Deliver every timer due now (a `Timeout` to its execution) and remove it.
        Returns how many fired.

        One at a time, not in parallel: delivering a timeout commits a CAS write on the
        execution, and two timers of the same execution (nested composites with independent
        timeouts) must not race — the second would load a stale version."""
        fired = 0
        for execution_id, path, fire_at in (yield from store("due_timers", self._clock())):
            target = yield from store("load", execution_id)
            if target is not None and target.status is Status.SUSPENDED:
                continue  # left armed: it fires once resumed (the worker path parks it the same way)
            # deliver before delete: a crash between the two is safe — dedup prevents re-delivery
            yield from self._deliver_timeout_flow(
                execution_id, engine.timeout_event(execution_id, path, fire_at)
            )
            yield from store("delete_timer", execution_id, path, fire_at)
            fired += 1
        return fired

    def start_flow(self, exe: Execution) -> Flow:
        yield from self._run_flow(exe, engine.start(self.defn, exe))
        yield from self._flush_flow()

    def inject_flow(self, exe: Execution, event: Event) -> Flow:
        """Process one event for `exe`. A domain event is broadcast to the live regions (a
        region = a child Execution); control events drive `exe` itself. The relay then
        routes the emits (a region's `Finished` back to its parent)."""
        # a submachine `invoke` child is a black box — never broadcast a domain event in
        candidate_ids = [cid for cid, cs in exe.children.items() if not cs.finished and not cs.submachine]
        loaded = yield from parallel([store("load", cid) for cid in candidate_ids])
        live = [child for child in loaded if child is not None]
        broadcast = event.kind not in _CONTROL and bool(live)
        targets = live if broadcast else [exe]

        def deliver_one(target: Execution) -> Flow:
            if (yield from store("is_processed", target.id, event.id)):
                return  # dedupe: at-least-once delivery may re-deliver an event
            yield from self._run_flow(
                target,
                engine.process(self._definition_for(target), target, event),
                event_id=event.id,
                event=event,
            )

        # each target is a distinct execution → independent CAS rows → safe in parallel
        yield from parallel([deliver_one(t) for t in targets])
        if broadcast:
            yield from self._rearm_ttl_flow(exe, event)
        yield from self._flush_flow()

    def _rearm_ttl_flow(self, exe: Execution, event: Event) -> Flow:
        """A domain event broadcast to `exe`'s live regions never runs `process` on `exe`
        itself, but it is still activity for `exe`'s `ttl`: restart the budget."""
        delay = engine.ttl_delay(self._definition_for(exe), exe)
        if delay is None or (yield from store("is_processed", exe.id, event.id)):
            return
        exe.expires_at = self._clock() + delay
        stamp(exe, self._clock())
        yield from store(
            "commit",
            exe,
            [],
            processed_event_id=event.id,
            timers=(TimerOp("schedule", engine.TTL_PATH, exe.expires_at),),
        )


def _error_message(exc: Exception) -> str:
    """`type: message` for `exc`; if it's chained (`__cause__`, set when an `on error`
    handler's own action raised in turn — see `DriverLogic._drive_flow`), append the original
    failure that triggered the (unsuccessful) recovery attempt, so the dead-letter
    doesn't bury the root cause behind the recovery's own failure."""
    msg = f"{type(exc).__name__}: {exc}"
    if exc.__cause__ is not None:
        cause = exc.__cause__
        msg = f"{msg} (while recovering from {type(cause).__name__}: {cause})"
    return msg


class FailOnActionError:
    """The production policy for an action error nothing in the model handles. It is a bug,
    not a modelled failure: neither propagated (it would crash the worker) nor retried (a
    deterministic bug loops) — the execution fails terminally (`status=FAILED` + `error`),
    and the persisted FAILED record is the dead letter."""

    def _on_action_error(self, exe: Execution, exc: Exception) -> None:
        logger.exception("unhandled action error; failing execution %s", exe.id)
        exe.status = Status.FAILED
        exe.error = _error_message(exc)


class TransportDriverLogic(DriverLogic):
    """The driver's logic when deferred effects flow through a transport (the `transport`
    port): a fired timer and the outbox are published instead of run here, and `route`
    fans a domain event out to the live regions' groups."""

    def _deliver_timeout_flow(self, execution_id: str, event: Event) -> Flow:
        exe = yield from store("load", execution_id)
        priority = exe.priority if exe is not None else 0
        yield from transport("publish", execution_id, event, priority=priority)

    def _flush_flow(self, primary_priority: Optional[dict[str, int]] = None) -> Flow:
        while True:
            spawns = yield from store("pending_spawns")
            outbox = yield from store("pending_outbox")
            if not spawns and not outbox:
                return
            if spawns:
                # each spawn targets a different child_id → independent store rows → safe in
                # parallel. Here creating a child only writes its record; its initial event is
                # published through the outbox in the next pass.
                yield from parallel([self._create_spawn_flow(s) for s in spawns])
                yield from parallel([store("ack_spawn", s.seq) for s in spawns])
            for entry in outbox:
                if entry.target_id is not None:
                    # self-targeted re-publish uses this exe's priority (primary_priority);
                    # a cross-execution emit (e.g. a region's Finished -> parent) uses the
                    # TARGET's own priority, not 0, so it doesn't pin the target's group.
                    prio = (primary_priority or {}).get(entry.target_id)
                    if prio is None:
                        target = yield from store("load", entry.target_id)
                        prio = target.priority if target is not None else 0
                    yield from transport("publish", entry.target_id, entry.event, priority=prio)
                yield from store("ack_outbox", entry.seq)

    def route_flow(self, exe: Execution, event: Event) -> Flow:
        """Route `event` to `exe`: a domain event with live regions is published to each
        region's group; anything else runs the engine on `exe` here."""
        live = []
        for cid, cs in exe.children.items():
            if not cs.finished and not cs.submachine:
                child = yield from store("load", cid)
                if child is not None:
                    live.append(child)
        if event.kind not in _CONTROL and live:
            for child in live:
                yield from transport("publish", child.id, event, priority=child.priority)
            timers: tuple[TimerOp, ...] = ()
            delay = engine.ttl_delay(self.defn, exe)  # the broadcast is activity for exe's `ttl`
            if delay is not None:
                exe.expires_at = self._clock() + delay
                timers = (TimerOp("schedule", engine.TTL_PATH, exe.expires_at),)
            stamp(exe, self._clock())
            yield from store("commit", exe, [], processed_event_id=event.id, timers=timers)
            enqueued = False  # broadcast went straight to the transport; nothing in the outbox
        else:
            enqueued = yield from self._run_flow(
                exe, engine.process(self.defn, exe, event), event_id=event.id, event=event
            )
        # only run the relay (its HGETALL round-trips) when this event actually enqueued
        # outbox/spawn work — most events emit nothing. Orphans from a crash are still drained
        # by the next emitting event's relay and by `recover()` on startup (the idle loop never
        # flushed either, so this does not change the at-least-once guarantee).
        if enqueued:
            yield from self._flush_flow(primary_priority={exe.id: exe.priority})


class _InlineDriver(DriverLogic):
    """`DriverLogic` run in the caller's thread over a sync store — the bare `Driver`'s
    `execution="inline"`."""

    def __init__(self, defn: Definition, store: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(defn, *args, **kwargs)
        self.store = store

    @staticmethod
    def store_flow(method: str, *args: Any) -> Flow:
        return store(method, *args)

    def serve(self, flow: Flow) -> Any:
        from harel.engine.aio import facade

        facade._guard_no_running_loop()
        return run_inline(flow, {"store": self.store})
