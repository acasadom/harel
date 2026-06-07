"""Driving the pure engine over `Execution`s.

`Driver` is the Execution-centric core: it consumes the engine's effect
generators (`RunAction`/`RunSelector` call the user function; `SpawnChildren`
creates+starts child Executions for an orthogonal fork; `Emit(Finished)` is
routed back to the parent for the join), for a parent Execution plus the child
Executions a fork spawns. It is the single in-memory runtime, shared by tests
(driving raw Executions) and by the production façade. The bare `Driver` lets
action exceptions propagate so test bugs surface; `_RuntimeDriver` (the
production base) fails the execution terminally instead.
"""

from __future__ import annotations

import logging
import time
from importlib import import_module
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.model import ActionRef, Definition
from harel.engine.execution import Execution, Status
from harel.engine.resolve import ResolveError
from harel.engine.store import DictStore, ExecutionStore, TimerOp
from harel.spec.states import Event

# events the parent Execution handles itself (not broadcast to its regions)
_CONTROL = {"Start", "Reset", "Cancel", "SetState", "Finished"}

logger = logging.getLogger(__name__)


def _resolve(action: ActionRef):
    fn = action.function
    if callable(fn):
        return fn
    module_name, name = fn.rsplit(".", 1)
    return getattr(import_module(module_name, action.package), name)


class _Proxy:
    """Stand-in passed to an Execution's actions: exposes its `execution_ctx` and
    a stable `idempotency_key` for the current action (set by the driver before
    each call). The key is `{execution_id}:{version}:{index}` — deterministic, so
    an at-least-once redelivery of the same event reproduces the same key per
    action. Actions (or the FaaS stub) pass it to an external idempotency backend
    to dedupe their side effect; harel itself records nothing (a harel-side record
    would roll back with the failed commit — see `harel.idempotency`)."""

    def __init__(self, context: dict) -> None:
        self.execution_ctx = context
        self.idempotency_key: Optional[str] = None


class Driver:
    """In-memory runtime for a Definition: drives one or more Executions through
    the pure engine, applying effects. Subclasses customize the action proxy, a
    pre-action hook, and the action-error policy."""

    def __init__(
        self,
        defn: Definition,
        store: Optional[ExecutionStore] = None,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
    ) -> None:
        self.defn = defn
        self.store: ExecutionStore = store if store is not None else DictStore()
        self._clock = clock  # injectable so timer fire-times are deterministic in tests
        # multi-Definition support (submachine `invoke`): a registry to process each
        # target with its OWN Definition, and a resolver to spawn a submachine child.
        self._definitions = definitions  # definition_id -> Definition (None => single-defn)
        self.resolve_machine = resolve_machine  # FQN -> Definition (+ registers it)

    def _definition_for(self, exe: Execution) -> Definition:
        """The Definition to drive `exe` with: its own (a submachine child runs a
        different Definition than this driver's `defn`), falling back to `self.defn`."""
        if self._definitions is not None:
            return self._definitions.get(exe.definition_id, self.defn)
        return self.defn

    # --- hooks -------------------------------------------------------------
    def _proxy(self, exe: Execution) -> Any:
        """The object passed to `exe`'s actions (must expose `execution_ctx`)."""
        return _Proxy(exe.context)

    def _before_action(self, exe: Execution, node) -> None:
        """Called just before an action of `exe` runs (e.g. to reflect state)."""

    def _on_action_error(self, exe: Execution, exc: Exception) -> None:
        """Policy when a user action raises. Default: propagate (so a buggy action
        surfaces loudly — the bare Driver is the test/scenario harness). Production
        drivers (`_RuntimeDriver`) override this to fail the execution terminally."""
        raise exc

    # --- core --------------------------------------------------------------
    def register(self, exe: Execution) -> None:
        """Make an Execution known to the store (initial persist)."""
        self.store.save(exe)

    def get(self, execution_id: str) -> Optional[Execution]:
        return self.store.load(execution_id)

    def _run(self, exe: Execution, gen, event_id: Optional[str] = None) -> None:
        """Drive `exe` to quiescence for one event, then atomically checkpoint it,
        enqueue its emitted events to the outbox, and record `event_id` as handled
        (`commit`). The emits are delivered afterwards by `_flush`, i.e. only once
        the Execution that produced them is committed (no dual-write window)."""
        emits, timer_ops, spawns = self._drive(exe, gen)
        self.store.commit(
            exe, emits, processed_event_id=event_id, timers=tuple(timer_ops), spawns=tuple(spawns)
        )

    def _drive(
        self, exe: Execution, gen
    ) -> tuple[list[tuple[Optional[str], Event]], list[TimerOp], list[tuple[str, str, dict]]]:
        emits: list[tuple[Optional[str], Event]] = []
        timer_ops: list[TimerOp] = []
        spawns: list[tuple[str, str, dict]] = []
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
                    try:
                        ret = _resolve(action)(proxy, effect.event, **dict(action.inputs))
                    except Exception as exc:
                        self._on_action_error(exe, exc)  # base: re-raises; runtime: fails the exe
                        gen.close()
                        # drop this event's partial effects; the (possibly FAILED) exe still commits
                        return [], [], []
                    effect = gen.send(engine.ActionResult(value=ret))
                elif isinstance(effect, engine.SpawnChildren):
                    # the fork's children are enqueued (committed atomically with the
                    # parent's join expectations), then created by the relay (_flush)
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
                    timer_ops.append(TimerOp("schedule", effect.path, self._clock() + delay))
                    effect = gen.send(None)
                elif isinstance(effect, engine.CancelTimer):
                    timer_ops.append(TimerOp("cancel", effect.path))
                    effect = gen.send(None)
                else:
                    effect = gen.send(None)
        except StopIteration:
            pass
        return emits, timer_ops, spawns

    def _create_spawn(self, entry) -> None:
        """Create + start one pending child Execution, idempotently: if the child
        already exists (a crash-and-retry re-runs the fork), skip — its progress is
        kept. The child's own emits (e.g. an immediate `Finished`) go to the outbox.
        An orthogonal region shares this driver's `Definition`; a submachine `invoke`
        child runs another, named by the `__invoke_fqn__` riding in its context (so
        the durable spawn schema is untouched) — the runner resolves it."""
        if self.store.load(entry.child_id) is not None:
            return
        context = dict(entry.context)
        fqn = context.pop("__invoke_fqn__", None)
        if fqn is not None:
            if self.resolve_machine is None:
                raise ResolveError(f"invoke {fqn!r} but this runner has no machine resolver")
            child_defn = self.resolve_machine(fqn)
        else:
            child_defn = self.defn
        child = Execution(
            id=entry.child_id,
            definition_id=child_defn.id,
            definition_fqn=fqn,  # persisted so any worker can (re)resolve a submachine child
            root_path=entry.root_path,
            context=context,
            parent_id=entry.parent_id,
            child_id=entry.child_id,
        )
        self._run(child, engine.start(child_defn, child))

    def _flush(self) -> None:
        """Drive deferred work to quiescence: create pending children (the spawn
        outbox — a fork's children, committed atomically with the parent's join
        expectations) and deliver pending outbox events (e.g. a region's `Finished`
        to its parent's join). Reads the durable stores so a crash mid-relay re-runs
        on restart (at-least-once; children are idempotent, events deduped). Loops
        because creating a child or delivering an event may enqueue more of either."""
        while True:
            progressed = False
            for spawn in self.store.pending_spawns():
                self._create_spawn(spawn)
                self.store.ack_spawn(spawn.seq)
                progressed = True
            for entry in self.store.pending_outbox():
                target = self.store.load(entry.target_id) if entry.target_id is not None else None
                if target is not None and not self.store.is_processed(target.id, entry.event.id):
                    self._run(
                        target,
                        engine.process(self._definition_for(target), target, entry.event),
                        event_id=entry.event.id,
                    )
                self.store.ack_outbox(entry.seq)
                progressed = True
            if not progressed:
                return

    def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        """Deliver a fired timer's `Timeout` event. Base: run it inline (like the
        outbox relay). The distributed driver overrides this to publish instead."""
        target = self.store.load(execution_id)
        if target is not None and not self.store.is_processed(target.id, event.id):
            self._run(target, engine.process(self._definition_for(target), target, event), event_id=event.id)
            self._flush()

    def fire_due_timers(self) -> int:
        """Deliver every timer due now (a `Timeout` event to its execution) and
        remove it. The Timeout id is stable, so a timer swept twice takes effect
        once (dedupe). Returns how many fired (for an idle-loop to back off on 0)."""
        fired = 0
        for execution_id, path, fire_at in self.store.due_timers(self._clock()):
            self._deliver_timeout(execution_id, engine.timeout_event(execution_id, path, fire_at))
            self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    # --- public API --------------------------------------------------------
    def recover(self) -> None:
        """Drain the durable outbox: deliver events committed before a crash but
        not yet delivered (the relay entry point on restart). Idempotent — if the
        outbox is empty it does nothing."""
        self._flush()

    def start(self, exe: Execution) -> None:
        self._run(exe, engine.start(self.defn, exe))
        self._flush()

    def inject(self, exe: Execution, event: Event) -> None:
        """Process one event for `exe`. A domain event is broadcast to the live
        regions (a region = a child Execution), mirroring the forward-to-children
        behaviour; control events drive `exe` itself. `Finished` emits are then
        routed back to the parent."""
        # broadcast to live orthogonal regions (they share the event stream); a
        # submachine `invoke` child is black-box — never broadcast a domain event in
        live = [
            child
            for cid, cs in exe.children.items()
            if not cs.finished and not cs.submachine and (child := self.store.load(cid)) is not None
        ]
        targets = live if (event.kind not in _CONTROL and live) else [exe]
        for target in targets:
            if self.store.is_processed(target.id, event.id):
                continue  # dedupe: at-least-once delivery may re-deliver an event
            self._run(target, engine.process(self._definition_for(target), target, event), event_id=event.id)
        self._flush()


class _RuntimeDriver(Driver):
    """The production driver (durable + distributed). An UNHANDLED exception in a
    user action is a programming bug, not a modelled failure: the controlled path
    is expressed in the statechart (a selector/transition). So we neither propagate
    it (that would crash the worker) nor `nack`+retry (a deterministic bug would
    loop forever) — we **fail the execution terminally** (`status=FAILED` + the
    captured `error`) and ack. The persisted FAILED record is the dead-letter."""

    def _on_action_error(self, exe: Execution, exc: Exception) -> None:
        logger.exception("unhandled action error; failing execution %s", exe.id)
        exe.status = Status.FAILED
        exe.error = f"{type(exc).__name__}: {exc}"
