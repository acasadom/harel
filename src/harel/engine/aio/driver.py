"""The async interpreter of the (unchanged) sync engine generator.

`AsyncDriver` mirrors `harel.engine.runtime.Driver` line-for-line; the only differences
are awaited IO (store/transport) and the action call site. The engine generator stays
synchronous — `next(gen)`/`gen.send(...)` are CPU between awaits; the loop suspends only
at the awaited action and the awaited store commit. That is the whole point: many
Executions can have their action IO in flight at once on one loop.

Action dispatch (FastAPI-style): a coroutine action is `await`ed; a plain sync action runs
in the default thread pool (`run_in_executor`) so a blocking sync action does NOT freeze
the loop. Reuses `runtime._resolve`/`_Proxy`/`_CONTROL` and the distributed helpers verbatim.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine.execution import Execution, Status
from harel.engine.resolve import ResolveError
from harel.engine.runtime import _CONTROL, _Proxy, _resolve
from harel.engine.store import TimerOp
from harel.spec.states import Event

logger = logging.getLogger(__name__)


class AsyncDriver:
    """Async in-memory runtime: drives Executions through the pure engine, awaiting
    coroutine actions (sync actions go to a thread pool) and the async store. The bare
    driver propagates action errors (so parity tests surface bugs), like the sync `Driver`."""

    def __init__(
        self,
        defn: Definition,
        store: Any,
        clock: Callable[[], float] = time.time,
        definitions: Optional[dict[str, Definition]] = None,
        resolve_machine: Optional[Callable[[str], Definition]] = None,
    ) -> None:
        self.defn = defn
        self.store = store
        self._clock = clock
        self._definitions = definitions
        self.resolve_machine = resolve_machine

    # --- hooks (mirror Driver) --------------------------------------------
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
    async def _run(self, exe: Execution, gen, event_id: Optional[str] = None) -> None:
        emits, timer_ops, spawns = await self._drive(exe, gen)
        await self.store.commit(
            exe, emits, processed_event_id=event_id, timers=tuple(timer_ops), spawns=tuple(spawns)
        )

    async def _call_action(self, fn, proxy, event, inputs: dict):
        """Run a user action. Coroutine actions are awaited; sync actions go to the default
        thread pool so a blocking call doesn't freeze the loop (FastAPI's sync-handler model)."""
        if inspect.iscoroutinefunction(fn):
            return await fn(proxy, event, **inputs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(fn, proxy, event, **inputs))

    async def _drive(
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
                    proxy.idempotency_key = f"{exe.id}:{exe.version}:{action_index}"
                    action_index += 1
                    try:
                        ret = await self._call_action(
                            _resolve(action), proxy, effect.event, dict(action.inputs)
                        )
                    except Exception as exc:
                        self._on_action_error(exe, exc)  # base: re-raises; runtime: fails the exe
                        gen.close()
                        return [], [], []
                    effect = gen.send(engine.ActionResult(value=ret))
                elif isinstance(effect, engine.SpawnChildren):
                    spawns.extend((s.child_id, s.root_path, dict(s.context)) for s in effect.specs)
                    effect = gen.send(None)
                elif isinstance(effect, engine.Emit):
                    emits.append((effect.to, effect.event))
                    effect = gen.send(None)
                elif isinstance(effect, engine.ScheduleTimer):
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

    async def _create_spawn(self, entry) -> None:
        if await self.store.load(entry.child_id) is not None:
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
            definition_fqn=fqn,
            root_path=entry.root_path,
            context=context,
            parent_id=entry.parent_id,
            child_id=entry.child_id,
        )
        await self._run(child, engine.start(child_defn, child))

    async def _flush(self) -> None:
        while True:
            progressed = False
            for spawn in await self.store.pending_spawns():
                await self._create_spawn(spawn)
                await self.store.ack_spawn(spawn.seq)
                progressed = True
            for entry in await self.store.pending_outbox():
                target = await self.store.load(entry.target_id) if entry.target_id is not None else None
                if target is not None and not await self.store.is_processed(target.id, entry.event.id):
                    await self._run(
                        target,
                        engine.process(self._definition_for(target), target, entry.event),
                        event_id=entry.event.id,
                    )
                await self.store.ack_outbox(entry.seq)
                progressed = True
            if not progressed:
                return

    async def _deliver_timeout(self, execution_id: str, event: Event) -> None:
        """Deliver a fired timer's `Timeout` event inline (like the outbox relay). The
        distributed driver overrides this to publish to the transport instead."""
        target = await self.store.load(execution_id)
        if target is not None and not await self.store.is_processed(target.id, event.id):
            await self._run(
                target, engine.process(self._definition_for(target), target, event), event_id=event.id
            )
            await self._flush()

    async def fire_due_timers(self) -> int:
        """Deliver every timer due now (a `Timeout` to its execution) and remove it.
        Returns how many fired. Mirror of the sync `Driver.fire_due_timers`."""
        fired = 0
        for execution_id, path, fire_at in await self.store.due_timers(self._clock()):
            await self._deliver_timeout(execution_id, engine.timeout_event(execution_id, path, fire_at))
            await self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    # --- public API --------------------------------------------------------
    async def recover(self) -> None:
        await self._flush()

    async def start(self, exe: Execution) -> None:
        await self._run(exe, engine.start(self.defn, exe))
        await self._flush()

    async def inject(self, exe: Execution, event: Event) -> None:
        live = [
            child
            for cid, cs in exe.children.items()
            if not cs.finished and not cs.submachine and (child := await self.store.load(cid)) is not None
        ]
        targets = live if (event.kind not in _CONTROL and live) else [exe]
        for target in targets:
            if await self.store.is_processed(target.id, event.id):
                continue
            await self._run(
                target, engine.process(self._definition_for(target), target, event), event_id=event.id
            )
        await self._flush()


class _AsyncRuntimeDriver(AsyncDriver):
    """Production policy: an unhandled action error fails the execution terminally
    (`status=FAILED` + `error`) instead of propagating — mirror of `_RuntimeDriver`."""

    def _on_action_error(self, exe: Execution, exc: Exception) -> None:
        logger.exception("unhandled action error; failing execution %s", exe.id)
        exe.status = Status.FAILED
        exe.error = f"{type(exc).__name__}: {exc}"
