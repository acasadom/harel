"""A headless, durable host for state-machine executions (sync API).

`DurableRunner` drives bare Executions through the pure engine over a persistent
`ExecutionStore`, checkpointing at every event boundary; the `Execution` is the single
source of truth, so a run created in one process resumes in another. Its semantics are
synchronous: `create`/`process` return the execution once the event has been processed.

Its logic is `harel.engine.hosting.DurableLogic`; `execution=` picks how it runs:

- `"background"` (the default) — on the shared background event loop (`harel.engine.aio.
  facade`), as `AsyncDurableRunner`: each call blocks until the loop has done it. A sync
  store is adapted so the async engine can await it; an async store may be passed too.
  Sync actions run in a thread pool.
- `"inline"` — in the caller's own thread, with no event loop: every store call and every
  action runs right there, on the caller's connection and inside the caller's transaction
  (what a framework that keeps a connection per thread — Django, SQLAlchemy sessions —
  needs). It takes a sync store, and refuses a coroutine action.

For async callers, use `AsyncDurableRunner` directly (calling this sync façade from inside
a running event loop is refused with a clear error, in either mode).
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

from harel.definition.model import Definition
from harel.engine import control
from harel.engine.aio import facade
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.execution import Execution
from harel.engine.flow import Flow, run_inline
from harel.engine.hosting import ControlPort, DurableLogic, check_execution
from harel.engine.resolve import MachineResolver
from harel.engine.store import ExecutionStore
from harel.spec.states import Event


class _InlineDurable(DurableLogic):
    """`DurableLogic` run in the caller's thread, over a sync store."""

    def __init__(self, store: ExecutionStore, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._ports = {"store": store, "control": ControlPort(control, store)}

    def serve(self, flow: Flow) -> Any:
        facade._guard_no_running_loop()
        return run_inline(flow, self._ports)


class DurableRunner:
    def __init__(
        self,
        store: ExecutionStore,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
        trace: bool = False,
        *,
        execution: str = "background",
        on_action_error: str = "fail",
    ) -> None:
        """`execution` is `"background"` or `"inline"` (see the module docstring);
        `on_action_error` is `"fail"` (dead-letter the execution) or `"raise"` (the
        exception reaches the caller, and that step isn't committed)."""
        check_execution(execution, ("store", store))
        self.store = store  # kept so callers can introspect the same store object
        self.definitions = definitions
        self.resolver = resolver
        self._clock = clock
        self.execution = execution
        self._inline: Optional[_InlineDurable] = None
        self._async: Optional[AsyncDurableRunner] = None
        if execution == "inline":
            self._inline = _InlineDurable(store, definitions, clock, resolver, trace, on_action_error)
        else:
            # build the async runner ON the shared portal loop (so async backends bind their
            # connection pools to that loop); a sync store is adapted to the async interface
            self._async = facade.run(self._build, store, definitions, clock, resolver, trace, on_action_error)

    @staticmethod
    async def _build(
        store, definitions, clock, resolver, trace=False, on_action_error="fail"
    ) -> AsyncDurableRunner:
        return AsyncDurableRunner(
            facade.as_async_store(store),
            definitions,
            clock,
            resolver,
            trace=trace,
            on_action_error=on_action_error,
        )

    def _do(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Run operation `name` — `DurableLogic.<name>_flow` inline, or the async runner's
        `<name>` on the background loop."""
        if self._inline is not None:
            return self._inline.serve(getattr(self._inline, f"{name}_flow")(*args, **kwargs))
        return facade.run(getattr(self._async, name), *args, **kwargs)

    def create(
        self,
        definition_id: str,
        context: Optional[dict] = None,
        execution_id: Optional[str] = None,
        priority: int = 0,
    ) -> Execution:
        """Create, start and persist a new Execution; return its committed state.

        Pass `execution_id` to use an externally-supplied id (e.g. a Stripe PaymentIntent
        id) instead of a generated one.  Raises `ExecutionAlreadyExists` if that id is
        already in the store.  `priority` (0–4) is stored on the Execution but has no effect
        here — it controls transport claim weighting only under `DistributedRunner` (this
        single-process runner has no transport to weight).
        """
        return self._do("create", definition_id, context, execution_id, priority)

    def process(self, execution_id: str, event: Event) -> Execution:
        """Load a persisted Execution, feed it one event, return the committed state."""
        return self._do("process", execution_id, event)

    def recover(self, definition_id: str) -> None:
        """Drain the durable outbox for `definition_id`'s Executions (relay on restart)."""
        return self._do("recover", definition_id)

    def fire_due_timers(self) -> int:
        """Deliver every timer due now inline; returns how many fired."""
        return self._do("fire_due_timers")

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> Execution:
        """Cancel (cooperative if the model has `on: Cancel`, else forceful); the
        cooperative cleanup runs inline. `reason` is an opaque payload on the `Cancel`."""
        return self._do("cancel", execution_id, reason=reason)

    def terminate(self, execution_id: str) -> Execution:
        """Forcefully cancel `execution_id` now (no cleanup, no hooks)."""
        return self._do("terminate", execution_id)

    def suspend(self, execution_id: str) -> Execution:
        """Pause `execution_id` (reversible; state and backlog preserved)."""
        return self._do("suspend", execution_id)

    def resume(self, execution_id: str) -> Execution:
        """Resume a suspended `execution_id`, continuing where it stopped."""
        return self._do("resume", execution_id)

    def purge(self, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
        """Permanently delete a finished execution tree (root, regions, invokes and all
        their store rows), passing it to `archive` first if given. Refuses a child or a
        tree with a member not DONE/CANCELLED. False if it no longer exists."""
        return self._do("purge", execution_id, archive=archive)

    def redrive(self, execution_id: str, target_path: str) -> Execution:
        """Force a dead-lettered (FAILED) `execution_id` back to RUNNING at
        `target_path` (a leaf state you choose). Use once the bug that failed it is
        fixed; context is untouched. No-op if not FAILED."""
        return self._do("redrive", execution_id, target_path)
