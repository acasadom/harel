"""A headless, durable host for state-machine executions.

`DurableRunner` drives **bare Executions** through the pure engine (the
`Driver`) over a persistent `ExecutionStore` — there is no public `StateMachine`
object; the `Execution` is the single source of truth, checkpointed at every
event boundary. Because the store survives the process, a run can be created in
one process and resumed in another: load the Execution by id and feed it the
next event.

The `Driver` is stateless per call (a function of `Definition` + store), so a
fresh one is used each step. `Definition`s are looked up from a registry by
`definition_id` (rebuilt from their source — YAML/spec — by the host).

Out of scope here (kept for later): at-least-once event de-duplication
(idempotency), mid-event resume (`pending`), and a distributed transport for
orthogonal regions running in separate processes.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine import control
from harel.engine.execution import Execution
from harel.engine.resolve import MachineResolver, ResolveError
from harel.engine.runtime import Driver, _RuntimeDriver
from harel.engine.store import ExecutionStore
from harel.spec.states import Event


class DurableRunner:
    def __init__(
        self,
        store: ExecutionStore,
        definitions: dict[str, Definition],
        clock: Callable[[], float] = time.time,
        resolver: Optional[MachineResolver] = None,
    ) -> None:
        self.store = store
        self.definitions = definitions  # definition_id -> Definition
        # inline `invoke` targets ship with their parent: register them by id (= their
        # synthetic FQN) so they resolve without an external resolver
        for defn in list(definitions.values()):
            self.definitions.update({s.id: s for s in defn.submachines.values()})
        self._clock = clock  # injectable so durable timers fire deterministically in tests
        self.resolver = resolver  # FQN -> Definition for submachine `invoke` (optional)

    def _resolve_machine(self, fqn: str) -> Definition:
        if fqn in self.definitions:  # an inline submachine (id == synthetic FQN)
            return self.definitions[fqn]
        if self.resolver is None:
            raise ResolveError(f"invoke {fqn!r} but DurableRunner has no resolver")
        defn = self.resolver.resolve(fqn)
        self.definitions[defn.id] = defn  # register so the child routes by its own id
        return defn

    def _driver(self, definition_id: str) -> Driver:
        # the production policy: an unhandled action error fails the execution
        # terminally (FAILED + error) instead of propagating to the caller.
        return _RuntimeDriver(
            self.definitions[definition_id],
            store=self.store,
            clock=self._clock,
            definitions=self.definitions,
            resolve_machine=self._resolve_machine,
        )

    def create(self, definition_id: str, context: Optional[dict] = None) -> Execution:
        """Create, start and persist a new Execution; return its committed state."""
        exe = Execution(definition_id=definition_id, context=dict(context or {}))
        self._driver(definition_id).start(exe)
        loaded = self.store.load(exe.id)
        assert loaded is not None
        return loaded

    def process(self, execution_id: str, event: Event) -> Execution:
        """Load a persisted Execution, feed it one event, and return the
        committed state (the Driver checkpoints it via the store)."""
        exe = self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        self._driver(exe.definition_id).inject(exe, event)
        loaded = self.store.load(execution_id)
        assert loaded is not None
        return loaded

    def recover(self, definition_id: str) -> None:
        """Drain the durable outbox for `definition_id`'s Executions (the relay
        entry point on restart): deliver any events committed before a crash but
        not yet delivered (e.g. a region's `Finished` that never reached the
        parent's join). Idempotent."""
        self._driver(definition_id).recover()

    def fire_due_timers(self) -> int:
        """Deliver every timer due now inline (a `Timeout` to its execution),
        resolving each execution's own Definition, then remove the timer. The
        Timeout id is stable so a re-fire is deduped. Returns how many fired."""
        fired = 0
        for execution_id, path, fire_at in self.store.due_timers(self._clock()):
            exe = self.store.load(execution_id)
            if exe is not None and exe.definition_id in self.definitions:
                event = engine.timeout_event(execution_id, path, fire_at)
                self._driver(exe.definition_id)._deliver_timeout(execution_id, event)
            self.store.delete_timer(execution_id, path, fire_at)
            fired += 1
        return fired

    # --- control plane (lifecycle commands; bypass the event queue) ---------
    def cancel(self, execution_id: str, *, reason: Optional[dict] = None) -> Execution:
        """Cancel `execution_id` (cooperative if it models `on: Cancel`, else
        forceful). The cooperative path's cleanup transition runs inline here.
        `reason` is an opaque payload carried on the `Cancel` event for the model."""
        exe = self.store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        driver = self._driver(exe.definition_id)
        control.cancel(self.store, driver.defn, execution_id, reason=reason)
        driver.recover()  # deliver the injected Cancel inline (runs the cleanup)
        loaded = self.store.load(execution_id)
        assert loaded is not None
        return loaded

    def terminate(self, execution_id: str) -> Execution:
        """Forcefully cancel `execution_id` now (no cleanup, no hooks)."""
        control.terminate(self.store, execution_id)
        loaded = self.store.load(execution_id)
        assert loaded is not None
        return loaded

    def suspend(self, execution_id: str) -> Execution:
        """Pause `execution_id` (reversible; state and backlog preserved)."""
        control.suspend(self.store, execution_id)
        loaded = self.store.load(execution_id)
        assert loaded is not None
        return loaded

    def resume(self, execution_id: str) -> Execution:
        """Resume a suspended `execution_id`, continuing where it stopped."""
        control.resume(self.store, execution_id)
        loaded = self.store.load(execution_id)
        assert loaded is not None
        return loaded
