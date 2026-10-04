"""Observing an execution's steps: `ObservedStore(store, on_step)`.

Every writer describes each commit with a `Step` (`harel.engine.store.base`): what caused it —
an event, a start, a control-plane command — and how the execution moved: the state and the
status before and after, the actions it ran. `ObservedStore` wraps any store and, once a commit
of the store it wraps has returned, calls `on_step(step, execution)`. That is where to hang
whatever reacts to an execution's progress — metrics, an audit log, a notification when one
finishes or fails — without touching the machines or the runners:

```text
def on_step(step, exe):
    if step.to_status is not step.from_status and exe.status is Status.FAILED:
        alert(f"{exe.id} failed: {exe.error}")

runner = DurableRunner(ObservedStore(SqliteStore("state.db"), on_step), definitions)
```

`on_step` runs after the commit, in the writer's thread or loop, and gets a copy of the
execution. If it raises, the error is logged and the step stands — it is already committed.

A store whose commits join a transaction it doesn't end — one on the caller's connection — has
committed nothing durable when `commit` returns: such a store reads `step` in its own `commit`,
and defers what it does to the transaction's own commit.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Awaitable, Callable, Optional, Union

from harel.engine.execution import Execution
from harel.engine.store import Step, TimerOp
from harel.spec.states import Event

logger = logging.getLogger(__name__)


class ObservedStore:
    """`store`, telling `on_step(step, execution)` about every commit once it has returned.
    Everything else is the wrapped store's."""

    def __init__(self, store: Any, on_step: Callable[[Step, Execution], Any]) -> None:
        self.store = store
        self._on_step = on_step

    def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
        step: Optional[Step] = None,
    ) -> list[int]:
        seqs = self.store.commit(
            exe,
            emits,
            processed_event_id=processed_event_id,
            timers=timers,
            spawns=spawns,
            trace=trace,
            step=step,
        )
        if step is not None:
            try:
                self._on_step(step, exe.model_copy(deep=True))
            except Exception:
                logger.exception("on_step raised for execution %s; the step is committed", exe.id)
        return seqs

    def __getattr__(self, name: str) -> Any:
        return getattr(self.store, name)


class AsyncObservedStore:
    """`ObservedStore` for an async store; `on_step` may be a plain or a coroutine function."""

    def __init__(self, store: Any, on_step: Callable[[Step, Execution], Union[Any, Awaitable[Any]]]) -> None:
        self.store = store
        self._on_step = on_step

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict], ...] = (),
        trace: Optional[dict] = None,
        step: Optional[Step] = None,
    ) -> list[int]:
        seqs = await self.store.commit(
            exe,
            emits,
            processed_event_id=processed_event_id,
            timers=timers,
            spawns=spawns,
            trace=trace,
            step=step,
        )
        if step is not None:
            try:
                result = self._on_step(step, exe.model_copy(deep=True))
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("on_step raised for execution %s; the step is committed", exe.id)
        return seqs

    def __getattr__(self, name: str) -> Any:
        return getattr(self.store, name)
