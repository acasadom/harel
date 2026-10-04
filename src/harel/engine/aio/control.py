"""Async control plane: `harel.engine.control`'s commands run with coroutines, over an
`AsyncExecutionStore`.

Each command is the same flow as the sync one (`control.terminate_flow`, ...) — same
semantics, see `harel.engine.control`'s module docstring — served by the coroutine
interpreter: every store call is awaited, and the regions a command propagates to are
updated concurrently. A purge's `archive` may be a plain function or a coroutine function.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Iterable, Optional

from harel.definition.model import Definition
from harel.engine import control
from harel.engine.control import _PURGEABLE, PurgeRefused, PurgeReport  # noqa: F401 (re-exported)
from harel.engine.execution import Status
from harel.engine.flow import Flow, run_async


async def _serve(flow: Flow, store: Any) -> Any:
    return await run_async(flow, {"store": store})


async def terminate(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`control.terminate_flow` over `store`."""
    await _serve(control.terminate_flow(execution_id, clock=clock), store)


async def cancel(
    store: Any,
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
    clock: Callable[[], float] = time.time,
) -> None:
    """`control.cancel_flow` over `store`."""
    await _serve(control.cancel_flow(defn, execution_id, reason=reason, clock=clock), store)


async def suspend(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`control.suspend_flow` over `store`."""
    await _serve(control.suspend_flow(execution_id, clock=clock), store)


async def resume(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`control.resume_flow` over `store`."""
    await _serve(control.resume_flow(execution_id, clock=clock), store)


async def redrive(
    store: Any,
    defn: Definition,
    execution_id: str,
    target_path: str,
    *,
    clock: Callable[[], float] = time.time,
) -> None:
    """`control.redrive_flow` over `store`."""
    await _serve(control.redrive_flow(defn, execution_id, target_path, clock=clock), store)


async def purge(store: Any, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
    """`control.purge_flow` over `store`."""
    return await _serve(control.purge_flow(execution_id, archive=archive), store)


async def purge_finished(
    store: Any,
    *,
    older_than: float,
    statuses: Iterable[Status] = _PURGEABLE,
    archive: Optional[Callable[[dict], Any]] = None,
    include_undated: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
    now: Optional[float] = None,
) -> PurgeReport:
    """`control.purge_finished_flow` over `store`."""
    return await _serve(
        control.purge_finished_flow(
            older_than=older_than,
            statuses=statuses,
            archive=archive,
            include_undated=include_undated,
            limit=limit,
            dry_run=dry_run,
            now=now,
        ),
        store,
    )
