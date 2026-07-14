"""Async control plane — the async mirror of `harel.engine.control`.

Same semantics (terminate/cancel/suspend/resume as CAS writes on the Execution record,
propagated to orthogonal regions, with optimistic-concurrency retry), awaited against an
`AsyncExecutionStore`. `engine.has_cancel_handler` is pure — called directly, no await.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from harel import engine
from harel.definition.model import Definition
from harel.engine.execution import Execution, Status
from harel.engine.store import StoreConflict
from harel.spec.states import Event

_RETRIES = 5
_TERMINAL = (Status.CANCELLED, Status.DONE)


async def _children(store: Any, exe: Execution) -> list[Execution]:
    loaded = await asyncio.gather(*[store.load(cid) for cid in exe.children])
    return [c for c in loaded if c is not None]


async def _commit_status(
    store: Any,
    execution_id: str,
    new_status: Status,
    *,
    emit_cancel: bool = False,
    cancel_data: Optional[dict] = None,
) -> None:
    for _ in range(_RETRIES):
        exe = await store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if exe.status in _TERMINAL and new_status is not Status.CANCELLED:
            return
        exe.status = new_status
        emits: list[tuple[Optional[str], Event]] = (
            [(exe.id, Event(kind="Cancel", data=dict(cancel_data or {})))] if emit_cancel else []
        )
        try:
            await store.commit(exe, emits)
            return
        except StoreConflict:
            continue


async def _propagate(store: Any, parent_id: str, new_status: Status) -> None:
    parent = await store.load(parent_id)
    if parent is None:
        return

    async def _propagate_one(child: Execution) -> None:
        await _commit_status(store, child.id, new_status)
        await _propagate(store, child.id, new_status)

    await asyncio.gather(*[_propagate_one(c) for c in await _children(store, parent)])


async def terminate(store: Any, execution_id: str) -> None:
    await _commit_status(store, execution_id, Status.CANCELLED)
    await _propagate(store, execution_id, Status.CANCELLED)


async def cancel(
    store: Any,
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
) -> None:
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if engine.has_cancel_handler(defn, exe):
        await _commit_status(store, execution_id, Status.CANCELLING, emit_cancel=True, cancel_data=reason)
        await _propagate(store, execution_id, Status.CANCELLED)
    else:
        await terminate(store, execution_id)


async def suspend(store: Any, execution_id: str) -> None:
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.RUNNING:
        return
    await _commit_status(store, execution_id, Status.SUSPENDED)
    await _propagate(store, execution_id, Status.SUSPENDED)


async def resume(store: Any, execution_id: str) -> None:
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.SUSPENDED:
        return
    await _commit_status(store, execution_id, Status.RUNNING)
    await _propagate(store, execution_id, Status.RUNNING)
