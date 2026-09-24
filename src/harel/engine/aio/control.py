"""Async control plane — the async mirror of `harel.engine.control`.

Same semantics (terminate/cancel/suspend/resume as CAS writes on the Execution record,
propagated to orthogonal regions, with optimistic-concurrency retry), awaited against an
`AsyncExecutionStore`. `engine.has_cancel_handler` is pure — called directly, no await.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Optional

from harel import engine
from harel.definition.model import Definition, NodeKind, is_descendant
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
    require_status: Optional[Status] = None,
    validate: Optional[Callable[[Execution], None]] = None,
    active_path: Optional[str] = None,
    clear_error: bool = False,
    clear_history: bool = False,
    emit_cancel: bool = False,
    cancel_data: Optional[dict] = None,
) -> None:
    for _ in range(_RETRIES):
        exe = await store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if require_status is not None and exe.status is not require_status:
            return  # precondition no longer holds — someone else already moved it
        if exe.status in _TERMINAL and new_status is not Status.CANCELLED:
            return
        if validate is not None:
            validate(exe)  # raises to abort — not caught, doesn't count as a retry
        exe.status = new_status
        if active_path is not None:
            exe.active_path = active_path
        if clear_error:
            exe.error = None
        if clear_history:
            exe.history.clear()
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


def _validate_redrive_target(defn: Definition, exe: Execution, target_path: str) -> None:
    """Pure mirror of `harel.engine.control._validate_redrive_target` — see its
    docstring. Re-run against the freshly loaded `exe` on every CAS attempt."""
    node = defn.index.get(target_path)
    if node is None or node.is_composite:
        raise ValueError(f"redrive target must be a leaf state, got {target_path!r}")
    root = defn.index[exe.root_path]
    if not is_descendant(node, root):
        raise ValueError(
            f"redrive target {target_path!r} is outside this execution's own branch "
            f"(rooted at {exe.root_path!r})"
        )
    cur = node.parent
    while cur is not None:
        if cur.kind is NodeKind.ORTHOGONAL:
            raise ValueError(
                f"redrive target {target_path!r} is inside orthogonal state "
                f"{cur.full_path!r} — a single leaf can't represent a fork's parallel "
                f"regions; target a leaf before it instead, and let the model's own "
                f"transition re-fork it normally"
            )
        if cur is root:
            break
        cur = cur.parent
    if any(not cs.finished for cs in exe.children.values()):
        raise ValueError("redrive refused: execution has unfinished children (cancel/terminate them first)")


async def redrive(store: Any, defn: Definition, execution_id: str, target_path: str) -> None:
    """Async mirror of `harel.engine.control.redrive` — see its docstring."""
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.FAILED:
        return
    _validate_redrive_target(defn, exe, target_path)  # fail fast before any CAS work
    await _commit_status(
        store,
        execution_id,
        Status.RUNNING,
        require_status=Status.FAILED,
        validate=lambda fresh: _validate_redrive_target(defn, fresh, target_path),
        active_path=target_path,
        clear_error=True,
    )
