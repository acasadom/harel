"""Async control plane — the async mirror of `harel.engine.control`.

Same semantics (terminate/cancel/suspend/resume as CAS writes on the Execution record,
propagated to orthogonal regions, with optimistic-concurrency retry), awaited against an
`AsyncExecutionStore`. `engine.has_cancel_handler` is pure — called directly, no await.
"""

# See harel.engine.control's module docstring for the full cooperative-vs-forceful
# cancel design, including why `Cancel` must resolve directly to a terminal.

from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Callable, Iterable, Optional

from harel import engine
from harel.definition.model import Definition, NodeKind, is_descendant
from harel.engine.control import (
    _PURGEABLE,
    PurgeRefused,
    PurgeReport,
    _archive_bundle,
    _check_purgeable,
    _purge_statuses,
    _take_candidates,
    _tree_of,
)
from harel.engine.execution import Execution, Status, stamp
from harel.engine.store import StoreConflict, TimerOp
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
    rearm_ttl_of: Optional[Definition] = None,
    clock: Callable[[], float] = time.time,
) -> None:
    for _ in range(_RETRIES):
        exe = await store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if require_status is not None and exe.status is not require_status:
            return  # precondition no longer holds — someone else already moved it
        if exe.status in _TERMINAL:
            return  # already finished; no control-plane command changes it further
        if validate is not None:
            validate(exe)  # raises to abort — not caught, doesn't count as a retry
        exe.status = new_status
        if active_path is not None:
            exe.active_path = active_path
        if clear_error:
            exe.error = None
        if clear_history:
            exe.history.clear()
        timers: tuple[TimerOp, ...] = ()
        delay = engine.ttl_delay(rearm_ttl_of, exe) if rearm_ttl_of is not None else None
        if delay is not None:  # the machine's `ttl` restarts from this write
            exe.expires_at = clock() + delay
            timers = (TimerOp("schedule", engine.TTL_PATH, exe.expires_at),)
        try:
            stamp(exe, clock())
            await store.commit(exe, [], timers=timers)
            return
        except StoreConflict:
            continue


async def _propagate(
    store: Any, parent_id: str, new_status: Status, *, clock: Callable[[], float] = time.time
) -> None:
    parent = await store.load(parent_id)
    if parent is None:
        return

    async def _propagate_one(child: Execution) -> None:
        await _commit_status(store, child.id, new_status, clock=clock)
        await _propagate(store, child.id, new_status, clock=clock)

    await asyncio.gather(*[_propagate_one(c) for c in await _children(store, parent)])


async def terminate(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """Forceful cancel: status -> CANCELLED now, no hooks, no cleanup. No-op if the
    execution already finished (`_commit_status`'s terminal guard)."""
    await _commit_status(store, execution_id, Status.CANCELLED, clock=clock)
    await _propagate(store, execution_id, Status.CANCELLED, clock=clock)


async def cancel(
    store: Any,
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
    clock: Callable[[], float] = time.time,
) -> None:
    """Async mirror of `harel.engine.control.cancel` — see its docstring. No-op if
    the execution already finished: a validator rule requires `on Cancel` to
    resolve directly to a terminal, so the cooperative path always completes in the
    same step the injected `Cancel` is processed — there is no window where a
    second `cancel()` could find the execution "mid cleanup". A `FAILED`
    execution is always terminated forcefully, and the cooperative decision is
    re-made on a concurrent write."""
    for _ in range(_RETRIES):
        exe = await store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if exe.status in _TERMINAL:
            return
        cancel_event = Event(kind="Cancel", data=dict(reason or {}))
        if exe.status is Status.FAILED or not engine.has_cancel_handler(defn, exe, cancel_event):
            await terminate(store, execution_id, clock=clock)
            return
        exe.status = Status.CANCELLING
        stamp(exe, clock())
        try:
            await store.commit(exe, [(exe.id, cancel_event)])
        except StoreConflict:
            continue
        await _propagate(store, execution_id, Status.CANCELLED, clock=clock)
        return


async def suspend(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.RUNNING:
        return
    await _commit_status(store, execution_id, Status.SUSPENDED, clock=clock)
    await _propagate(store, execution_id, Status.SUSPENDED, clock=clock)


async def resume(store: Any, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    exe = await store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.SUSPENDED:
        return
    await _commit_status(store, execution_id, Status.RUNNING, clock=clock)
    await _propagate(store, execution_id, Status.RUNNING, clock=clock)


def _validate_redrive_target(defn: Definition, exe: Execution, target_path: str) -> None:
    """Pure mirror of `harel.engine.control._validate_redrive_target` — see its
    docstring. Re-run against the freshly loaded `exe` on every CAS attempt. A
    third copy of this same invariant (leaf, own branch, not orthogonal-nested)
    lives in `engine.is_valid_reposition_target`, as a single boolean rather than
    per-check `ValueError`s — update all three if it ever changes."""
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


async def _collect_tree(store: Any, root: Execution) -> tuple[list[Execution], list[str]]:
    """Async mirror of `harel.engine.control._collect_tree`."""
    found = {
        i: e for i in await store.ids_with_prefix(root.id + ":") if (e := await store.load(i)) is not None
    }
    frontier = [root, *found.values()]
    while frontier:
        for cid in frontier.pop().children:
            if cid != root.id and cid not in found and (child := await store.load(cid)) is not None:
                found[cid] = child
                frontier.append(child)
    return _tree_of(root, found)


async def purge(store: Any, execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> bool:
    """Async mirror of `harel.engine.control.purge` — see its docstring. `archive` may
    be a plain function or a coroutine function."""
    root = await store.load(execution_id)
    if root is None:
        return False
    tree, missing = await _collect_tree(store, root)
    _check_purgeable(root, tree)
    if archive is not None:
        traces = {e.id: await store.read_trace(e.id) for e in tree}
        result = archive(_archive_bundle(root, tree, traces))
        if inspect.isawaitable(result):
            await result
    for exe in reversed(tree[1:]):
        await _purge_one(store, exe)
    for cid in missing:
        await store.purge(cid, 0)  # sweep what an interrupted purge left behind (version unused when absent)
    await _purge_one(store, root)
    return True


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
    """Async mirror of `harel.engine.control.purge_finished` — see its docstring. `archive`
    may be a plain function or a coroutine function."""
    chosen = _purge_statuses(statuses)
    cutoff = (time.time() if now is None else now) - older_than
    report = PurgeReport()
    candidates: list[str] = []
    cursor: Optional[str] = None
    while limit is None or len(candidates) < limit:
        page = await store.list_executions(status=chosen, roots_only=True, limit=500, cursor=cursor)
        _take_candidates(page.items, cutoff, include_undated, report, candidates)
        cursor = page.next_cursor
        if cursor is None:
            break
    candidates = candidates[:limit]
    if dry_run:
        for root_id in candidates:
            root = await store.load(root_id)
            if root is None:
                continue
            try:
                _check_purgeable(root, (await _collect_tree(store, root))[0])
            except PurgeRefused as exc:
                report.refused[root_id] = str(exc)
            else:
                report.purged.append(root_id)
        return report
    for root_id in candidates:
        try:
            if await purge(store, root_id, archive=archive):
                report.purged.append(root_id)
        except (PurgeRefused, StoreConflict) as exc:  # anything else, e.g. the archiver, stops the run
            report.refused[root_id] = str(exc)
    return report


async def _purge_one(store: Any, exe: Execution) -> None:
    if not await store.purge(exe.id, exe.version):
        raise StoreConflict(exe.id, expected=exe.version, found=None)


async def redrive(
    store: Any,
    defn: Definition,
    execution_id: str,
    target_path: str,
    *,
    clock: Callable[[], float] = time.time,
) -> None:
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
        clear_history=True,
        rearm_ttl_of=defn,
        clock=clock,
    )
