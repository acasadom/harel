"""The **control plane**: lifecycle commands that bypass the event queue.

Cancel/terminate/suspend/resume are *runtime* operations, not statechart
transitions — UML/statecharts model them as a `terminate` pseudostate or leave
them to the implementation. They act on the `Execution` record directly (a CAS
write to the store), so they take effect at the next event boundary instead of
waiting behind the FIFO backlog of domain events.

Two cancellation modes, decided by the machine itself:

- **Forceful** (`terminate`, and `cancel` of a state with no `Cancel` transition):
  status -> CANCELLED immediately. No hooks run. The backlog drains as no-ops
  (the engine ignores domain events while not RUNNING).
- **Cooperative** (`cancel` of a state that models `on: Cancel`): the machine
  owns its cleanup. The execution goes to CANCELLING and a `Cancel` event is
  enqueued *in the same commit* (transactional outbox — no dual-write). The
  worker then drains the backlog doing nothing until that `Cancel` arrives, at
  which point the machine runs its own cleanup transition (see `distributed` /
  `runtime`). This gives the queue-jump semantics portably (no transport-level
  priority/purge, which SQS FIFO could not provide anyway): the *worker* discards
  the backlog, not the transport.

`suspend`/`resume` are reversible: state, history and the queued backlog are all
preserved; resume returns to RUNNING and processing continues where it stopped.

`redrive` is different in kind: an operator repairing one dead-lettered (`FAILED`)
Execution after fixing the bug that killed it, not a lifecycle transition the model
reacts to. It repositions `active_path` to a caller-chosen leaf of *this*
Execution's own branch and does **not** propagate to (or otherwise touch) an
orthogonal parent's regions — a region is a separate Execution with its own
`root_path`, redriven independently if it also dead-lettered.

All other commands propagate to an orthogonal parent's regions (each region is a
separate child `Execution`/group) and use optimistic-concurrency retry, since a
worker may be committing an event for the same Execution concurrently.
"""

from __future__ import annotations

from typing import Callable, Optional

from harel import engine
from harel.definition.model import Definition, NodeKind, is_descendant
from harel.engine.execution import Execution, Status
from harel.engine.store import ExecutionStore, StoreConflict
from harel.spec.states import Event

_RETRIES = 5

# statuses past which a lifecycle command is a no-op (already finished)
_TERMINAL = (Status.CANCELLED, Status.DONE)


def _children(store: ExecutionStore, exe: Execution) -> list[Execution]:
    """Load an orthogonal parent's live region Executions (direct children)."""
    out: list[Execution] = []
    for cid in exe.children:
        child = store.load(cid)
        if child is not None:
            out.append(child)
    return out


def _commit_status(
    store: ExecutionStore,
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
    """CAS the Execution to `new_status` (and, if `emit_cancel`, enqueue a `Cancel`
    event for itself in the same commit, carrying the caller's `cancel_data` as an
    opaque payload), retrying on a concurrent writer. `require_status`, when given,
    is re-checked on every attempt (not just before the loop) — a no-op once it no
    longer holds, e.g. a concurrent writer already moved the Execution on. `validate`,
    when given, is also called fresh on every attempt (against the just-loaded `exe`)
    and may raise to abort — e.g. a business-rule precondition (`redrive`'s "no live
    children") that a concurrent writer could otherwise invalidate between the first
    check and the winning write. Optional `active_path`/`clear_error`/`clear_history`
    reposition/clear the dead-letter reason/discard stale history in the same write
    (used by `redrive` — a level whose own `on_exit` raised never got to record its
    own history entry, even though descendants that exited cleanly earlier in the
    same cascade did, so a partial, inconsistent history is worse than none)."""
    for _ in range(_RETRIES):
        exe = store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if require_status is not None and exe.status is not require_status:
            return  # precondition no longer holds — someone else already moved it
        if exe.status in _TERMINAL and new_status is not Status.CANCELLED:
            return  # already finished; only a (forceful) terminate may still fire
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
            store.commit(exe, emits)
            return
        except StoreConflict:
            continue


def _propagate(store: ExecutionStore, parent_id: str, new_status: Status) -> None:
    """Forcefully apply `new_status` to a parent's region children (recursively)."""
    parent = store.load(parent_id)
    if parent is None:
        return
    for child in _children(store, parent):
        _commit_status(store, child.id, new_status)
        _propagate(store, child.id, new_status)


def terminate(store: ExecutionStore, execution_id: str) -> None:
    """Forceful cancel: status -> CANCELLED now, no hooks, no cleanup. Regions
    follow. The queued backlog drains as no-ops."""
    _commit_status(store, execution_id, Status.CANCELLED)
    _propagate(store, execution_id, Status.CANCELLED)


def cancel(
    store: ExecutionStore,
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
) -> None:
    """Cancel respecting the machine: cooperative if the active state models a
    `Cancel` transition (-> CANCELLING + an injected `Cancel` for the cleanup),
    forceful terminate otherwise. Regions are terminated forcefully (a cancelled
    parent does not outlive its regions). `reason` is an opaque payload carried on
    the cooperative `Cancel` event, readable by the model's cleanup transition."""
    exe = store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if engine.has_cancel_handler(defn, exe):
        _commit_status(store, execution_id, Status.CANCELLING, emit_cancel=True, cancel_data=reason)
        _propagate(store, execution_id, Status.CANCELLED)
    else:
        terminate(store, execution_id)


def suspend(store: ExecutionStore, execution_id: str) -> None:
    """Pause: RUNNING -> SUSPENDED. State, history and the backlog are preserved.
    No-op if not RUNNING. Regions are suspended too."""
    exe = store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.RUNNING:
        return
    _commit_status(store, execution_id, Status.SUSPENDED)
    _propagate(store, execution_id, Status.SUSPENDED)


def resume(store: ExecutionStore, execution_id: str) -> None:
    """Unpause: SUSPENDED -> RUNNING, continuing where it stopped (the backlog is
    intact). No-op if not SUSPENDED. Regions resume too."""
    exe = store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.SUSPENDED:
        return
    _commit_status(store, execution_id, Status.RUNNING)
    _propagate(store, execution_id, Status.RUNNING)


def _validate_redrive_target(defn: Definition, exe: Execution, target_path: str) -> None:
    """Pure precondition check for `redrive`, re-run against the freshly loaded `exe`
    on every CAS attempt (not just once) — a concurrent writer could otherwise finish
    spawning a child, or the check could otherwise run against a stale snapshot,
    between an initial check and the winning write. Raises `ValueError`."""
    node = defn.index.get(target_path)
    if node is None or node.is_composite:
        raise ValueError(f"redrive target must be a leaf state, got {target_path!r}")
    root = defn.index[exe.root_path]
    if not is_descendant(node, root):
        raise ValueError(
            f"redrive target {target_path!r} is outside this execution's own branch "
            f"(rooted at {exe.root_path!r})"
        )
    # an orthogonal node is never descended into via a plain active_path — the engine
    # always parks AT it and represents each branch as a separate child Execution (see
    # _fork/_descend). A target nested under one bypasses that entirely: it would park
    # active_path deep inside a single branch with no sibling region ever spawned, and
    # `exe.children` — empty if the dead-lettered execution never reached the fork —
    # would vacuously look "fully joined" forever.
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


def redrive(store: ExecutionStore, defn: Definition, execution_id: str, target_path: str) -> None:
    """Force a dead-lettered execution back to life: FAILED -> RUNNING, repositioned
    at `target_path` (a caller-chosen leaf — never inferred from the failed
    `active_path`, which an `on_exit` failure may have left parked mid-cascade on a
    composite, not a valid resting position). Context is untouched — this assumes a
    *code* fix, not a data fix; use it once the bug that dead-lettered the execution
    is fixed. History IS cleared: a level whose own `on_exit` raised never recorded
    its own history entry, even though a descendant that exited cleanly earlier in
    the same cascade did — that partial, inconsistent history is discarded rather
    than risking a later history re-entry restoring the pre-crash position instead
    of wherever you just redrove to. No-op if not FAILED. Like `resume`, does not
    drain automatic transitions inline — the next event/timer picks that up normally.

    Refuses (`ValueError`, see `_validate_redrive_target`) a target outside this
    Execution's own branch — a region spawned by an orthogonal fork has its own
    `root_path`, and `chain()` asserts the root is an ancestor of the active leaf,
    so a foreign target would silently corrupt the record and crash the next event
    it processes. Also refuses a target while any spawned child (region/invoke) is
    still unfinished: an `on_exit` failure can dead-letter a parent before it
    cancels its live regions (see `_take`), and moving the parent elsewhere would
    orphan them — cancel or terminate those children first."""
    exe = store.load(execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.FAILED:
        return
    _validate_redrive_target(defn, exe, target_path)  # fail fast before any CAS work
    _commit_status(
        store,
        execution_id,
        Status.RUNNING,
        require_status=Status.FAILED,
        validate=lambda fresh: _validate_redrive_target(defn, fresh, target_path),
        active_path=target_path,
        clear_error=True,
        clear_history=True,
    )
