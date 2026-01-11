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

All commands propagate to an orthogonal parent's regions (each region is a
separate child `Execution`/group) and use optimistic-concurrency retry, since a
worker may be committing an event for the same Execution concurrently.
"""

from __future__ import annotations

from typing import Optional

from harel import engine
from harel.definition.model import Definition
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
    emit_cancel: bool = False,
    cancel_data: Optional[dict] = None,
) -> None:
    """CAS the Execution to `new_status` (and, if `emit_cancel`, enqueue a `Cancel`
    event for itself in the same commit, carrying the caller's `cancel_data` as an
    opaque payload), retrying on a concurrent writer."""
    for _ in range(_RETRIES):
        exe = store.load(execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if exe.status in _TERMINAL and new_status is not Status.CANCELLED:
            return  # already finished; only a (forceful) terminate may still fire
        exe.status = new_status
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
