"""The **control plane**: lifecycle commands that bypass the event queue.

Cancel/terminate/suspend/resume are *runtime* operations, not statechart
transitions — UML/statecharts model them as a `terminate` pseudostate or leave
them to the implementation. They act on the `Execution` record directly (a CAS
write to the store), so they take effect at the next event boundary instead of
waiting behind the FIFO backlog of domain events.

Two cancellation modes, decided by the machine itself:

- **Forceful** (`terminate`, `cancel` of a state with no `Cancel` transition, and
  `cancel` of a dead-lettered `FAILED` execution): status -> CANCELLED
  immediately. No hooks run. The backlog drains as no-ops (the engine ignores
  domain events while not RUNNING). On a `FAILED` execution this abandons the
  dead letter; `error` is kept.
- **Cooperative** (`cancel` of a state that models `on: Cancel`): the machine
  owns its cleanup. The execution goes to CANCELLING and a `Cancel` event is
  enqueued *in the same commit* (transactional outbox — no dual-write). The
  worker then drains the backlog doing nothing until that `Cancel` arrives, at
  which point the machine runs its own cleanup transition (see `distributed` /
  `runtime`). This gives the queue-jump semantics portably (no transport-level
  priority/purge, which SQS FIFO could not provide anyway): the *worker* discards
  the backlog, not the transport.

`Cancel` is the control plane's own teardown signal, not a business event: a
validator rule requires its transition to resolve directly to a terminal, so the
cooperative path always finishes in the very same step the injected `Cancel` is
processed — it never leaves the execution parked mid-cleanup, indistinguishable
from an ordinary RUNNING execution, waiting on some further event. A model whose
own domain logic needs a multi-step or asynchronous unwind (release a lock now,
wait for an external refund later, ...) is modelling a *business* cancellation,
not an execution one — that belongs on its own event name (e.g. `CancelOrder`),
handled with ordinary transitions, with no relation to `cancel()` at all.

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

Each command is written once, as a flow over the store (`*_flow`, see `harel.engine.flow`):
the functions here run it in the caller's thread over a sync `ExecutionStore`, and
`harel.engine.aio.control` runs the same flow with coroutines over an async one. The
runners' own control plane (`DurableRunner.cancel`, ...) runs these flows too.
"""

from __future__ import annotations

import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from harel import engine
from harel.definition.model import Definition, NodeKind, is_descendant
from harel.engine.execution import Execution, ExecutionSummary, Status, stamp
from harel.engine.flow import Await, Flow, call, parallel, run_inline
from harel.engine.store import ExecutionStore, Step, StoreConflict, TimerOp
from harel.spec.states import Event

_RETRIES = 5

# statuses past which a lifecycle command is a no-op (already finished)
_TERMINAL = (Status.CANCELLED, Status.DONE)


def _store(method: str, *args: Any, **kwargs: Any) -> Flow:
    return (yield from call("store", method, *args, **kwargs))


def _inline(flow: Flow, store: ExecutionStore) -> Any:
    return run_inline(flow, {"store": store})


def _children_flow(exe: Execution) -> Flow:
    """Load an orthogonal parent's live region Executions (direct children)."""
    loaded = yield from parallel([_store("load", cid) for cid in exe.children])
    return [child for child in loaded if child is not None]


def _commit_status_flow(
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
    command: str,
) -> Flow:
    """CAS the Execution to `new_status`, retrying on a concurrent writer. `command` names the
    control-plane command for the commit's `Step`.
    `require_status`, when given, is re-checked on every attempt (not just before
    the loop) — a no-op once it no longer holds, e.g. a concurrent writer already
    moved the Execution on. `validate`,
    when given, is also called fresh on every attempt (against the just-loaded `exe`)
    and may raise to abort — e.g. a business-rule precondition (`redrive`'s "no live
    children") that a concurrent writer could otherwise invalidate between the first
    check and the winning write. Optional `active_path`/`clear_error`/`clear_history`
    reposition/clear the dead-letter reason/discard stale history in the same write
    (used by `redrive` — a level whose own `on_exit` raised never got to record its
    own history entry, even though descendants that exited cleanly earlier in the
    same cascade did, so a partial, inconsistent history is worse than none)."""
    for _ in range(_RETRIES):
        exe = yield from _store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if require_status is not None and exe.status is not require_status:
            return  # precondition no longer holds — someone else already moved it
        if exe.status in _TERMINAL:
            return  # already finished; no control-plane command changes it further
        if validate is not None:
            validate(exe)  # raises to abort — not caught, doesn't count as a retry
        from_status, from_path = exe.status, exe.active_path
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
            yield from _store(
                "commit", exe, [], timers=timers, step=_control_step(command, from_status, from_path, exe)
            )
            return
        except StoreConflict:
            continue


def _control_step(command: str, from_status: Status, from_path: Optional[str], exe: Execution) -> Step:
    return Step(
        cause="control",
        command=command,
        from_status=from_status,
        to_status=exe.status,
        from_path=from_path,
        to_path=exe.active_path,
    )


def _propagate_flow(
    parent_id: str, new_status: Status, *, clock: Callable[[], float] = time.time, command: str
) -> Flow:
    """Forcefully apply `new_status` to a parent's region children (recursively). Each
    region is its own record, so they're independent (concurrent where the interpreter
    allows)."""
    parent = yield from _store("load", parent_id)
    if parent is None:
        return

    def propagate_one(child: Execution) -> Flow:
        yield from _commit_status_flow(child.id, new_status, clock=clock, command=command)
        yield from _propagate_flow(child.id, new_status, clock=clock, command=command)

    yield from parallel([propagate_one(child) for child in (yield from _children_flow(parent))])


def terminate_flow(execution_id: str, *, clock: Callable[[], float] = time.time) -> Flow:
    """Forceful cancel: status -> CANCELLED now, no hooks, no cleanup. Regions
    follow. The queued backlog drains as no-ops. No-op if the execution already
    finished (`_commit_status_flow`'s terminal guard) — it does not retroactively
    reclassify a `DONE` execution as `CANCELLED`."""
    yield from _terminate_flow(execution_id, clock=clock, command="terminate")


def _terminate_flow(execution_id: str, *, clock: Callable[[], float], command: str) -> Flow:
    yield from _commit_status_flow(execution_id, Status.CANCELLED, clock=clock, command=command)
    yield from _propagate_flow(execution_id, Status.CANCELLED, clock=clock, command=command)


def cancel_flow(
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
    clock: Callable[[], float] = time.time,
) -> Flow:
    """Cancel respecting the machine: cooperative if the active state models a
    `Cancel` transition (-> CANCELLING + an injected `Cancel` for the cleanup),
    forceful terminate otherwise. Regions are terminated forcefully (a cancelled
    parent does not outlive its regions). `reason` is an opaque payload carried on
    the cooperative `Cancel` event, readable by the model's cleanup transition (and
    by its guard, e.g. `on Cancel where reason == "user_request"`). No-op if the
    execution already finished (`DONE`/`CANCELLED`) — a validator rule requires
    `on Cancel` to resolve directly to a terminal, so the cooperative path always
    completes in the same step the injected `Cancel` is processed; there is no
    window where a second `cancel()` could find the execution "mid cleanup".

    A dead-lettered (`FAILED`) execution is always terminated forcefully: its
    position is not a trustworthy resting state (an `on_exit` failure can leave it
    on a composite mid-cascade), so running the model's cleanup from it is unsafe.
    The cooperative decision and its write are one CAS attempt: on a concurrent
    write (a worker failing the execution, or moving it elsewhere) the decision is
    re-made against the fresh record rather than applied to a stale one."""
    for _ in range(_RETRIES):
        exe = yield from _store("load", execution_id)
        if exe is None:
            raise KeyError(execution_id)
        if exe.status in _TERMINAL:
            return
        cancel_event = Event(kind="Cancel", data=dict(reason or {}))
        if (
            exe.status is Status.FAILED
            or engine.definition_problem(defn, exe) is not None  # its state is gone: no cleanup to run
            or not engine.has_cancel_handler(defn, exe, cancel_event)
        ):
            yield from _terminate_flow(execution_id, clock=clock, command="cancel")
            return
        from_status = exe.status
        exe.status = Status.CANCELLING
        stamp(exe, clock())
        try:
            step = _control_step("cancel", from_status, exe.active_path, exe)
            yield from _store("commit", exe, [(exe.id, cancel_event)], step=step)
        except StoreConflict:
            continue
        yield from _propagate_flow(execution_id, Status.CANCELLED, clock=clock, command="cancel")
        return


def suspend_flow(execution_id: str, *, clock: Callable[[], float] = time.time) -> Flow:
    """Pause: RUNNING -> SUSPENDED. State, history and the backlog are preserved.
    No-op if not RUNNING. Regions are suspended too."""
    exe = yield from _store("load", execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.RUNNING:
        return
    yield from _commit_status_flow(execution_id, Status.SUSPENDED, clock=clock, command="suspend")
    yield from _propagate_flow(execution_id, Status.SUSPENDED, clock=clock, command="suspend")


def resume_flow(execution_id: str, *, clock: Callable[[], float] = time.time) -> Flow:
    """Unpause: SUSPENDED -> RUNNING, continuing where it stopped (the backlog is
    intact). No-op if not SUSPENDED. Regions resume too."""
    exe = yield from _store("load", execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.SUSPENDED:
        return
    yield from _commit_status_flow(execution_id, Status.RUNNING, clock=clock, command="resume")
    yield from _propagate_flow(execution_id, Status.RUNNING, clock=clock, command="resume")


def _validate_redrive_target(defn: Definition, exe: Execution, target_path: str) -> None:
    """Pure precondition check for `redrive`, re-run against the freshly loaded `exe`
    on every CAS attempt (not just once) — a concurrent writer could otherwise finish
    spawning a child, or the check could otherwise run against a stale snapshot,
    between an initial check and the winning write. Raises `ValueError`.

    Same three structural checks as `engine.is_valid_reposition_target` (leaf, within
    this Execution's own branch, not nested inside an orthogonal ancestor) — kept as its
    own walk here, not a call to that shared boolean, because `redrive` is
    control-plane-invoked and wants a precise, caller-facing reason for exactly which
    check failed. If the invariant ever changes, update both."""
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


def redrive_flow(
    defn: Definition,
    execution_id: str,
    target_path: str,
    *,
    clock: Callable[[], float] = time.time,
) -> Flow:
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
    exe = yield from _store("load", execution_id)
    if exe is None:
        raise KeyError(execution_id)
    if exe.status is not Status.FAILED:
        return
    _validate_redrive_target(defn, exe, target_path)  # fail fast before any CAS work
    yield from _commit_status_flow(
        execution_id,
        Status.RUNNING,
        require_status=Status.FAILED,
        validate=lambda fresh: _validate_redrive_target(defn, fresh, target_path),
        active_path=target_path,
        clear_error=True,
        clear_history=True,
        rearm_ttl_of=defn,
        clock=clock,
        command="redrive",
    )


# statuses a purged tree may be in: finished for good. FAILED is a dead letter awaiting
# redrive, so it must be deliberately abandoned with `terminate` first.
_PURGEABLE = (Status.DONE, Status.CANCELLED)


class PurgeRefused(ValueError):
    """`purge` refused a tree: not a root, or a member not finished. A `ValueError`, so a
    caller treating any refusal as a bad request still catches it."""


def _check_purgeable(root: Execution, tree: list[Execution]) -> None:
    """Pure precondition for `purge` (shared with the async control plane): `root` is a
    root, and every member of its tree is finished. Raises `PurgeRefused`."""
    if root.parent_id is not None:
        raise PurgeRefused(
            f"purge refused: {root.id!r} is a child of {root.parent_id!r} — purge its root, "
            f"which removes the whole tree"
        )
    live = [e for e in tree if e.status not in _PURGEABLE]
    if live:
        listed = ", ".join(f"{e.id} ({e.status.name})" for e in live)
        hint = (
            " — terminate a dead letter to abandon it" if any(e.status is Status.FAILED for e in live) else ""
        )
        raise PurgeRefused(f"purge refused: not finished: {listed}{hint}")


def _archive_bundle(root: Execution, tree: list[Execution], traces: dict[str, list[dict]]) -> dict:
    """What `purge` hands the archiver: the whole tree, root first, plus each trace."""
    return {
        "root_id": root.id,
        "executions": [e.model_dump(mode="json") for e in tree],
        "traces": traces,
    }


def _tree_of(root: Execution, found: dict[str, Execution]) -> tuple[list[Execution], list[str]]:
    """`root` and its descendants among `found`, parents before children; plus the ids a
    member lists in `children` that are no longer stored. A descendant is recognised by
    its `parent_id` chain reaching `root`, so an unrelated execution in `found` (a root
    whose caller-chosen id merely shares the prefix) is never taken in."""
    members = {root.id: root}
    added = True
    while added:
        added = False
        for exe in found.values():
            if exe.id not in members and exe.parent_id in members:
                members[exe.id] = exe
                added = True
    tree = list(members.values())
    missing = [cid for exe in tree for cid in exe.children if cid not in members]
    return tree, missing


def _collect_tree_flow(root: Execution) -> Flow:
    """The root and every descendant still stored (regions, invokes, fan-out instances),
    parents first. Two sources, since neither alone is complete: a parent drops a child
    from `children` once done with it (an invoke that returned, a fork or fan-out
    re-entered, a `Reset`) while its record stays in the store — found by the id prefix a
    child's id extends its parent's with; and `children` lists what a parent spawned."""
    found: dict[str, Execution] = {}
    for i in (yield from _store("ids_with_prefix", root.id + ":")):
        if (e := (yield from _store("load", i))) is not None:
            found[i] = e
    frontier = [root, *found.values()]
    while frontier:
        for cid in frontier.pop().children:
            if (
                cid != root.id
                and cid not in found
                and (child := (yield from _store("load", cid))) is not None
            ):
                found[cid] = child
                frontier.append(child)
    return _tree_of(root, found)


def purge_flow(execution_id: str, *, archive: Optional[Callable[[dict], Any]] = None) -> Flow:
    """Permanently delete a finished execution tree — the root, every region/invoke
    descendant, and everything the store keys by them (dedupe, trace, timers, pending
    outbox/spawns). Returns False if no such execution exists (already purged).

    Refuses (`PurgeRefused`, a `ValueError`) a child (purge its root), or a tree with any member not
    `DONE`/`CANCELLED` — a `FAILED` dead letter must be abandoned with `terminate()`
    first. `archive`, when given, receives the tree (see `_archive_bundle`) before
    anything is deleted, so a failing archiver aborts the purge; it may see the same
    root again if a purge is retried. It may be a coroutine function where the flow runs
    with coroutines (`aio.control`); in the caller's thread it must be a plain function.

    Descendants go first and the root last, each deleted only if unchanged since it was
    checked: a member that moved on concurrently (e.g. a `Reset` revived it) raises
    `StoreConflict` and stops the purge. Because the root is still there, re-running
    `purge` resumes it and also sweeps the leftovers of children already deleted."""
    root = yield from _store("load", execution_id)
    if root is None:
        return False
    tree, missing = yield from _collect_tree_flow(root)
    _check_purgeable(root, tree)
    if archive is not None:
        traces = {}
        for e in tree:
            traces[e.id] = yield from _store("read_trace", e.id)
        result = archive(_archive_bundle(root, tree, traces))
        if inspect.isawaitable(result):
            yield Await(result)
    for exe in reversed(tree[1:]):
        yield from _purge_one_flow(exe)
    for cid in missing:
        # sweep what an interrupted purge left behind (version unused when absent)
        yield from _store("purge", cid, 0)
    yield from _purge_one_flow(root)
    return True


def _purge_one_flow(exe: Execution) -> Flow:
    if not (yield from _store("purge", exe.id, exe.version)):
        raise StoreConflict(exe.id, expected=exe.version, found=None)


@dataclass
class PurgeReport:
    """What `purge_finished` did: the root ids purged (or, on a dry run, that would be),
    how many finished roots carry no `finished_at` (written before it was recorded) and
    were skipped, and the roots refused, each with the reason."""

    purged: list[str] = field(default_factory=list)
    skipped_undated: int = 0
    refused: dict[str, str] = field(default_factory=dict)


def _purge_statuses(statuses: Iterable[Status]) -> set[Status]:
    """`purge_finished`'s `statuses`, checked to be a subset of the purgeable ones."""
    chosen = set(statuses)
    if not chosen <= set(_PURGEABLE):
        raise ValueError(f"only {', '.join(s.name for s in _PURGEABLE)} executions can be purged")
    return chosen


def _take_candidates(
    summaries: Iterable[ExecutionSummary],
    cutoff: float,
    include_undated: bool,
    report: PurgeReport,
    candidates: list[str],
) -> None:
    """Append to `candidates` the roots of a listing page that finished before `cutoff`
    (shared with the async control plane); count the undated ones it skips in `report`."""
    for summary in summaries:
        if summary.finished_at is None:
            if not include_undated:
                report.skipped_undated += 1
                continue
        elif summary.finished_at >= cutoff:
            continue
        candidates.append(summary.id)


def purge_finished_flow(
    *,
    older_than: float,
    statuses: Iterable[Status] = _PURGEABLE,
    archive: Optional[Callable[[dict], Any]] = None,
    include_undated: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
    now: Optional[float] = None,
) -> Flow:
    """Purge every root tree that finished more than `older_than` seconds ago (see `purge`).

    Candidates are the roots in `statuses` (a subset of DONE/CANCELLED) whose
    `finished_at` is before the cutoff; all are collected before any is deleted, so paging
    isn't disturbed. A root without `finished_at` is skipped unless `include_undated`. A
    candidate `purge` refuses (a member still live, or changed concurrently) is recorded in
    `refused` and the run goes on; an archiver error aborts it. `limit` caps how many roots
    are purged; `dry_run` only reports them, applying the same whole-tree check."""
    statuses = _purge_statuses(statuses)
    cutoff = (time.time() if now is None else now) - older_than
    report = PurgeReport()
    candidates: list[str] = []
    cursor: Optional[str] = None
    while limit is None or len(candidates) < limit:
        page = yield from _store(
            "list_executions", status=statuses, roots_only=True, limit=500, cursor=cursor
        )
        _take_candidates(page.items, cutoff, include_undated, report, candidates)
        cursor = page.next_cursor
        if cursor is None:
            break
    candidates = candidates[:limit]
    if dry_run:
        # the same whole-tree check `purge` makes, without deleting: a root whose tree
        # has a member not yet finished is reported as refused, not as purgeable
        for root_id in candidates:
            root = yield from _store("load", root_id)
            if root is None:
                continue
            try:
                _check_purgeable(root, (yield from _collect_tree_flow(root))[0])
            except PurgeRefused as exc:
                report.refused[root_id] = str(exc)
            else:
                report.purged.append(root_id)
        return report
    for root_id in candidates:
        try:
            if (yield from purge_flow(root_id, archive=archive)):
                report.purged.append(root_id)
        except (PurgeRefused, StoreConflict) as exc:  # anything else, e.g. the archiver, stops the run
            report.refused[root_id] = str(exc)
    return report


# --- over a sync store, in the caller's thread ---------------------------------------------
def terminate(store: ExecutionStore, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`terminate_flow` over `store`."""
    _inline(terminate_flow(execution_id, clock=clock), store)


def cancel(
    store: ExecutionStore,
    defn: Definition,
    execution_id: str,
    *,
    reason: Optional[dict] = None,
    clock: Callable[[], float] = time.time,
) -> None:
    """`cancel_flow` over `store`."""
    _inline(cancel_flow(defn, execution_id, reason=reason, clock=clock), store)


def suspend(store: ExecutionStore, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`suspend_flow` over `store`."""
    _inline(suspend_flow(execution_id, clock=clock), store)


def resume(store: ExecutionStore, execution_id: str, *, clock: Callable[[], float] = time.time) -> None:
    """`resume_flow` over `store`."""
    _inline(resume_flow(execution_id, clock=clock), store)


def redrive(
    store: ExecutionStore,
    defn: Definition,
    execution_id: str,
    target_path: str,
    *,
    clock: Callable[[], float] = time.time,
) -> None:
    """`redrive_flow` over `store`."""
    _inline(redrive_flow(defn, execution_id, target_path, clock=clock), store)


def purge(
    store: ExecutionStore, execution_id: str, *, archive: Optional[Callable[[dict], None]] = None
) -> bool:
    """`purge_flow` over `store`: `archive` must be a plain function."""
    return _inline(purge_flow(execution_id, archive=archive), store)


def purge_finished(
    store: ExecutionStore,
    *,
    older_than: float,
    statuses: Iterable[Status] = _PURGEABLE,
    archive: Optional[Callable[[dict], None]] = None,
    include_undated: bool = False,
    limit: Optional[int] = None,
    dry_run: bool = False,
    now: Optional[float] = None,
) -> PurgeReport:
    """`purge_finished_flow` over `store`."""
    return _inline(
        purge_finished_flow(
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
