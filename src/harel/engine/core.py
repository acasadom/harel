"""The **Engine**: pure, effects-based interpretation of a `Definition`.

`start(...)` and `process(...)` are generators that *describe* side effects
(`RunAction` / `RunSelector` — blocking, the runner sends back an `ActionResult`;
`Emit` — fire-and-forget) and mutate the `Execution`'s position/status. The
engine runs no user code and does no IO; a runner drives the generator.

Hierarchical machines: scope-based transition resolution (innermost-first +
parent fallback, with the automatic-transition asymmetry), LCA-based enter/exit,
history, automatic drain, composite finish/bubble and sinks. Action resolution
is **UML semantics**: each entered/exited level runs its *own* enter/exit hook
(no override-by-depth inheritance from ancestors), and a self/local transition
(target == active leaf) fires nothing. Control events (Reset/SetState/Cancel).
Orthogonal (Phase 3) is model (i): entering an AND-state forks one
child `Execution` per region (`SpawnChildren`); the parent stays on the
orthogonal node and only transitions out once every region reports `Finished`
(the join, gated in `_drain`). The `EventFilter` operator table matches the
legacy one except the legacy `lt`==`eq` bug, which is fixed here (`lt` is real
less-than). Composable filters (all/any/not) and missing-field semantics are
still deferred to the filters phase.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generator, Optional, Union

from harel.definition.model import (
    ActionRef,
    Definition,
    EventFilter,
    Node,
    NodeKind,
    Selector,
    chain,
    lca,
    resolve_relative,
)
from harel.engine.execution import ChildState, Execution, Status
from harel.spec.states import Event

_ORTHOGONAL = (NodeKind.ORTHOGONAL,)


class Hook(Enum):
    ENTER = "on_enter"
    EXIT = "on_exit"
    ACTIVITY = "on_activity"


@dataclass
class RunAction:
    node: Node
    hook: Hook
    action: ActionRef
    event: Optional[Event] = None  # event passed to the action (None on start/automatic, like legacy)


@dataclass
class RunSelector:
    node: Node
    selector: Selector
    event: Optional[Event] = None


@dataclass
class Emit:
    event: Event
    to: Optional[str] = None


@dataclass
class ChildSpec:
    """One region of an orthogonal fork: a child `Execution` over the same
    `Definition` rooted at `root_path` (the branch node), with its own `context`."""

    child_id: str
    root_path: str
    context: dict = field(default_factory=dict)


@dataclass
class SpawnChildren:
    """Deferred effect: the runner creates and starts one child Execution per
    spec, wiring each child's `parent_id`/`child_id` back to this Execution."""

    specs: list


@dataclass
class ScheduleTimer:
    """Deferred effect: arm a durable timer for `path`. The delay is either a literal
    `delay` (seconds) or read from `context_key` at schedule time (so a state's
    `on_enter` can compute a dynamic/backoff value the timer then uses). Emitted on
    entering a state with `timeout`; the runner persists it in the same commit and a
    sweep delivers a `Timeout` event when it is due."""

    path: str
    delay: Optional[float] = None
    context_key: Optional[str] = None


@dataclass
class CancelTimer:
    """Deferred effect: disarm the timer for `path` (emitted on exiting a state
    with `timeout`, so leaving the state before it fires cancels it)."""

    path: str


Effect = Union[RunAction, RunSelector, Emit, SpawnChildren, ScheduleTimer, CancelTimer]


@dataclass
class ActionResult:
    value: Any = None
    emitted: list = field(default_factory=list)


# A step generator yields effects and is sent back an ActionResult (or None).
Step = Generator[Effect, Optional[ActionResult], None]

_OP = {
    "lt": operator.lt,  # legacy had this mapped to operator.eq (a bug); fixed here
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
    "ge": operator.ge,
    "gt": operator.gt,
    "in": lambda a, b: operator.contains(b, a),
}


def _eval(pred, data: dict) -> bool:
    """Evaluate a composable predicate tree against the event data. A leaf on a
    field absent from the event fails (it cannot be evaluated)."""
    if pred.node == "leaf":
        return pred.field in data and _OP[pred.op](data[pred.field], pred.value)
    if pred.node == "all":
        return all(_eval(c, data) for c in pred.children)
    if pred.node == "any":
        return any(_eval(c, data) for c in pred.children)
    if pred.node == "not":
        return not _eval(pred.children[0], data)
    raise ValueError(f"unknown predicate node {pred.node!r}")


def _matches(ef: EventFilter, event: Event) -> bool:
    if event.kind not in [k.strip() for k in ef.kind.split("|")]:
        return False
    for key, value in ef.predicates.items():
        name, op = key.split("__") if "__" in key else (key, "eq")
        # a predicate on a field absent from the event fails (cannot be evaluated)
        if name not in event.data or not _OP[op](event.data[name], value):
            return False
    if ef.predicate is not None and not _eval(ef.predicate, event.data):
        return False
    return True


def _hook(node: Node, hook: Hook) -> Optional[ActionRef]:
    return {Hook.ENTER: node.on_enter, Hook.EXIT: node.on_exit, Hook.ACTIVITY: node.on_activity}[hook]


def _run(node: Node, hook: Hook, event: Optional[Event]) -> Step:
    """Run this node's own hook if it defines one (used for the entry cascade —
    initial start and composite descent — where each level runs its own action).
    A node with `timeout` arms its timer on enter and disarms it on exit (so every
    enter/exit path — descend, take, sink — schedules/cancels symmetrically)."""
    action = _hook(node, hook)
    if action is not None:
        yield RunAction(node, hook, action, event)
    if node.timeout is not None:
        if hook is Hook.ENTER:
            # `timeout` is either a literal (seconds) or {context: key} → the delay
            # is read from the Execution context when the runner schedules it.
            if isinstance(node.timeout, dict):
                yield ScheduleTimer(node.full_path, context_key=node.timeout.get("context"))
            else:
                yield ScheduleTimer(node.full_path, delay=float(node.timeout))
        elif hook is Hook.EXIT:
            yield CancelTimer(node.full_path)


# --- transition resolution by scope (faithful: innermost-first + fallback) ----


def _nested(scope: Node, rel: list[Node], predicate, allow_parent: bool):
    """Resolve a transition for `rel` (the chain of nodes from a child of `scope`
    down to the active node). Innermost composite wins; outer scopes are a
    fallback only when `allow_parent` (event transitions); automatic lookups pass
    `allow_parent=False`, reproducing the legacy asymmetry. Returns (scope, t)."""
    head = rel[0]
    if len(rel) > 1:
        deeper = _nested(head, rel[1:], predicate, True)
        if deeper is not None:
            return deeper
        if not allow_parent:
            return None
    for t in scope.transitions:
        if t.source is head and predicate(t):
            return (scope, t)
    return None


def _resolve(defn: Definition, exe: Execution, predicate, allow_parent: bool):
    root = defn.index[exe.root_path]
    assert exe.active_path is not None
    active = defn.index[exe.active_path]
    if active is root:
        return None
    rel = chain(root, active)[1:]  # [top child of root, ..., active]
    return _nested(root, rel, predicate, allow_parent)


def _event_pred(event: Event):
    return lambda t: t.event_filter is not None and _matches(t.event_filter, event)


def _resolve_at(defn: Definition, path: str, predicate):
    """Resolve a transition for a `Timeout` at `path`: anchored at the node that
    timed out and **bubbling up** through its ancestors (the source is that node or
    an ancestor — never an inner state). Walking from the timed-out node upward
    keeps a composite's budget timeout from being shadowed by an inner state's
    `Timeout` transition, while still letting an inner state's timeout be handled by
    an enclosing `on Timeout` (the parent scope owns the cleanup). Returns the
    innermost match (scope, t) up the chain, or None."""
    cur = defn.index.get(path)
    while cur is not None and cur.parent is not None:
        scope = cur.parent
        for t in scope.transitions:
            if t.source is cur and predicate(t):
                return (scope, t)
        cur = cur.parent
    return None


def _auto_pred(t) -> bool:
    return t.event_filter is None


def _any_pred(t) -> bool:
    return True


def _kind_pred(kind: str):
    """A transition predicate matching any event_filter whose `kind` alternation
    includes `kind`, ignoring data predicates (a structural check on the model)."""

    def pred(t) -> bool:
        ef = t.event_filter
        return ef is not None and kind in [k.strip() for k in ef.kind.split("|")]

    return pred


def timeout_event(execution_id: str, path: str, fire_at: float) -> Event:
    """The `Timeout` event a due timer delivers. Its id is **stable** (derived from
    the timer key + fire time) so a timer swept by two workers dedupes to one
    effect; `data.path` is the timed state (the staleness guard in `process`)."""
    return Event(kind="Timeout", id=f"timeout:{execution_id}:{path}:{fire_at}", data={"path": path})


def _is_active(defn: Definition, exe: Execution, path: str) -> bool:
    """Whether `path` is in the active configuration (the active leaf or one of its
    ancestors up to the root). Used as the timer staleness guard."""
    if exe.active_path is None or path not in defn.index:
        return False
    root = defn.index[exe.root_path]
    active = defn.index[exe.active_path]
    return any(n.full_path == path for n in chain(root, active))


def has_cancel_handler(defn: Definition, exe: Execution) -> bool:
    """Whether the active configuration has its own `Cancel` transition (in scope,
    with parent fallback). The control plane uses this to choose cooperative
    cancel (let the machine clean up) over forceful terminate."""
    if exe.active_path is None:
        return False
    return _resolve(defn, exe, _kind_pred("Cancel"), allow_parent=True) is not None


# --- LCA-based enter/exit (UML semantics: own hook per entered/exited level) ---


def _joined(exe: Execution) -> bool:
    """True once every spawned region has reported `Finished` (the join)."""
    return all(child.finished for child in exe.children.values())


def _region_key(exe: Execution, child_id: str) -> str:
    """A stable, friendly key for a region in `region_results`: the child_id with
    the parent's `id:` prefix stripped (`Fork.A` for a static region, `Process:0`
    for a fan-out instance)."""
    prefix = f"{exe.id}:"
    return child_id[len(prefix) :] if child_id.startswith(prefix) else child_id


def _expose_region_results(exe: Execution) -> None:
    """At a join, surface each child's result (terminal outcome + carried context)
    keyed by region/instance — only when a child reported something, so a plain
    join leaves the parent's context untouched."""
    results = {
        (cs.key or _region_key(exe, cid)): {"outcome": cs.outcome, **cs.result}
        for cid, cs in exe.children.items()
    }
    if any(r["outcome"] is not None or len(r) > 1 for r in results.values()):
        exe.context["region_results"] = results


def _fan_out(exe: Execution, node: Node) -> Step:
    """Fan-out invoke: fork ONE addressed child per entry of the `invoke_each`
    collection, each running the target Definition (FQN rides in its context) with
    its own slice; the parent joins on completion. The actor/data-parallel sibling
    of the orthogonal fork — instances are addressed (no domain broadcast)."""
    loop_var, coll_key = node.invoke_each  # type: ignore[misc]
    seq = exe.invoke_seq.get(node.full_path, 0)  # per-entry seq: a re-entry spawns fresh child ids
    owned = f"{exe.id}:{node.full_path}:"
    for cid in [c for c in exe.children if c.startswith(owned)]:
        exe.children.pop(cid, None)  # a re-entry: drop the previous entry's (finished) instances
    specs: list[ChildSpec] = []
    for i, item in enumerate(exe.context.get(coll_key, [])):
        cid = f"{exe.id}:{node.full_path}:{seq}:{i}"
        child_ctx = {
            ck: (item if src == loop_var else exe.context.get(src)) for ck, src in node.invoke_with.items()
        }
        child_ctx["__invoke_fqn__"] = node.invoke
        exe.children[cid] = ChildState(root_path="", submachine=True, key=f"{node.full_path}:{i}")
        specs.append(ChildSpec(child_id=cid, root_path="", context=child_ctx))
    yield SpawnChildren(specs)


def _fork(exe: Execution, node: Node) -> Step:
    """Enter an orthogonal (AND) node: instead of descending, spawn one child
    Execution per region and stay positioned on the orthogonal node until they all
    finish (`_joined`). Regions share the parent's Definition and SEE the parent's
    domain events (broadcast — UML semantics). Data-parallel fan-out (N independent,
    addressed workers) is a `fan-out invoke`, not this."""
    seq = exe.invoke_seq.get(node.full_path, 0)  # per-entry seq: a re-entry spawns fresh child ids
    exe.children = {}
    specs: list[ChildSpec] = []
    for child in node.children:
        cid = f"{exe.id}:{child.full_path}:{seq}"
        specs.append(ChildSpec(child_id=cid, root_path=child.full_path))
        exe.children[cid] = ChildState(root_path=child.full_path, key=child.full_path)
    yield SpawnChildren(specs)


def _leave_regions(exe: Execution, node: Node) -> None:
    """Leaving an orthogonal / fan-out node: bump its per-entry counter so a later
    re-entry spawns FRESH child Executions (distinct ids) instead of colliding with
    the already-completed ones from the previous entry — the relay's create-is-
    idempotent skip would otherwise never re-run them and the join would deadlock.
    Mirrors the single `invoke` path, which bumps `invoke_seq` on completion. We do
    NOT drop the finished ChildStates here: they persist for post-join inspection
    (`region_results` / outcomes); a re-entry replaces them (`_fork` wipes the dict,
    `_fan_out` drops this node's stale entries)."""
    exe.invoke_seq[node.full_path] = exe.invoke_seq.get(node.full_path, 0) + 1


def _descend(defn: Definition, exe: Execution, node: Node, event: Optional[Event]) -> Step:
    """Enter into a composite's start (or history) child, down to a leaf."""
    while node.is_composite:
        if node.kind in _ORTHOGONAL:
            yield from _fork(exe, node)
            return
        if not node.allow_history:
            exe.history.pop(node.full_path, None)
        child_path = exe.history.get(node.full_path)
        if child_path is None and node.start_state is not None:
            child = node.child(node.start_state)
            child_path = child.full_path if child is not None else None
        if child_path is None:
            return
        child = defn.index[child_path]
        yield from _run(child, Hook.ENTER, event)
        exe.active_path = child_path
        exe.history[node.full_path] = child_path
        node = child


def _target_of(
    defn: Definition, exe: Execution, scope: Node, t, event
) -> Generator[Effect, Optional[ActionResult], Node]:
    if t.selector is not None:
        assert exe.active_path is not None
        result = yield RunSelector(defn.index[exe.active_path], t.selector, event)
        assert result is not None
        target_name = t.selector.mapper.get(str(result.value), t.selector.default)
        assert target_name is not None, f"selector result {result.value!r} matched no branch and no `else`"
        target = resolve_relative(scope, target_name)
        assert target is not None
        return target
    assert t.target is not None
    return t.target


def _take(defn: Definition, exe: Execution, target: Node, event: Optional[Event]) -> Step:
    """Take a transition with UML LCA semantics: run the **own** `on_exit` of each
    level from the active leaf up to lca(leaf, target) (innermost-first), then the
    **own** `on_enter` of each level from there down to the target (outermost-
    first), then descend the target into its initial/history child. No hook is
    inherited from an ancestor. A self/local transition (target == active leaf)
    has an empty lca chain and therefore fires nothing."""
    assert exe.active_path is not None
    source = defn.index[exe.active_path]
    pivot = lca(source, target)
    for node in reversed(chain(pivot, source)[1:]):  # exited levels, innermost-first
        yield from _run(node, Hook.EXIT, event)
        if node.parent is not None:
            exe.history[node.parent.full_path] = node.full_path
        if node.kind in _ORTHOGONAL or node.invoke_each is not None:
            _leave_regions(exe, node)  # bump invoke_seq so re-entry spawns fresh child ids
    exe.active_path = pivot.full_path
    for node in chain(pivot, target)[1:]:  # entered levels, outermost-first
        yield from _run(node, Hook.ENTER, event)
        exe.active_path = node.full_path
        if node.parent is not None:
            exe.history[node.parent.full_path] = node.full_path
    yield from _descend(defn, exe, target, event)


def _drain(defn: Definition, exe: Execution) -> Step:
    """Follow automatic transitions; bubble finished composites up; settle/sink."""
    root = defn.index[exe.root_path]
    while True:
        assert exe.active_path is not None
        active = defn.index[exe.active_path]
        if active.invoke is not None and active.invoke_each is None:
            # single submachine invoke: on first reach fork ONE addressed child
            # running another Definition (FQN rides in its context for the runner to
            # resolve), then park — the parent waits for the child's `Finished`,
            # delivered to the model as a `Returned` completion event.
            seq = exe.invoke_seq.get(active.full_path, 0)
            cid = f"{exe.id}:{active.full_path}:{seq}"  # per-entry id: re-invoke spawns afresh
            if cid not in exe.children:
                child_ctx = {
                    ck: exe.context[pk] for ck, pk in active.invoke_with.items() if pk in exe.context
                }
                child_ctx["__invoke_fqn__"] = active.invoke
                exe.children[cid] = ChildState(root_path="", submachine=True)
                yield SpawnChildren([ChildSpec(child_id=cid, root_path="", context=child_ctx)])
            return
        if active.invoke is not None:
            # fan-out invoke: spawn N addressed children (once per entry), then join
            # like an AND-state — but the instances run a target Definition and are
            # addressed. The guard is scoped to THIS entry's seq so a re-entry (seq
            # bumped on leave) fans out afresh instead of seeing the prior instances.
            seq = exe.invoke_seq.get(active.full_path, 0)
            if not any(c.startswith(f"{exe.id}:{active.full_path}:{seq}:") for c in exe.children):
                yield from _fan_out(exe, active)
            if not _joined(exe):
                return
            _expose_region_results(exe)
        elif active.kind in _ORTHOGONAL:
            if not _joined(exe):
                return  # AND-state: wait for every region to report Finished
            _expose_region_results(exe)  # surface region results for the join transition

        auto = _resolve(defn, exe, _auto_pred, allow_parent=False)
        if auto is not None:
            scope, t = auto
            target = yield from _target_of(defn, exe, scope, t, None)
            yield from _take(defn, exe, target, None)
            continue

        if _resolve(defn, exe, _any_pred, allow_parent=False) is not None:
            return  # has a transition at its own scope (waiting for an event)

        # sink at its immediate scope: run on_exit, then bubble up or finish. The
        # innermost terminal that declares an `outcome` sets the Execution's result
        # (the model's success/failure label); plain sinks leave it None.
        if active.outcome is not None and exe.outcome is None:
            exe.outcome = active.outcome
        yield from _run(active, Hook.EXIT, None)
        if active.parent is not None and active.parent is not root:
            exe.history.pop(active.parent.full_path, None)
            exe.active_path = active.parent.full_path
            continue
        if exe.status is Status.RUNNING:
            # the outcome is whatever terminal the model reached (set by the sink
            # rule above); the engine does NOT guess an aggregate. A parent routes
            # out of an orthogonal join with a selector over `region_results` to its
            # own outcome-bearing terminal — policy stays in the model.
            exe.status = Status.DONE
            if exe.parent_id is not None:
                # a region reached its global sink: report the join to the parent,
                # carrying its outcome + the `carry`-projected context (the region's
                # "return value"). `Finished` is a system event with an opaque payload.
                carried = {k: exe.context[k] for k in root.carry if k in exe.context}
                yield Emit(
                    Event(
                        kind="Finished",
                        data={"child_id": exe.child_id, "outcome": exe.outcome, **carried},
                    ),
                    to=exe.parent_id,
                )
        return


def start(defn: Definition, exe: Execution) -> Step:
    exe.status = Status.RUNNING
    root = defn.index[exe.root_path]
    assert root.start_state is not None
    start_node = root.child(root.start_state)
    assert start_node is not None
    exe.active_path = start_node.full_path
    yield from _run(start_node, Hook.ENTER, None)
    yield from _descend(defn, exe, start_node, None)
    yield from _drain(defn, exe)


def set_state(defn: Definition, exe: Execution, path: str) -> Step:
    """Restore a position by address (SetState): does not run the state's enter,
    just positions and drains automatic transitions."""
    exe.active_path = path
    exe.status = Status.RUNNING
    yield from _drain(defn, exe)


def process(defn: Definition, exe: Execution, event: Event) -> Step:
    if event.kind == "Reset":
        exe.context.clear()
        exe.history.clear()
        exe.active_path = None
        exe.outcome = None
        exe.error = None
        exe.status = Status.PENDING
        yield from start(defn, exe)
        return
    if event.kind == "Cancel":
        # Cancel is modelable: if the active configuration has its own Cancel
        # transition (a critical section that owns its cleanup), take it like a
        # normal event; otherwise fall back to the forceful terminate (no hooks).
        # CANCELLING is the cooperative-cancel-in-flight status set by the control
        # plane: the injected Cancel resumes the machine (RUNNING) to run cleanup.
        active = exe.status in (Status.RUNNING, Status.CANCELLING) and exe.active_path is not None
        found = _resolve(defn, exe, _event_pred(event), allow_parent=True) if active else None
        if found is None:
            exe.status = Status.CANCELLED
            return
        exe.status = Status.RUNNING
        scope, t = found
        dest = yield from _target_of(defn, exe, scope, t, event)
        yield from _take(defn, exe, dest, event)
        yield from _drain(defn, exe)
        exe.processed_events += 1
        return
    if event.kind == "Start":
        if exe.status is not Status.CANCELLED:
            # a Start may carry initial parameters (the caller's opaque payload):
            # seed the context before the machine runs.
            if event.data:
                exe.context.update(event.data)
            yield from start(defn, exe)
        return
    if event.kind == "SetState":
        target = event.data.get("current_state")
        if target is not None:
            yield from set_state(defn, exe, target)
        return
    if event.kind == "Finished":
        # a child reported completion; record its outcome + carried result
        cid = event.data.get("child_id")
        if cid in exe.children:
            exe.children[cid].finished = True
            exe.children[cid].outcome = event.data.get("outcome")
            # everything past the reserved keys is the child's carried context
            exe.children[cid].result = {
                k: v for k, v in event.data.items() if k not in ("child_id", "outcome")
            }
        active_node = defn.index[exe.active_path] if exe.active_path is not None else None
        if (
            active_node is not None
            and active_node.invoke is not None
            and active_node.invoke_each is None
            and exe.status is Status.RUNNING
        ):
            # a SINGLE submachine state is active: handle ITS child's completion (a Finished
            # from a stale earlier-seq child is a no-op). Deliver the child's outcome +
            # result to the model as a `Returned` event and fire `on Returned where ...`.
            inv_path = active_node.full_path
            seq = exe.invoke_seq.get(inv_path, 0)
            if cid == f"{exe.id}:{inv_path}:{seq}":
                cs = exe.children[cid]
                completion = Event(kind="Returned", data={"outcome": cs.outcome, **cs.result})
                found = _resolve(defn, exe, _event_pred(completion), allow_parent=True)
                if found is not None:
                    # leaving the invoke-state: bump its entry counter and drop the
                    # completed child so a later re-entry spawns a fresh submachine
                    exe.invoke_seq[inv_path] = seq + 1
                    exe.children.pop(cid, None)
                    scope, t = found
                    dest = yield from _target_of(defn, exe, scope, t, completion)
                    yield from _take(defn, exe, dest, completion)
                    yield from _drain(defn, exe)
            return
        # otherwise this is an orthogonal region's join: re-drain (the AND-state's
        # gate opens once every region is finished)
        if exe.status is Status.RUNNING:
            yield from _drain(defn, exe)
        return
    if event.kind == "Timeout":
        # a durable timer fired for `path`. Staleness guard: only act if that
        # state is STILL active (its timer was not cancelled by an exit, and the
        # state was not left+re-entered). Then resolve the Timeout like a normal
        # event (the model owns the reaction); unhandled => no-op.
        path = event.data.get("path")
        if exe.status is not Status.RUNNING or path is None or not _is_active(defn, exe, path):
            return
        # fire the transition of the state that timed out (by path), not the
        # innermost Timeout transition — so a composite's budget isn't shadowed.
        found = _resolve_at(defn, path, _event_pred(event))
        if found is not None:
            scope, t = found
            dest = yield from _target_of(defn, exe, scope, t, event)
            yield from _take(defn, exe, dest, event)
            yield from _drain(defn, exe)
            exe.processed_events += 1
        return

    # domain event: the legacy engine ignores events while not running
    if exe.status is not Status.RUNNING:
        return

    found = _resolve(defn, exe, _event_pred(event), allow_parent=True)
    if found is not None:
        scope, t = found
        target = yield from _target_of(defn, exe, scope, t, event)
        yield from _take(defn, exe, target, event)
    else:
        # no transition: run the active leaf's own activity (no inheritance)
        assert exe.active_path is not None
        yield from _run(defn.index[exe.active_path], Hook.ACTIVITY, event)
    yield from _drain(defn, exe)
    exe.processed_events += 1
