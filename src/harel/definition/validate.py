"""Static validation of a `Definition` — correctness checks you run *before*
executing, independent of any authoring surface (YAML, the future DSL, objects).

A pure pass over the immutable graph. It catches the structural defects the
builder does not: unresolved selector targets, missing composite initials,
non-deterministic automatic transitions, unreachable states, and references to
**undeclared events** (every event a transition fires on must be declared — or be a
reserved engine event) plus unknown event fields.

Action *bugs* are out of scope (those surface at run time); this is about the
shape of the machine being well-formed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from harel.definition.events import RESERVED_EVENTS
from harel.definition.model import (
    Definition,
    EventFilter,
    Node,
    NodeKind,
    Predicate,
    Transition,
    resolve_relative,
)

# Ops that only make sense on an ordered (numeric) field.
_NUMERIC_OPS = {"lt", "le", "gt", "ge"}
_ORTHOGONAL = {NodeKind.ORTHOGONAL}


@dataclass(frozen=True)
class Issue:
    """One validation finding. `severity` is "error" (blocks) or "warning"."""

    code: str
    severity: str
    path: str  # the node's full_path the issue is about ("" = root)
    message: str

    def __str__(self) -> str:
        where = self.path or "<root>"
        return f"[{self.severity}] {self.code} at {where}: {self.message}"


class ValidationError(Exception):
    """Raised by `validate_or_raise` when the Definition has error-level issues."""

    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
        errors = [i for i in issues if i.severity == "error"]
        super().__init__("invalid Definition:\n" + "\n".join(f"  {i}" for i in errors))


# --- predicate helpers --------------------------------------------------------


def _flat_fields(predicates: dict) -> set[str]:
    """Field names referenced by the flat `field__op -> value` predicates."""
    return {key.split("__")[0] for key in predicates}


def _flat_leaves(predicates: dict) -> list[tuple[str, str]]:
    """(field, op) pairs from the flat predicates (op defaults to eq)."""
    out = []
    for key in predicates:
        name, op = key.split("__") if "__" in key else (key, "eq")
        out.append((name, op))
    return out


def _tree_leaves(pred: Optional[Predicate]) -> list[Predicate]:
    if pred is None:
        return []
    if pred.node == "leaf":
        return [pred]
    return [leaf for child in pred.children for leaf in _tree_leaves(child)]


# --- the checks ---------------------------------------------------------------


def _check_selectors(node: Node, issues: list[Issue]) -> None:
    """Selector well-formedness + mapper targets resolve (the builder never
    resolves them, so this is the only place they are checked)."""
    for t in node.transitions:
        sel = t.selector
        if sel is None:
            continue
        if sel.action is None or not sel.mapper:
            issues.append(
                Issue(
                    "selector_malformed",
                    "error",
                    node.full_path,
                    "selector needs an action and a non-empty mapper",
                )
            )
            continue
        targets = list(sel.mapper.items())
        if sel.default is not None:
            targets.append(("else", sel.default))
        for result, target_name in targets:
            if resolve_relative(node, target_name) is None:
                issues.append(
                    Issue(
                        "selector_target_unresolved",
                        "error",
                        node.full_path,
                        f"selector branch {result!r} -> {target_name!r} does not resolve from this scope",
                    )
                )
        if sel.enum is not None:
            phantom = [k for k in sel.mapper if k not in sel.enum]
            if phantom:
                issues.append(
                    Issue(
                        "selector_phantom_branch",
                        "error",
                        node.full_path,
                        f"selector branches {phantom} are not in the declared result set {sel.enum}",
                    )
                )
            uncovered = [v for v in sel.enum if v not in sel.mapper]
            if uncovered and sel.default is None:
                issues.append(
                    Issue(
                        "selector_non_exhaustive",
                        "error",
                        node.full_path,
                        f"selector does not cover {uncovered} and has no `else`",
                    )
                )


def _check_initial(node: Node, issues: list[Issue]) -> None:
    """Every composite (not orthogonal) with children declares an initial that
    resolves to one of its children."""
    if not node.children or node.kind in _ORTHOGONAL:
        return
    if node.start_state is None:
        issues.append(
            Issue("missing_initial", "error", node.full_path, "composite has children but no initial state")
        )
    elif node.child(node.start_state) is None:
        issues.append(
            Issue(
                "initial_unresolved",
                "error",
                node.full_path,
                f"initial state {node.start_state!r} is not a child of this composite",
            )
        )


def _check_nondeterminism(defn: Definition, issues: list[Issue]) -> None:
    """A source with more than one automatic (eventless) transition fires
    ambiguously on drain."""
    by_source: dict[int, tuple[Node, int]] = {}
    for node in defn.index.values():
        for t in node.transitions:
            if t.event_filter is None:  # automatic: plain `to:` or a selector with no event
                src = t.source
                _, count = by_source.get(id(src), (src, 0))
                by_source[id(src)] = (src, count + 1)
    for src, count in by_source.values():
        if count > 1:
            issues.append(
                Issue(
                    "nondeterministic_automatic",
                    "error",
                    src.full_path,
                    f"{count} automatic (eventless) transitions leave this state; the drain is ambiguous",
                )
            )


def _reachable(defn: Definition) -> set[int]:
    """Node ids reachable from the root by initial-descent + transition/selector
    targets, to a fixpoint."""
    seen: set[int] = set()
    work: list[Node] = []

    def visit(n: Node) -> None:
        if id(n) not in seen:
            seen.add(id(n))
            work.append(n)

    visit(defn.root)
    while work:
        node = work.pop()
        # entering a composite activates its initial child; orthogonal activates every region
        if node.children:
            if node.kind in _ORTHOGONAL:
                for c in node.children:
                    visit(c)
            elif node.start_state and node.child(node.start_state) is not None:
                visit(node.child(node.start_state))  # type: ignore[arg-type]
        for t in node.transitions:
            if t.target is not None:
                visit(t.target)
            if t.selector is not None:
                # the `else` branch is a reachable target too (e.g. the `join ...
                # else to X` sugar routes X only through the default)
                branches = list(t.selector.mapper.values())
                if t.selector.default is not None:
                    branches.append(t.selector.default)
                for target_name in branches:
                    tgt = resolve_relative(node, target_name)
                    if tgt is not None:
                        visit(tgt)
    return seen


def _check_reachability(defn: Definition, issues: list[Issue]) -> None:
    reachable = _reachable(defn)
    for node in defn.index.values():
        if id(node) not in reachable:
            issues.append(
                Issue("unreachable", "warning", node.full_path, "state is not reachable from the root")
            )


def _check_event(node: Node, ef: EventFilter, defn: Definition, issues: list[Issue]) -> None:
    # Every referenced event must be declared (or be a RESERVED_EVENT): an undeclared
    # event is an error, so a typo can't slip through. (Reserved engine events and
    # automatic — eventless — transitions are exempt; the latter have no EventFilter.)
    fields = _flat_fields(ef.predicates) | {leaf.field for leaf in _tree_leaves(ef.predicate) if leaf.field}
    leaves = _flat_leaves(ef.predicates) + [
        (leaf.field, leaf.op) for leaf in _tree_leaves(ef.predicate) if leaf.field
    ]
    for kind in (k.strip() for k in ef.kind.split("|")):
        if kind in RESERVED_EVENTS:
            continue
        etype = defn.events.get(kind)
        if etype is None:
            issues.append(
                Issue(
                    "unknown_event",
                    "error",
                    node.full_path,
                    f"transition references undeclared event {kind!r}",
                )
            )
            continue
        if not etype.fields:  # declared but schemaless => no field checks
            continue
        for f in fields:
            if f not in etype.fields:
                issues.append(
                    Issue(
                        "unknown_event_field", "error", node.full_path, f"event {kind!r} has no field {f!r}"
                    )
                )
        for f, op in leaves:
            spec = etype.fields.get(f)
            if spec and op in _NUMERIC_OPS and spec.type in ("string", "bool"):
                issues.append(
                    Issue(
                        "op_type_mismatch",
                        "warning",
                        node.full_path,
                        f"op {op!r} on {f!r} ({spec.type}) compares a non-ordered field",
                    )
                )


def _check_events(defn: Definition, issues: list[Issue]) -> None:
    for node in defn.index.values():
        for t in node.transitions:
            if t.event_filter is not None:
                _check_event(node, t.event_filter, defn, issues)


def _has_timeout_handler(node: Node) -> bool:
    """Whether a `Timeout` for `node` would be handled: a Timeout transition whose
    source is `node` or — since a Timeout bubbles up — any of its ancestors (the
    engine's `_resolve_at` walks the same node→root chain). Guards are ignored here
    (a structural check; whether a `where` matches at run time is the model's call)."""
    cur: Optional[Node] = node
    while cur is not None and cur.parent is not None:
        if any(
            t.source is cur
            and t.event_filter is not None
            and "Timeout" in [k.strip() for k in t.event_filter.kind.split("|")]
            for t in cur.parent.transitions
        ):
            return True
        cur = cur.parent
    return False


def _ancestor_scopes(node: Node) -> list[Node]:
    out, cur = [], node.parent
    while cur is not None:
        out.append(cur)
        cur = cur.parent
    return out


def _check_timeout(node: Node, issues: list[Issue]) -> None:
    if node.timeout is None:
        return
    t = node.timeout
    if isinstance(t, dict):
        ctx = t.get("context")
        if list(t) != ["context"] or not isinstance(ctx, str) or not ctx:
            issues.append(
                Issue(
                    "timeout_malformed", "error", node.full_path, "dynamic timeout must be {context: <key>}"
                )
            )
    elif isinstance(t, bool) or not isinstance(t, int) or t <= 0:
        issues.append(
            Issue("timeout_invalid", "error", node.full_path, f"timeout must be a positive int, got {t!r}")
        )
    if not _has_timeout_handler(node):
        issues.append(
            Issue(
                "timeout_unhandled",
                "warning",
                node.full_path,
                "timeout is armed but no `on: Timeout` transition handles it (guaranteed no-op on fire)",
            )
        )


def _check_outcome(node: Node, issues: list[Issue]) -> None:
    if node.outcome is None:
        return
    if node.children:
        issues.append(
            Issue("outcome_on_composite", "warning", node.full_path, "outcome on a non-terminal composite")
        )
    elif any(t.source is node for t in _all_transitions_from(node)):
        issues.append(
            Issue(
                "outcome_on_nonterminal",
                "warning",
                node.full_path,
                "outcome on a state with outgoing transitions",
            )
        )


def _check_invoke(node: Node, issues: list[Issue]) -> None:
    """An `invoke` state is a black-box leaf. A SINGLE invoke parks until the
    submachine returns and routes on a `Returned` completion, so it must not have an
    automatic outgoing transition (which would fire before the return). A FAN-OUT
    invoke (`for V in COLL`) joins on completion and DOES route automatically
    (`join all/any`), so that check does not apply to it."""
    if node.invoke is None:
        return
    if node.children:
        issues.append(
            Issue("invoke_on_composite", "error", node.full_path, "an `invoke` state must be a leaf")
        )
    if node.invoke_each is None and any(
        t.source is node and t.event_filter is None for t in _all_transitions_from(node)
    ):
        issues.append(
            Issue(
                "invoke_automatic_exit",
                "error",
                node.full_path,
                "a single `invoke` state must not have an automatic outgoing transition "
                "(it would fire before the submachine returns); use `on Returned`",
            )
        )


def _all_transitions_from(node: Node) -> list[Transition]:
    """Transitions whose source is `node`, gathered across this node and its
    ancestor scopes (a composite owns transitions for its descendants)."""
    out = list(node.transitions)
    for anc in _ancestor_scopes(node):
        out.extend(t for t in anc.transitions if t.source is node)
    return out


def _is_terminal(node: Node) -> bool:
    """A terminal (sink): a leaf with no outgoing transition. Reaching it ends the
    enclosing Execution (it bubbles up to the root, or — for a region — reports the
    join). An `invoke` state is never a terminal (it parks for the submachine)."""
    if node.children or node.invoke is not None:
        return False
    return not any(t.source is node for t in _all_transitions_from(node))


def _execution_roots(defn: Definition) -> list[Node]:
    """The subtrees that each run as their own `Execution`: the machine root and
    every orthogonal region (a child of an `Orthogonal` node).
    Each one ends with an outcome the surrounding model routes on (the join, or the
    execution's external result)."""
    roots = [defn.root]
    for node in defn.index.values():
        if node.kind in _ORTHOGONAL:
            roots.extend(node.children)
    return roots


def _execution_terminals(root: Node) -> list[Node]:
    """Leaf sinks in `root`'s subtree that actually END `root`'s Execution: the
    bubble from the leaf reaches `root` UNCAUGHT — no ancestor in between has an
    outgoing transition (which would keep the Execution running: an automatic
    transition fires, an event transition waits). Does NOT descend into a nested
    orthogonal's regions (those are their own execution roots, validated apart)."""
    out: list[Node] = []

    def ends_execution(leaf: Node) -> bool:
        anc = leaf.parent
        while anc is not None and anc is not root:
            if any(t.source is anc for t in _all_transitions_from(anc)):
                return False  # this composite catches the bubble; the Execution goes on
            anc = anc.parent
        return True

    def walk(node: Node) -> None:
        if not node.children:
            if _is_terminal(node) and ends_execution(node):
                out.append(node)
            return
        if node.kind in _ORTHOGONAL:
            return  # the regions below are separate execution roots
        for child in node.children:
            walk(child)

    walk(root)
    return out


def _check_terminal_outcomes(defn: Definition, issues: list[Issue]) -> None:
    """Every terminal that ends an Execution — the machine root's and each
    orthogonal region's — must declare an `outcome` (the success/failed verdict the
    surrounding model routes on). Terminals inside a plain composite are included
    (they end the Execution by bubbling up); composites themselves and non-terminal
    states are exempt (they keep `outcome=None`)."""
    seen: set[int] = set()
    for root in _execution_roots(defn):
        for term in _execution_terminals(root):
            if id(term) in seen:
                continue
            seen.add(id(term))
            if term.outcome is None:
                issues.append(
                    Issue(
                        "terminal_missing_outcome",
                        "error",
                        term.full_path,
                        "terminal of the machine/region must declare an `outcome` "
                        "(e.g. success / failed) — the verdict the model routes on",
                    )
                )


# --- entry points -------------------------------------------------------------


def validate(defn: Definition) -> list[Issue]:
    """Return all validation issues (errors and warnings); empty == well-formed."""
    issues: list[Issue] = []
    for node in defn.index.values():
        _check_selectors(node, issues)
        _check_initial(node, issues)
        _check_timeout(node, issues)
        _check_outcome(node, issues)
        _check_invoke(node, issues)
    _check_nondeterminism(defn, issues)
    _check_reachability(defn, issues)
    _check_events(defn, issues)
    _check_terminal_outcomes(defn, issues)
    return issues


def validate_or_raise(defn: Definition) -> list[Issue]:
    """Validate and raise `ValidationError` on any error-level issue. Returns the
    full issue list (so warnings are still visible) when it does not raise."""
    issues = validate(defn)
    if any(i.severity == "error" for i in issues):
        raise ValidationError(issues)
    return issues
