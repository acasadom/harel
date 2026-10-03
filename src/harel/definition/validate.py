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
from typing import Any, Optional

from harel.definition.events import RESERVED_EVENTS
from harel.definition.model import (
    Definition,
    EventFilter,
    Expr,
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


def _operands(leaf: Predicate) -> list[tuple[str, str]]:
    """The (namespace, field) pairs a leaf reads: its left side and, when it compares with
    a reference instead of a literal, its right side."""
    out = [(leaf.source, leaf.field)] if leaf.field else []
    if leaf.value_ref is not None and leaf.value_ref.field:
        out.append((leaf.value_ref.source or "event", leaf.value_ref.field))
    return out


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
            for tgt in _choice_targets(t):
                visit(tgt)
    return seen


def _check_reachability(defn: Definition, issues: list[Issue]) -> None:
    reachable = _reachable(defn)
    for node in defn.index.values():
        if id(node) not in reachable:
            issues.append(
                Issue("unreachable", "warning", node.full_path, "state is not reachable from the root")
            )


def _check_event(
    node: Node, ef: EventFilter, defn: Definition, issues: list[Issue], extra: tuple = ()
) -> None:
    # Every referenced event must be declared (or be a RESERVED_EVENT): an undeclared
    # event is an error, so a typo can't slip through. (Reserved engine events and
    # automatic — eventless — transitions are exempt; the latter have no EventFilter.)
    # the event's own fields only; `context.x` leaves are checked against the context schema
    event_reads = [
        (f, leaf.op or "eq")
        for leaf in _tree_leaves(ef.predicate)
        for src, f in _operands(leaf)
        if src == "event"
    ]
    fields = _flat_fields(ef.predicates) | {f for f, _ in event_reads}
    leaves = _flat_leaves(ef.predicates) + event_reads
    for field, op in extra:  # `choose` branch guards and `set` reads of this event
        fields.add(field)
        leaves.append((field, op))
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


def _choice_targets(t: Transition) -> list[Node]:
    if t.choice is None:
        return []
    targets = [target for _, target in t.choice.branches]
    if t.choice.default is not None:
        targets.append(t.choice.default)
    return targets


def _choice_leaves(t: Transition) -> list[Predicate]:
    """The leaves of a `choose`'s branch guards (`when`)."""
    if t.choice is None:
        return []
    return [leaf for guard, _ in t.choice.branches for leaf in _tree_leaves(guard)]


def _guard_leaves(t: Transition) -> list[Predicate]:
    """Every composable-tree leaf a transition's guards hold: its `where` and, for a
    `choose`, each branch's `when`."""
    where = _tree_leaves(t.event_filter.predicate) if t.event_filter is not None else []
    return where + _choice_leaves(t)


def _expr_refs(expr: Optional[Expr]) -> list[Expr]:
    """The `ref` nodes of an assignment's right-hand side."""
    if expr is None:
        return []
    if expr.kind == "ref":
        return [expr]
    return _expr_refs(expr.left) + _expr_refs(expr.right)


def _literal_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return "list"


def _expr_type(expr: Expr, defn: Definition) -> Optional[str]:
    """The static type of a right-hand side where it is knowable (None = not knowable)."""
    if expr.kind == "lit":
        return _literal_type(expr.value)
    if expr.kind == "ref":
        spec = defn.context_schema.get(expr.field or "") if expr.source == "context" else None
        return spec.type if spec is not None and spec.type != "any" else None
    if expr.op == "/":
        return "float"
    sides = {_expr_type(e, defn) for e in (expr.left, expr.right) if e is not None}
    if sides == {"int"}:
        return "int"  # `+ - *` of two ints
    if sides <= {"int", "float"} and "float" in sides:
        return "float"
    return None


def _check_assignments(node: Node, t: Transition, defn: Definition, issues: list[Issue]) -> None:
    """A transition's `set`: what it reads must be readable, what it writes must fit."""
    for assign in t.assignments:
        for ref in _expr_refs(assign.expr):
            if ref.source == "context" and ref.field:
                _check_context_ref(node, ref.field, "eq", defn, issues)
            elif ref.source == "event" and t.event_filter is None:
                issues.append(
                    Issue(
                        "event_ref_without_event",
                        "error",
                        node.full_path,
                        f"`set` reads event.{ref.field} on an automatic transition, which has no event",
                    )
                )
        if assign.expr.kind == "arith":
            for operand in (assign.expr.left, assign.expr.right):
                kind = _expr_type(operand, defn) if operand is not None else None
                if kind in ("string", "bool", "list"):
                    issues.append(
                        Issue(
                            "assign_type_mismatch",
                            "error",
                            node.full_path,
                            f"`{assign.expr.op}` in `set context.{assign.field}` needs numbers, got a {kind}",
                        )
                    )
        if not defn.context_schema:
            continue
        spec = defn.context_schema.get(assign.field)
        if spec is None:
            issues.append(
                Issue(
                    "unknown_context_field",
                    "error",
                    node.full_path,
                    f"`set` writes context.{assign.field}, which the context doesn't declare",
                )
            )
            continue
        kind = _expr_type(assign.expr, defn)
        fits = (
            kind is None
            or spec.type == "any"
            or kind == spec.type
            or (spec.type == "float" and kind == "int")
        )
        if not fits:
            issues.append(
                Issue(
                    "assign_type_mismatch",
                    "error",
                    node.full_path,
                    f"`set context.{assign.field}` ({spec.type}) is given a {kind}",
                )
            )


def _check_context_ref(node: Node, field: str, op: str, defn: Definition, issues: list[Issue]) -> None:
    """A `context.<field>` reference in the model, checked against the declared schema (a
    machine with no `context` block has an untyped context: nothing to check)."""
    if not defn.context_schema:
        return
    spec = defn.context_schema.get(field)
    if spec is None:
        issues.append(
            Issue(
                "unknown_context_field", "error", node.full_path, f"the context declares no field {field!r}"
            )
        )
    elif op in _NUMERIC_OPS and spec.type in ("string", "bool"):
        issues.append(
            Issue(
                "op_type_mismatch",
                "warning",
                node.full_path,
                f"op {op!r} on context.{field} ({spec.type}) compares a non-ordered field",
            )
        )


def _operand_type(source: str, field: str, t: Transition, defn: Definition) -> Optional[str]:
    """The declared type of what a guard reads, where it is knowable (None = not knowable):
    a context field from the schema, an event field from its event's declaration (the same
    type in every kind the transition accepts)."""
    if source == "context":
        specs = [defn.context_schema.get(field)]
    else:
        kinds = [k.strip() for k in t.event_filter.kind.split("|")] if t.event_filter is not None else []
        specs = [defn.events[k].fields.get(field) if k in defn.events else None for k in kinds]
    if not specs or any(spec is None for spec in specs):
        return None
    types = {spec.type for spec in specs if spec is not None}
    return types.pop() if len(types) == 1 and "any" not in types else None


def _comparable(a: str, b: str) -> bool:
    return a == b or {a, b} <= {"int", "float"}


def _check_compared_types(
    node: Node, t: Transition, leaf: Predicate, defn: Definition, issues: list[Issue]
) -> None:
    """A guard comparing two references whose declared types differ (an `int` with a
    `string`, ...) can't do what it says — they are never equal, never ordered: warn. A
    comparison with a literal isn't checked here."""
    ref = leaf.value_ref
    if ref is None or leaf.op == "in" or not leaf.field or not ref.field:
        return
    left = _operand_type(leaf.source, leaf.field, t, defn)
    right = _operand_type(ref.source or "event", ref.field, t, defn)
    if left is not None and right is not None and not _comparable(left, right):
        issues.append(
            Issue(
                "compare_type_mismatch",
                "warning",
                node.full_path,
                f"a guard compares {leaf.source}.{leaf.field} ({left}) with {ref.source}.{ref.field} "
                f"({right}): values of different types are never equal or ordered",
            )
        )


def _check_context_refs(defn: Definition, issues: list[Issue]) -> None:
    for node in defn.index.values():
        for parent_key in node.invoke_with.values():  # what a `with` reads from this context
            _check_context_ref(node, parent_key, "eq", defn, issues)
        for t in node.transitions:
            for leaf in _guard_leaves(t):
                for src, f in _operands(leaf):
                    if src == "context":
                        _check_context_ref(node, f, leaf.op or "eq", defn, issues)
                    elif t.event_filter is None:  # a `when` over the event on an eventless `choose`
                        issues.append(
                            Issue(
                                "event_ref_without_event",
                                "error",
                                node.full_path,
                                f"a guard reads the event's {f!r} on an automatic transition, "
                                "which has no event — it can never hold",
                            )
                        )
                _check_compared_types(node, t, leaf, defn, issues)
            _check_assignments(node, t, defn, issues)


def _check_events(defn: Definition, issues: list[Issue]) -> None:
    for node in defn.index.values():
        for t in node.transitions:
            if t.event_filter is not None:
                extra = [
                    (f, leaf.op or "eq")
                    for leaf in _choice_leaves(t)
                    for src, f in _operands(leaf)
                    if src == "event"
                ]
                extra += [
                    (r.field, "eq")
                    for a in t.assignments
                    for r in _expr_refs(a.expr)
                    if r.source == "event" and r.field
                ]
                _check_event(node, t.event_filter, defn, issues, tuple(extra))


# teardown events whose transition must land directly on a terminal: code + remedy
_TEARDOWN_EVENTS = {
    "Cancel": (
        "cancel_target_not_terminal",
        "model multi-step or async cancellation as an ordinary domain event instead "
        "(e.g. `CancelOrder`), not the reserved `Cancel`",
    ),
    "Expired": (
        "expired_target_not_terminal",
        "an expired execution ends in that same step; a longer wind-down belongs in "
        "ordinary transitions before the `ttl` runs out",
    ),
}


def _check_cancel_target(node: Node, t: Transition, issues: list[Issue]) -> None:
    """`Cancel` is the control plane's cooperative-teardown signal, not a business
    event: `cancel()` decides cooperative-vs-forceful structurally
    (`has_cancel_handler`), and the worker starts discarding the execution's queued
    backlog the moment the CAS to CANCELLING lands — before the injected `Cancel`
    even gets its turn. So its transition must resolve, in that same step, straight
    to the model's own terminal (a sink `_is_terminal` already recognizes, `final`
    or not). A model whose cleanup needs more than that — release a lock now, then
    wait on a further event to actually finish — is modelling *business*
    cancellation, not execution cancellation, and should use its own event name
    (e.g. `CancelOrder`) instead of the reserved `Cancel`.

    `Expired` — the machine's `ttl` running out — is held to the same rule for the same
    reason: the execution is being ended, not asked to do more work."""
    ef = t.event_filter
    if ef is None:
        return
    kinds = [k for k in (k.strip() for k in ef.kind.split("|")) if k in _TEARDOWN_EVENTS]
    if not kinds:
        return
    targets: list[Node] = []
    if t.target is not None:
        targets.append(t.target)
    if t.selector is not None:
        names = list(t.selector.mapper.values())
        if t.selector.default is not None:
            names.append(t.selector.default)
        for name in names:
            # relative to `node` — the transition's OWNING scope (it lives in
            # `node.transitions`), not `t.source`: a dotted `from` can put the
            # source deep inside a nested composite while the transition itself
            # is owned by an outer scope, and a name can resolve to a different
            # node from each starting point. `node` is what the engine/builder
            # actually resolves selector branches against (see `_check_selectors`,
            # the same convention) — resolving from `t.source` here would validate
            # a target the engine never actually lands on.
            resolved = resolve_relative(node, name)
            if resolved is not None:
                targets.append(resolved)
    targets.extend(_choice_targets(t))
    root = _execution_root_of(node)
    for kind in kinds:
        code, remedy = _TEARDOWN_EVENTS[kind]
        for target in targets:
            why = _why_not_ending(target, root)
            if why is not None:
                issues.append(
                    Issue(
                        code,
                        "error",
                        node.full_path,
                        f"`on {kind}` must resolve directly to a terminal that ends the execution, "
                        f"got {target.full_path!r}: {why} — {remedy}",
                    )
                )


def _why_not_ending(target: Node, root: Node) -> Optional[str]:
    """Why reaching `target` would NOT end `root`'s Execution in that same step, or None
    if it would: it must be a sink of this Execution whose bubble reaches `root` uncaught."""
    if _execution_root_of(target) is not root:
        return "it is not part of this execution"
    if not _is_terminal(target):
        return "it has its own outgoing transitions"
    catcher = _catching_ancestor(target, root)
    if catcher is not None:
        return f"the enclosing {catcher.full_path!r} has its own transition and the execution would go on"
    return None


def _check_cancel_targets(defn: Definition, issues: list[Issue]) -> None:
    for node in defn.index.values():
        for t in node.transitions:
            _check_cancel_target(node, t, issues)


def _check_ttl(defn: Definition, issues: list[Issue]) -> None:
    """`ttl` must be a positive number of seconds, and `on Expired` only ever fires on a
    machine that declares one."""
    if defn.ttl is not None and defn.ttl <= 0:
        issues.append(Issue("ttl_not_positive", "error", "", f"`ttl` must be positive, got {defn.ttl}"))
    if defn.ttl is None:
        for node in defn.index.values():
            for t in node.transitions:
                ef = t.event_filter
                if ef is not None and "Expired" in [k.strip() for k in ef.kind.split("|")]:
                    issues.append(
                        Issue(
                            "expired_without_ttl",
                            "warning",
                            node.full_path,
                            "`on Expired` never fires: the machine declares no `ttl`",
                        )
                    )


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


def _check_with(node: Node, issues: list[Issue]) -> None:
    """`with` passes context to an `invoke` child or to each region of an orthogonal node;
    anywhere else nothing reads it."""
    if node.invoke_with and node.invoke is None and node.kind not in _ORTHOGONAL:
        issues.append(
            Issue(
                "with_without_children",
                "warning",
                node.full_path,
                "`with` has no effect here: it passes context to an `invoke` or to an orthogonal node's regions",
            )
        )


def _can_rerun(source: Node) -> bool:
    """Whether the execution can get past `source` other than through its automatic
    transitions: a transition on an event (other than the teardown ones, which just end the
    execution) from `source` — a step taken there re-drains, re-running them — or from an
    enclosing state of the same execution, which leaves it; or an `on activity` hook, which an
    event with no transition runs (and may change the context) before re-draining."""
    if source.on_activity is not None:
        return True
    root = _execution_root_of(source)
    node: Optional[Node] = source
    while node is not None:
        for t in _all_transitions_from(node):
            kinds = (
                {k.strip() for k in t.event_filter.kind.split("|")} if t.event_filter is not None else set()
            )
            if kinds - set(_TEARDOWN_EVENTS):
                return True
        if node is root:
            break
        node = node.parent
    return False


def _check_choose_can_hang(defn: Definition, issues: list[Issue]) -> None:
    """An automatic `choose` with no `else` and no branch holding simply doesn't fire — fine
    while something can re-run it, a dead end otherwise: the execution would sit in its
    source forever. With an `else` it always fires."""
    for node in defn.index.values():
        for t in node.transitions:
            if t.choice is None or t.event_filter is not None or t.choice.default is not None:
                continue
            if not _can_rerun(t.source):
                issues.append(
                    Issue(
                        "choose_can_hang",
                        "error",
                        t.source.full_path,
                        "an automatic `choose` without `else` can never be re-evaluated here (no "
                        "event transition, no `on activity`): if no branch holds, the execution "
                        "waits forever — add an `else`",
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


def _catching_ancestor(leaf: Node, root: Node) -> Optional[Node]:
    """The first ancestor of `leaf` below `root` with an outgoing transition of its own —
    it catches the bubble from `leaf`'s sink and the Execution goes on (an automatic
    transition fires, an event transition waits). None if the bubble reaches `root`."""
    anc = leaf.parent
    while anc is not None and anc is not root:
        if any(t.source is anc for t in _all_transitions_from(anc)):
            return anc
        anc = anc.parent
    return None


def _execution_root_of(node: Node) -> Node:
    """The node whose subtree runs as `node`'s own Execution: the nearest ancestor-or-self
    that is an orthogonal region (a child of an orthogonal node), else the machine root."""
    cur = node
    while cur.parent is not None:
        if cur.parent.kind in _ORTHOGONAL:
            return cur
        cur = cur.parent
    return cur


def _execution_terminals(root: Node) -> list[Node]:
    """Leaf sinks in `root`'s subtree that actually END `root`'s Execution: the
    bubble from the leaf reaches `root` UNCAUGHT (see `_catching_ancestor`). Does NOT
    descend into a nested orthogonal's regions (those are their own execution roots,
    validated apart)."""
    out: list[Node] = []

    def walk(node: Node) -> None:
        if not node.children:
            if _is_terminal(node) and _catching_ancestor(node, root) is None:
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
        _check_with(node, issues)
    _check_nondeterminism(defn, issues)
    _check_reachability(defn, issues)
    _check_events(defn, issues)
    _check_context_refs(defn, issues)
    _check_cancel_targets(defn, issues)
    _check_choose_can_hang(defn, issues)
    _check_ttl(defn, issues)
    _check_terminal_outcomes(defn, issues)
    return issues


def validate_or_raise(defn: Definition) -> list[Issue]:
    """Validate and raise `ValidationError` on any error-level issue. Returns the
    full issue list (so warnings are still visible) when it does not raise."""
    issues = validate(defn)
    if any(i.severity == "error" for i in issues):
        raise ValidationError(issues)
    return issues
