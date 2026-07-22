"""The state-machine **Definition**: the immutable "program".

A Definition is a tree of `Node`s navigated by **real references** (`parent` /
`children`) — not by parsing dotted `full_path` strings. `full_path` is kept only
as a stable identity/address (for serialization and for addressing a position in
an `Execution`). Transitions hold references to their source/target nodes and
belong to the composite that is their scope.

This module is pure data + navigation helpers; it has no execution state and no
IO. It is produced from the normalized YAML/JSON dict by the builder (next step).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Union

from harel.definition.events import EventType


class NodeKind(Enum):
    LEAF = "State"
    COMPOSITE = "CompositeState"
    PARALLEL = "ParallelState"
    ORTHOGONAL = "OrthogonalState"


@dataclass
class ActionRef:
    """Reference to a user function (resolved lazily) plus bound inputs."""

    function: Union[str, Callable]
    inputs: dict = field(default_factory=dict)
    package: Optional[str] = None


@dataclass
class Predicate:
    """A composable event-data predicate. Exactly one shape per node:
    a `leaf` (field/op/value) or a combinator `all`/`any`/`not` over children
    (`not` has a single child). Leaf op is one of eq/ne/lt/le/gt/ge/in."""

    node: str  # "leaf" | "all" | "any" | "not"
    # `children` first so the dataclasses `field()` call is not shadowed by the
    # `field` attribute below (both share the name `field` in the class body)
    children: list["Predicate"] = field(default_factory=list)
    field: Optional[str] = None
    op: Optional[str] = None
    value: Any = None


@dataclass
class EventFilter:
    """Guard over an event. `kind` allows `A | B` alternation. `predicates` are
    flat `field__op -> value` leaves combined with AND (back-compat + PlantUML);
    `predicate` is an optional composable tree (`all`/`any`/`not`). Both, when
    present, are AND-ed. A predicate on a field absent from the event fails."""

    kind: str
    predicates: dict = field(default_factory=dict)
    predicate: Optional[Predicate] = None


@dataclass
class Selector:
    """A transition bifurcation: run `action`, map its result to a target name
    (resolved within the transition's scope). The result is matched by `str(value)`
    against `mapper`; an unmatched result falls back to `default` (the `else`
    branch) or is an error. `enum`, when set, declares the result values the
    selector may return — the validator checks the mapper covers them."""

    action: ActionRef
    mapper: dict = field(default_factory=dict)  # result(str) -> target node name
    default: Optional[str] = None  # `else` target name; None => an unmatched result is an error
    enum: Optional[list] = None  # declared result values (validation only)


@dataclass
class Transition:
    """Owned by the composite that is its scope. `source` is a node within that
    scope; the transition applies to `source` and its descendants."""

    source: "Node"
    target: Optional["Node"] = None
    event_filter: Optional[EventFilter] = None  # None => automatic transition
    selector: Optional[Selector] = None

    @property
    def is_automatic(self) -> bool:
        return self.event_filter is None


@dataclass
class Node:
    name: str
    full_path: str  # DERIVED: stable identity/address, not the navigation mechanism
    kind: NodeKind
    parent: Optional["Node"] = None
    children: list["Node"] = field(default_factory=list)
    on_enter: Optional[ActionRef] = None
    on_activity: Optional[ActionRef] = None
    on_exit: Optional[ActionRef] = None
    timeout: Optional[Union[int, dict]] = None  # literal seconds, or {context: key} (dynamic/backoff)
    outcome: Optional[str] = None  # result label recorded on the Execution when this state is a terminal
    carry: tuple[str, ...] = ()  # context keys a region propagates on its `Finished` (besides `outcome`)
    defer: frozenset[str] = frozenset()  # event kinds held (not dropped) while unhandled in this node's scope
    invoke: Optional[str] = None  # submachine FQN: entering this state runs that machine as a child
    invoke_with: dict[str, str] = field(default_factory=dict)  # child-ctx-key -> parent-ctx-key (input)
    invoke_each: Optional[tuple[str, str]] = None  # (loop_var, collection_key): fan out ONE child per
    #                                                collection entry (addressed; join on completion)
    start_state: Optional[str] = None  # initial child name (composites)
    allow_history: bool = True
    transitions: list[Transition] = field(default_factory=list)  # scope = this node

    @property
    def is_composite(self) -> bool:
        return self.kind is not NodeKind.LEAF

    def child(self, name: str) -> Optional["Node"]:
        return next((c for c in self.children if c.name == name), None)

    def __repr__(self) -> str:  # avoid recursing into parent/children
        return f"<Node {self.kind.value} {self.full_path!r}>"


@dataclass
class Definition:
    """The whole program: a root node plus a path->node index for O(1) lookup."""

    id: str
    root: Node
    index: dict[str, Node] = field(default_factory=dict)
    events: dict[str, EventType] = field(default_factory=dict)  # declared event types (empty => unchecked)
    submachines: dict[str, "Definition"] = field(default_factory=dict)  # synthetic FQN -> inline `invoke`
    #                                                                     target Definition (QML-style)

    def get(self, path: str) -> Optional[Node]:
        return self.index.get(path)


# --- navigation helpers (operate on references, never on path strings) --------


def ancestors(node: Node) -> list[Node]:
    """`node`'s ancestors from the immediate parent up to the root (inclusive)."""
    out: list[Node] = []
    cur = node.parent
    while cur is not None:
        out.append(cur)
        cur = cur.parent
    return out


def is_descendant(node: Node, ancestor: Node) -> bool:
    """True if `node` is `ancestor` itself or somewhere below it."""
    cur: Optional[Node] = node
    while cur is not None:
        if cur is ancestor:
            return True
        cur = cur.parent
    return False


def chain(top: Node, leaf: Node) -> list[Node]:
    """The path of nodes from `top` (inclusive) down to `leaf` (inclusive).

    `top` must be an ancestor-or-self of `leaf`. Returns [top, ..., leaf].
    """
    out: list[Node] = []
    cur: Optional[Node] = leaf
    while cur is not None:
        out.append(cur)
        if cur is top:
            return list(reversed(out))
        cur = cur.parent
    raise ValueError(f"{top!r} is not an ancestor of {leaf!r}")


def lca(a: Node, b: Node) -> Node:
    """Least common ancestor of two nodes (UML transition enter/exit pivot)."""
    a_anc = {id(n): n for n in [a, *ancestors(a)]}
    cur: Optional[Node] = b
    while cur is not None:
        if id(cur) in a_anc:
            return cur
        cur = cur.parent
    raise ValueError(f"no common ancestor for {a!r} and {b!r}")


def descend(scope: Node, name: str) -> Optional[Node]:
    """Resolve `name` relative to `scope` by descending child by child."""
    node: Optional[Node] = scope
    for seg in name.split("."):
        node = node.child(seg) if node is not None else None
    return node


def resolve_relative(scope: Node, name: str) -> Optional[Node]:
    """Sibling-lookup: descend `name` from `scope`, then walk up the ancestors.

    The reference-based equivalent of the old `_get_sibling_state`: resolves a
    transition target (or selector mapper name) relative to the scope composite,
    preferring the innermost match.
    """
    cur: Optional[Node] = scope
    while cur is not None:
        found = descend(cur, name)
        if found is not None:
            return found
        cur = cur.parent
    return None
