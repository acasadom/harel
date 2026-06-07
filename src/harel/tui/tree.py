"""The pure tree-highlight model — the testable core of the monitor TUI.

`build_tree_model(definition, execution)` walks the `Definition` node tree and produces
a serializable `TreeModel` of plain dataclasses (NO textual types): each node carries
whether it is the active leaf, on the active path, or inactive, plus an optional region
annotation for orthogonal/invoke children. The textual `StatechartTree` widget renders
this; tests assert against it directly. When the Definition can't be resolved the model
degrades to `resolved=False` (the UI then shows data panels only).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from harel.definition.model import Definition, Node, NodeKind, chain
from harel.engine.execution import Execution


class NodeMark(Enum):
    ACTIVE = "active"  # the active leaf (or the active branch the leaf descends from)
    ON_ACTIVE_PATH = "ancestor"  # an ancestor of the active node (the highlight chain)
    INACTIVE = "inactive"


@dataclass(frozen=True)
class RegionInfo:
    """Present iff this node is an orthogonal region root or an invoke child: the join
    state the parent Execution tracks for it (`Execution.children[child_id]`)."""

    child_id: str
    finished: bool
    outcome: Optional[str]
    submachine: bool  # an invoke child (a different Definition) — not descended into


@dataclass(frozen=True)
class TreeNode:
    full_path: str  # stable id (the widget's node key)
    name: str
    kind: NodeKind
    mark: NodeMark
    region: Optional[RegionInfo] = None
    children: tuple["TreeNode", ...] = ()


@dataclass(frozen=True)
class TreeModel:
    """The renderable statechart for one Execution. `root` is None when the Definition
    could not be resolved (`resolved=False`); the UI then shows data-only."""

    root: Optional[TreeNode]
    active_path: Optional[str]
    resolved: bool = True
    regions: tuple[RegionInfo, ...] = field(default=())  # all region annotations, for data-only mode


def _regions_by_path(exe: Execution) -> dict[str, RegionInfo]:
    """Map each child region's branch `root_path` to its join state. A region whose
    root_path is empty (the parent's own root) is skipped — it isn't a distinct node."""
    out: dict[str, RegionInfo] = {}
    for child_id, child in exe.children.items():
        if child.root_path:
            out[child.root_path] = RegionInfo(
                child_id=child_id,
                finished=child.finished,
                outcome=child.outcome,
                submachine=child.submachine,
            )
    return out


def build_tree_model(definition: Optional[Definition], exe: Execution) -> TreeModel:
    """Build the highlight tree for `exe` over `definition`. With no definition (or an
    active_path that doesn't resolve), degrade gracefully rather than raise."""
    regions = _regions_by_path(exe)
    if definition is None:
        return TreeModel(
            root=None, active_path=exe.active_path, resolved=False, regions=tuple(regions.values())
        )

    # the set of full_paths on the active chain (root..active leaf); the leaf is ACTIVE,
    # the rest ON_ACTIVE_PATH. An unknown active_path simply highlights nothing.
    active_node: Optional[Node] = definition.get(exe.active_path) if exe.active_path else None
    on_path: set[str] = set()
    if active_node is not None:
        on_path = {n.full_path for n in chain(definition.root, active_node)}

    def visit(node: Node) -> TreeNode:
        if active_node is not None and node.full_path == active_node.full_path:
            mark = NodeMark.ACTIVE
        elif node.full_path in on_path:
            mark = NodeMark.ON_ACTIVE_PATH
        else:
            mark = NodeMark.INACTIVE
        region = regions.get(node.full_path)
        # a submachine (invoke) child is a black box — a different Definition — so don't
        # descend; orthogonal regions live in this same Definition, so recurse normally.
        kids: tuple[TreeNode, ...] = ()
        if not (region is not None and region.submachine):
            kids = tuple(visit(c) for c in node.children)
        return TreeNode(node.full_path, node.name, node.kind, mark, region, kids)

    return TreeModel(
        root=visit(definition.root),
        active_path=exe.active_path,
        resolved=True,
        regions=tuple(regions.values()),
    )
