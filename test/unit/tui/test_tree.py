"""build_tree_model — the pure tree-highlight core (no textual)."""

from harel.dsl import definition_from_dsl
from harel.engine.execution import ChildState, Execution
from harel.tui.tree import NodeMark, build_tree_model

HIER = """
machine M {
   initial Idle
   state Idle {}
   state Work {
      initial Step1
      state Step1 {}
      state Step2 {}
      from Step1 to Step2 on Next
   }
   from Idle to Work on Go
}
"""

ORTHO = """
machine J {
   initial Fork
   orthogonal Fork {
      state A { initial A1  state A1 {}  final A2 success  from A1 to A2 on Go }
      state B { initial B1  state B1 {}  final B2 success  from B1 to B2 on Go }
   }
   final Done success
   from Fork to Done
}
"""


def _path(defn, name):
    return next(p for p, n in defn.index.items() if n.name == name)


def _find(node, name):
    if node.name == name:
        return node
    for c in node.children:
        hit = _find(c, name)
        if hit is not None:
            return hit
    return None


def test_active_leaf_and_ancestors_marked():
    defn = definition_from_dsl(HIER, "M")
    step1 = _path(defn, "Step1")
    exe = Execution(definition_id="M", active_path=step1)
    model = build_tree_model(defn, exe)

    assert model.resolved and model.active_path == step1
    assert _find(model.root, "Step1").mark is NodeMark.ACTIVE
    # Work and the root are on the active path (ancestors of the active leaf)
    assert _find(model.root, "Work").mark is NodeMark.ON_ACTIVE_PATH
    assert model.root.mark is NodeMark.ON_ACTIVE_PATH
    # a sibling/unvisited state is inactive
    assert _find(model.root, "Idle").mark is NodeMark.INACTIVE
    assert _find(model.root, "Step2").mark is NodeMark.INACTIVE


def test_no_active_path_marks_nothing():
    defn = definition_from_dsl(HIER, "M")
    model = build_tree_model(defn, Execution(definition_id="M"))  # active_path is None
    assert model.resolved
    assert all(
        n.mark is NodeMark.INACTIVE
        for n in [model.root, *(_find(model.root, x) for x in ("Idle", "Work", "Step1"))]
    )


def test_unknown_active_path_does_not_crash():
    defn = definition_from_dsl(HIER, "M")
    model = build_tree_model(defn, Execution(definition_id="M", active_path="nope.nope"))
    assert model.resolved
    assert _find(model.root, "Step1").mark is NodeMark.INACTIVE


def test_unresolved_definition_degrades_to_data_only():
    model = build_tree_model(None, Execution(definition_id="M", active_path="x"))
    assert model.resolved is False and model.root is None and model.active_path == "x"


def test_orthogonal_regions_annotated():
    defn = definition_from_dsl(ORTHO, "J")
    a_path, b_path = _path(defn, "A"), _path(defn, "B")
    exe = Execution(
        definition_id="J",
        active_path=_path(defn, "Fork"),
        children={
            "c-A": ChildState(root_path=a_path, finished=True, outcome="success"),
            "c-B": ChildState(root_path=b_path, finished=False, outcome=None),
        },
    )
    model = build_tree_model(defn, exe)
    a_node = _find(model.root, "A")
    assert a_node.region is not None and a_node.region.finished and a_node.region.outcome == "success"
    b_node = _find(model.root, "B")
    assert b_node.region is not None and not b_node.region.finished
    # a non-submachine region still descends into its own subtree
    assert _find(a_node, "A1") is not None


def test_submachine_region_is_a_stub_not_descended():
    defn = definition_from_dsl(ORTHO, "J")
    a_path = _path(defn, "A")
    exe = Execution(
        definition_id="J",
        active_path=_path(defn, "Fork"),
        children={"c-A": ChildState(root_path=a_path, submachine=True)},
    )
    a_node = _find(build_tree_model(defn, exe).root, "A")
    assert a_node.region is not None and a_node.region.submachine
    assert a_node.children == ()  # a black-box invoke child is not descended into
