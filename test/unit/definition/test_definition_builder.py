"""Unit tests for building a `Definition` from the DSL.

Structural only (no execution): node kinds, derived `full_path`s, real
`parent`/`children` edges, and transitions resolved to node *references* (by
identity) with their event filters / selectors. Plus the builder's error cases.
"""

import pytest

from harel.definition.builder import BuildError, build_definition
from harel.definition.model import NodeKind
from harel.dsl import definition_from_dsl

DSL = """
machine M {
   initial A

   state A {
      on enter mod.a
   }

   state Outer {
      initial In1
      state In1 {
         on enter mod.in1
      }
      state In2 {}

      from In1 to In2 on Step
   }

   state Done {}

   from A to Outer
   from Outer to Done on Fin
   from A select mod.pick on Pick { true to Outer  false to Done }
}
"""


def test_root_is_parallel_with_empty_path():
    defn = definition_from_dsl(DSL, "M")
    assert defn.root.kind is NodeKind.PARALLEL
    assert defn.root.full_path == ""
    assert defn.root.start_state == "A"


def test_index_and_kinds():
    defn = definition_from_dsl(DSL, "M")
    assert set(defn.index) == {"", "A", "Outer", "Outer.In1", "Outer.In2", "Done"}
    assert defn.index["A"].kind is NodeKind.LEAF
    assert defn.index["Outer"].kind is NodeKind.COMPOSITE  # has states, no explicit type
    assert defn.index["Outer"].start_state == "In1"


def test_real_parent_child_edges():
    defn = definition_from_dsl(DSL, "M")
    assert defn.index["Outer.In1"].parent is defn.index["Outer"]
    assert defn.index["Outer"].parent is defn.root
    assert defn.index["Outer"].child("In1") is defn.index["Outer.In1"]


def test_transitions_resolve_to_node_refs():
    defn = definition_from_dsl(DSL, "M")
    by_target = {}
    for t in defn.root.transitions:
        by_target[t.target.full_path if t.target else "selector"] = t

    auto = by_target["Outer"]
    assert auto.source is defn.index["A"]
    assert auto.target is defn.index["Outer"]
    assert auto.is_automatic  # no event_filter

    fin = by_target["Done"]
    assert fin.source is defn.index["Outer"]
    assert fin.event_filter.kind == "Fin"
    assert not fin.is_automatic

    sel = by_target["selector"]
    assert sel.source is defn.index["A"]
    assert sel.target is None
    assert sel.event_filter.kind == "Pick"
    assert sel.selector.mapper == {"True": "Outer", "False": "Done"}  # keys stringified


def test_nested_transition_scope_is_the_composite():
    defn = definition_from_dsl(DSL, "M")
    outer = defn.index["Outer"]
    assert len(outer.transitions) == 1
    t = outer.transitions[0]
    assert t.source is defn.index["Outer.In1"]
    assert t.target is defn.index["Outer.In2"]


def test_orthogonal_kind():
    dsl = """
machine M {
   initial Fork
   orthogonal Fork {
      state A {
         initial A1
         state A1 {}
      }
   }
}
"""
    defn = definition_from_dsl(dsl, "M")
    assert defn.index["Fork"].kind is NodeKind.ORTHOGONAL
    assert defn.index["Fork.A"].kind is NodeKind.PARALLEL


# --- error cases --------------------------------------------------------------


def test_transition_without_target_or_selector_raises():
    config = {"start": "A", "states": {"A": {}, "B": {}}, "transitions": [{"from": "A"}]}
    with pytest.raises(BuildError):
        build_definition(config, {}, "M")


def test_unresolvable_target_raises():
    config = {"start": "A", "states": {"A": {}}, "transitions": [{"from": "A", "to": "Nope"}]}
    with pytest.raises(BuildError):
        build_definition(config, {}, "M")


def test_unresolvable_source_raises():
    config = {"start": "A", "states": {"A": {}}, "transitions": [{"from": "Nope", "to": "A"}]}
    with pytest.raises(BuildError):
        build_definition(config, {}, "M")
