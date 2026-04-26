"""Unit tests for the reference-based `Definition` navigation helpers.

A small tree is built by hand so the helpers are exercised in isolation (no
builder, no engine). Shape:

    root ("")
      A          (leaf)
      Outer       (composite)
        Outer.In1 (leaf)
        Outer.In2 (leaf)
"""

import pytest

from harel.definition.model import (
    Definition,
    Node,
    NodeKind,
    ancestors,
    chain,
    descend,
    is_descendant,
    lca,
    resolve_relative,
)


def _tree():
    root = Node(name="M", full_path="", kind=NodeKind.PARALLEL)
    a = Node(name="A", full_path="A", kind=NodeKind.LEAF, parent=root)
    outer = Node(name="Outer", full_path="Outer", kind=NodeKind.COMPOSITE, parent=root)
    in1 = Node(name="In1", full_path="Outer.In1", kind=NodeKind.LEAF, parent=outer)
    in2 = Node(name="In2", full_path="Outer.In2", kind=NodeKind.LEAF, parent=outer)
    root.children = [a, outer]
    outer.children = [in1, in2]
    index = {n.full_path: n for n in (root, a, outer, in1, in2)}
    return Definition(id="M", root=root, index=index), index


def test_is_composite():
    _, ix = _tree()
    assert ix["Outer"].is_composite
    assert ix[""].is_composite  # PARALLEL root
    assert not ix["A"].is_composite


def test_child_lookup():
    _, ix = _tree()
    assert ix[""].child("A") is ix["A"]
    assert ix["Outer"].child("In2") is ix["Outer.In2"]
    assert ix[""].child("nope") is None


def test_ancestors():
    _, ix = _tree()
    assert ancestors(ix["Outer.In1"]) == [ix["Outer"], ix[""]]
    assert ancestors(ix[""]) == []


def test_is_descendant():
    _, ix = _tree()
    assert is_descendant(ix["Outer.In1"], ix["Outer"])
    assert is_descendant(ix["Outer"], ix["Outer"])  # itself
    assert is_descendant(ix["Outer.In1"], ix[""])
    assert not is_descendant(ix["A"], ix["Outer"])


def test_chain():
    defn, ix = _tree()
    assert chain(ix[""], ix["Outer.In1"]) == [ix[""], ix["Outer"], ix["Outer.In1"]]
    assert chain(ix["Outer"], ix["Outer.In1"]) == [ix["Outer"], ix["Outer.In1"]]
    assert chain(ix["Outer.In1"], ix["Outer.In1"]) == [ix["Outer.In1"]]


def test_chain_requires_ancestor():
    _, ix = _tree()
    with pytest.raises(ValueError):
        chain(ix["A"], ix["Outer.In1"])  # A is not an ancestor of In1


def test_lca():
    _, ix = _tree()
    assert lca(ix["Outer.In1"], ix["Outer.In2"]) is ix["Outer"]
    assert lca(ix["Outer.In1"], ix["A"]) is ix[""]
    assert lca(ix["Outer.In1"], ix["Outer.In1"]) is ix["Outer.In1"]
    assert lca(ix["Outer.In1"], ix["Outer"]) is ix["Outer"]


def test_descend():
    _, ix = _tree()
    assert descend(ix[""], "Outer.In1") is ix["Outer.In1"]
    assert descend(ix[""], "A") is ix["A"]
    assert descend(ix[""], "Outer.nope") is None


def test_resolve_relative_inner_then_outer():
    _, ix = _tree()
    # sibling within scope
    assert resolve_relative(ix["Outer"], "In2") is ix["Outer.In2"]
    # not in scope -> walk up to the root
    assert resolve_relative(ix["Outer"], "A") is ix["A"]
    assert resolve_relative(ix["Outer"], "Outer.In1") is ix["Outer.In1"]


def test_definition_get():
    defn, ix = _tree()
    assert defn.get("Outer.In1") is ix["Outer.In1"]
    assert defn.get("missing") is None
