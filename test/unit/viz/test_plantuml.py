"""PlantUML rendering of the features not already covered by the standard-job
golden tests (which exercise selectors -> <<choice>> and composite nesting).

The gap is the orthogonal AND-state, rendered as parallel regions separated by
`||`. (`test_plantuml_parity` only checks the two builders agree, not the output.)
"""

from harel.dsl import definition_from_dsl
from harel.viz.plantuml import render

ORTHOGONAL = """
machine M {
  initial Fork
  orthogonal Fork {
    state A { initial A1  state A1 { on enter mod.a1 } }
    state B { initial B1  state B1 { on enter mod.b1 } }
  }
  state Done { on enter mod.done }
  from Fork to Done
}
"""


def test_orthogonal_renders_parallel_regions():
    lines = [line.strip() for line in render(definition_from_dsl(ORTHOGONAL, "M")).split("\n")]

    # the two regions are nested composites separated by the `||` region marker
    assert "||" in lines
    assert 'state "A" as Fork.A {' in lines
    assert 'state "B" as Fork.B {' in lines
    # each region's states render (with their short function name) inside its block
    assert 'state "A1" as Fork.A.A1: <b>on_enter</b>: <i>a1</i>' in lines
    assert 'state "B1" as Fork.B.B1: <b>on_enter</b>: <i>b1</i>' in lines


COMPOSITE_GUARDS = """
event E {}
guard vip = tier == "gold" or spend > 1000
machine M {
  initial A
  state A {}
  final B success {}
  final C success {}
  final D success {}
  from A to B on E where (y > 1 and y < 5) or z == "go"
  from A to C on E where status == "paid" and not (n in [1, 2] or retry == true)
  from A to D on E where vip and region == "eu"
}
"""


def test_composite_and_named_guards_are_rendered():
    # the composable predicate tree (and/or/not, named guards) is drawn, not only the
    # flat comparisons — with DSL-style literals and parentheses where they help reading
    text = render(definition_from_dsl(COMPOSITE_GUARDS, "M"))
    assert "A --> B: E\\n[(y > 1 and y < 5) or z == 'go']" in text
    assert "A --> C: E\\n[status == 'paid' and not (n in [1, 2] or retry == true)]" in text
    assert "A --> D: E\\n[(tier == 'gold' or spend > 1000) and region == 'eu']" in text
