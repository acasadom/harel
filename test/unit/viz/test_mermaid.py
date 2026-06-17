"""Mermaid (`stateDiagram-v2`) rendering of a `Definition`.

The output is asserted at the line level for each construct (composite nesting,
orthogonal regions, selector choice, sinks, labels). Mermaid validity itself is
checked separately by the editor probe; here we pin the *shape* of the text.
"""

from pathlib import Path

from harel.dsl import definition_from_dsl, definition_from_dsl_file, parse
from harel.viz.mermaid import render

DATA = Path(__file__).parents[2] / "data"

FLAT_NESTED = """
machine M {
  initial Start
  state Start { on enter mod.enter }
  state Processing {
    initial Sim
    state Sim { on enter mod.sim }
    state Val { on enter mod.val }
    from Sim to Val on Notification where status == "Success"
  }
  state End { on enter mod.end }
  from Start to Processing
  from Processing to End on Done
}
"""

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

SELECTOR = """
machine M {
  initial Pick
  state Pick { on enter mod.pick }
  state Ok {}
  state No {}
  from Pick select mod.choose {
    "yes" to Ok
    "no"  to No
    else  to No
  }
}
"""

TIMED = """
machine M {
  initial Wait
  state Wait { on enter mod.go  timeout 30 }
  final Done success
  from Wait to Done on Tick
}
"""


def _lines(text: str, name: str) -> list[str]:
    return [line.strip() for line in render(definition_from_dsl(text, name)).split("\n")]


def test_header_first_line() -> None:
    assert _lines(FLAT_NESTED, "M")[0] == "stateDiagram-v2"


def test_initial_and_leaf_description() -> None:
    lines = _lines(FLAT_NESTED, "M")
    assert "[*] --> Start" in lines
    assert "Start : on enter: enter" in lines


def test_nested_composite_block() -> None:
    lines = _lines(FLAT_NESTED, "M")
    assert "state Processing {" in lines
    # the inner initial points at the path-addressed child id
    assert "[*] --> Processing_Sim" in lines
    assert 'state "Sim" as Processing_Sim' in lines


def test_event_predicate_label() -> None:
    lines = _lines(FLAT_NESTED, "M")
    assert "Processing_Sim --> Processing_Val : Notification<br/>[status == 'Success']" in lines


def test_sink_to_final_pseudostate() -> None:
    # End has no outgoing transition at the top scope → final pseudostate
    assert "End --> [*]" in _lines(FLAT_NESTED, "M")


def test_orthogonal_regions_split_by_double_dash() -> None:
    lines = _lines(ORTHOGONAL, "M")
    assert "--" in lines  # the Mermaid concurrency divider
    assert 'state "A" as Fork_A {' in lines
    assert 'state "B" as Fork_B {' in lines
    assert 'state "A1" as Fork_A_A1' in lines


def test_selector_choice() -> None:
    lines = _lines(SELECTOR, "M")
    assert "state Pick__choose <<choice>>" in lines
    assert "Pick --> Pick__choose" in lines
    assert "Pick__choose --> Ok : choose=yes" in lines
    assert "Pick__choose --> No : choose=no" in lines
    assert "Pick__choose --> No : else" in lines


def test_timeout_and_outcome_on_leaf() -> None:
    lines = _lines(TIMED, "M")
    assert "Wait : timeout: 30" in lines
    assert "Done : outcome: success" in lines


def test_corpus_renders_with_header() -> None:
    """Every machine in the example corpus renders (smoke)."""
    seen = 0
    for path in sorted(DATA.glob("*.stm")):
        for name in parse(path.read_text()).machines:
            out = render(definition_from_dsl_file(path, name))
            assert out.startswith("stateDiagram-v2\n")
            seen += 1
    assert seen >= 5
