"""The pure DSL → Mermaid render behind the editor preview request."""

from harel.lsp.preview import render_text

OK = """
machine M {
  initial A
  state A { on enter mod.a }
  final Done success
  from A to Done on Go
}
"""

TWO = """
machine First { initial A  state A {}  final D success  from A to D on Go }
machine Second { initial X  state X {}  final Y success  from X to Y on Go }
"""

PARSE_ERR = """
machine M {
  initial A
  state A { on enter
}
"""


def test_renders_first_machine() -> None:
    r = render_text(OK)
    assert r.error is None
    assert r.machine == "M"
    assert r.mermaid is not None and r.mermaid.startswith("stateDiagram-v2")
    assert "Done : outcome: success" in r.mermaid


def test_selects_named_machine() -> None:
    assert render_text(TWO, machine="Second").machine == "Second"
    # unknown name falls back to the first declared
    assert render_text(TWO, machine="nope").machine == "First"
    # absent selector → first declared
    assert render_text(TWO).machine == "First"


def test_parse_error_is_located_not_raised() -> None:
    r = render_text(PARSE_ERR)
    assert r.mermaid is None
    assert r.error
    assert r.line is not None


def test_no_machine_no_fragment() -> None:
    r = render_text("event E { }")
    assert r.mermaid is None
    assert "no machine or fragment" in (r.error or "")


FRAGMENT_ONLY = """
fragment Retry(work: action, check: action, base: value) {
  initial Attempt
  state Attempt { on enter work }
  state Done {}
  from Attempt select check {
    "ok"   to Done
    "fail" to Attempt
  }
}
"""

ALL_KINDS = """
fragment F(work: action, give_up: state, ok: guard, budget: value, trig: event) {
  initial Try
  state Try { on enter work  timeout budget }
  state Ok {}
  from Try to Ok on trig where ok
  from Try to give_up on Timeout
}
"""


def test_fragment_only_document_renders_via_wrap() -> None:
    r = render_text(FRAGMENT_ONLY)
    assert r.error is None
    assert r.is_fragment is True
    assert r.machine == "Retry"
    assert r.note and "Retry" in r.note
    # the fragment's own states render inside the synthetic preview machine
    assert "Attempt" in r.mermaid and "<<choice>>" in r.mermaid


def test_fragment_placeholders_for_every_param_kind() -> None:
    r = render_text(ALL_KINDS)
    assert r.error is None and r.is_fragment
    # state param -> a dummy sibling target; event param -> a synthetic kind;
    # value param -> a literal timeout; guard param -> a predicate
    assert "give_up" in r.mermaid
    assert "timeout: 1" in r.mermaid
    assert "TrigEvt" in r.mermaid


def test_named_fragment_selected_over_first() -> None:
    two = FRAGMENT_ONLY + "\nfragment Other(x: value) { initial S  state S {} }"
    assert render_text(two, machine="Other").machine == "Other"
    assert render_text(two).machine == "Retry"  # first fragment by default


TWO_MACHINES = """
machine A { initial S  state S {}  final D success  from S to D on Go }
machine B { initial T  state T {}  final E success  from T to E on Go }
"""


def test_targets_list_machines_and_fragments() -> None:
    r = render_text(TWO_MACHINES + FRAGMENT_ONLY)
    kinds = {(t["name"], t["kind"]) for t in r.targets}
    assert ("A", "machine") in kinds
    assert ("B", "machine") in kinds
    assert ("Retry", "fragment") in kinds


def test_named_machine_renders_and_keeps_targets() -> None:
    r = render_text(TWO_MACHINES, machine="B")
    assert r.machine == "B"
    assert r.mermaid and "[*] --> T" in r.mermaid
    assert any(t["name"] == "A" for t in r.targets)
