"""The `.stm` formatter (`harel.fmt.format_text`).

A brace-aware reindenter: canonical 2-space indentation, trimmed trailing space,
collapsed blank lines — preserving comments, line structure and content verbatim,
and idempotent.
"""

from pathlib import Path

from harel.fmt import format_text

DATA = Path(__file__).parents[2] / "data"


def test_reindents_to_two_spaces():
    src = "machine M {\n\t\tinitial A\n        state A {}\n}\n"
    assert format_text(src) == "machine M {\n  initial A\n  state A {}\n}\n"


def test_nested_blocks_indent_by_depth_and_closer_dedents():
    src = "machine M {\nstate Outer {\nstate In {}\n}\n}\n"
    expected = "machine M {\n  state Outer {\n    state In {}\n  }\n}\n"
    assert format_text(src) == expected


def test_preserves_comments_and_inline_content():
    src = "# header\nmachine M {   # trailing\n   state A { on enter f }  // note\n}\n"
    out = format_text(src)
    assert "# header" in out
    assert "  state A { on enter f }  // note" in out  # content verbatim, indent fixed
    assert "machine M {   # trailing" in out  # interior spacing untouched


def test_braces_inside_strings_and_comments_do_not_affect_depth():
    src = 'machine M {\nstate A { on enter f(x: "a { b } c") }  # a } brace\nstate B {}\n}\n'
    out = format_text(src)
    lines = out.splitlines()
    # both states sit at depth 1 despite the braces inside the string / comment
    assert lines[1] == '  state A { on enter f(x: "a { b } c") }  # a } brace'
    assert lines[2] == "  state B {}"


def test_collapses_blank_lines_and_strips_edges():
    src = "\n\nmachine M {\n\n\n  state A {}\n}\n\n\n"
    out = format_text(src)
    assert out == "machine M {\n\n  state A {}\n}\n"


def test_empty_input_is_empty():
    assert format_text("") == ""
    assert format_text("   \n\n  \n") == ""


def test_does_not_reflow_single_line_block():
    src = "machine M { initial A  state A {} }\n"
    assert format_text(src) == src  # one-liners are kept (reindenter, not reflow)


def test_idempotent_on_messy_input():
    src = "machine M {\n\t initial A\n  state A {\non enter f\n}\n   }\n"
    once = format_text(src)
    assert format_text(once) == once


def test_committed_data_files_are_already_canonical():
    # the test machines double as the formatter's golden corpus
    for path in sorted(DATA.glob("*.stm")):
        src = path.read_text()
        assert format_text(src) == src, f"{path.name} is not canonically formatted"
