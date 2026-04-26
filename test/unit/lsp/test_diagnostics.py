"""`harel.lsp.analyze` — the pure DSL analysis behind the language server.

Asserts the diagnostic each failure class produces: parse errors and structural
errors with an exact 1-based position, validate findings at the file head, and a
clean document → no diagnostics.
"""

import pytest

from harel.lsp import Diagnostic, analyze


def test_clean_document_has_no_diagnostics():
    assert analyze("machine M {\n  initial A\n  final A done\n}\n") == []


def test_lib_file_without_machine_is_clean():
    # only fragments / events: nothing to validate, no error
    assert analyze("event Go { n: int }\nfragment F(x: value) {\n  state S {}\n}\n") == []


def test_syntax_error_diagnostic_is_located():
    (d,) = analyze("machine M {\n  initial A\n  state A {}\n  state B {}\n  from A -> B\n}\n")
    assert d.severity == "error"
    assert (d.line, d.column) == (5, 10)
    assert d.end_column > d.column  # spans the rest of the line


def test_structural_error_carries_position_and_hint_in_message():
    (d,) = analyze("machine M {\n  initial A\n  state A {}\n  from A to Nope\n}\n")
    assert d.line == 4
    assert "cannot resolve transition target 'Nope'" in d.message


def test_unbound_handler_anchored_at_machine():
    (d,) = analyze("machine M {\n  initial A\n  state A { on enter go }\n}\n")
    assert d.line == 1
    assert "unbound action handlers: go" in d.message


def test_validate_finding_is_mapped_to_its_source_state():
    # a terminal sink with no `outcome` is a validate *error*, squiggled on the
    # state's own line (the loader's full_path->pos index), with the path in the message
    diags = analyze("machine M {\n  initial A\n  state A {}\n}\n")
    issue = next(d for d in diags if "outcome" in d.message)
    assert issue.severity == "error"
    assert issue.line == 3  # `  state A {}`
    assert issue.column == 3
    assert "(at A)" in issue.message


def test_unreachable_state_warning_points_at_the_state():
    diags = analyze(
        "machine M {\n  initial A\n  state A {}\n  state Z {}\n"
        "  final Done success\n  from A to Done on Go\n}\n"
    )
    warn = next(d for d in diags if "unreachable" in d.message)
    assert warn.severity == "warning"
    assert warn.line == 4  # `  state Z {}`
    assert "(at Z)" in warn.message


def test_diagnostic_is_frozen_dataclass():
    d = Diagnostic(1, 1, 1, 2, "error", "x")
    with pytest.raises(Exception):
        d.line = 9  # type: ignore[misc]
