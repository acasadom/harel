"""Smoke test for the pygls wiring (skipped if the `lsp` extra is absent).

The analysis itself is covered in `test_diagnostics`; here we only check the
1-based `Diagnostic` → 0-based LSP range conversion and that the server object
builds with its document features registered.
"""

import pytest

pytest.importorskip("pygls")

from lsprotocol import types as lsp  # noqa: E402

from harel.lsp.diagnostics import Diagnostic  # noqa: E402
from harel.lsp.server import _to_lsp, completion, definition, hover, server  # noqa: E402


class _Doc:
    def __init__(self, source: str) -> None:
        self.source = source


class _Workspace:
    def __init__(self, source: str) -> None:
        self._doc = _Doc(source)

    def get_text_document(self, uri: str) -> _Doc:
        return self._doc


class _LS:
    """Minimal stand-in for the LanguageServer the handlers read documents from."""

    def __init__(self, source: str) -> None:
        self.workspace = _Workspace(source)


SRC = "event Go {}\nmachine M {\n  initial A\n  state A {}\n  state B {}\n  from A to B on Go\n}\n"


def _params(cls, line: int, char: int):
    return cls(
        text_document=lsp.TextDocumentIdentifier(uri="file:///m.stm"),
        position=lsp.Position(line=line, character=char),
    )


def test_to_lsp_converts_to_zero_based_range():
    d = Diagnostic(line=5, column=10, end_line=5, end_column=14, severity="error", message="boom")
    out = _to_lsp(d)
    assert out.range.start == lsp.Position(line=4, character=9)
    assert out.range.end == lsp.Position(line=4, character=13)
    assert out.severity == lsp.DiagnosticSeverity.Error
    assert out.source == "harel"
    assert out.message == "boom"


def test_warning_severity_maps_through():
    out = _to_lsp(Diagnostic(1, 1, 1, 2, "warning", "w"))
    assert out.severity == lsp.DiagnosticSeverity.Warning


def test_server_registers_features():
    features = server.lsp.fm.features
    for feature in (
        lsp.TEXT_DOCUMENT_DID_OPEN,
        lsp.TEXT_DOCUMENT_DID_CHANGE,
        lsp.TEXT_DOCUMENT_HOVER,
        lsp.TEXT_DOCUMENT_DEFINITION,
        lsp.TEXT_DOCUMENT_COMPLETION,
    ):
        assert feature in features


def test_hover_returns_markdown_for_the_event_reference():
    # line 5 (0-based) "  from A to B on Go" — hover the event Go
    out = hover(_LS(SRC), _params(lsp.HoverParams, 5, SRC.splitlines()[5].index("Go")))
    assert out is not None
    assert out.contents.kind == lsp.MarkupKind.Markdown
    assert "event Go" in out.contents.value


def test_definition_jumps_to_the_state_declaration():
    # hover B in `from A to B` -> jump to `  state B {}` (line index 4, col 2)
    out = definition(_LS(SRC), _params(lsp.DefinitionParams, 5, SRC.splitlines()[5].index("B")))
    assert out is not None
    assert out.range.start == lsp.Position(line=4, character=2)


def test_definition_off_a_token_is_none():
    assert definition(_LS(SRC), _params(lsp.DefinitionParams, 5, 0)) is None


def test_completion_in_event_position_offers_events_only():
    # a parseable doc; the cursor on the event token of `on Go` -> events only
    src = (
        "event Go {}\nevent Stop {}\nmachine M {\n  initial A\n  state A {}\n"
        "  state B {}\n  from A to B on Go\n}\n"
    )
    col = src.splitlines()[6].index("Go")
    out = completion(_LS(src), _params(lsp.CompletionParams, 6, col))
    labels = {i.label for i in out.items}
    assert {"Go", "Stop"} <= labels
    assert "machine" not in labels  # keywords suppressed when the category is known and symbols exist


def test_completion_open_context_includes_keywords():
    out = completion(_LS(SRC), _params(lsp.CompletionParams, 2, 0))
    labels = {i.label for i in out.items}
    assert "machine" in labels  # keywords offered when context is open


def test_definition_jumps_into_an_imported_file(tmp_path):
    (tmp_path / "lib.stm").write_text("fragment Frag(x: value) {\n  state S {}\n}\n")
    main = 'import "lib.stm"\nmachine M {\n  initial A\n  state A {}\n  use Frag(x = 1) as F\n}\n'
    (tmp_path / "main.stm").write_text(main)
    uri = (tmp_path / "main.stm").as_uri()

    ls = _LS(main)
    params = lsp.DefinitionParams(
        text_document=lsp.TextDocumentIdentifier(uri=uri),
        position=lsp.Position(line=4, character=main.splitlines()[4].index("Frag")),
    )
    out = definition(ls, params)
    assert out is not None
    assert out.uri == (tmp_path / "lib.stm").as_uri()  # jumps to the imported file
    assert out.range.start.line == 0  # `fragment Frag ...` is line 1 (1-based) -> 0-based 0


def test_definition_is_scope_aware_for_repeated_state_names():
    src = (
        "machine M {\n  initial Outer\n  state Outer {\n    initial Step\n    state Step {}\n"
        "    final Done success\n    from Step to Done on Go\n  }\n  final Done success\n"
        "  from Outer to Done on Fin\n}\n"
    )
    ls = _LS(src)
    # "Done" inside Outer (line 6) -> inner final (line index 5)
    inner = definition(ls, _params(lsp.DefinitionParams, 6, src.splitlines()[6].index("Done")))
    assert inner.range.start.line == 5
    # "Done" at the root (line 9) -> outer final (line index 8)
    outer = definition(ls, _params(lsp.DefinitionParams, 9, src.splitlines()[9].index("Done")))
    assert outer.range.start.line == 8
