"""A Language Server for the statechart DSL (`.stm`).

Thin wiring around the pure analyses: `diagnostics.analyze` (on open / change /
save → publishDiagnostics) and `symbols` (hover, go-to-definition, completion).
Requires the `lsp` extra (`pygls`); run it as `python -m harel.lsp` (or the
`harel-lsp` script). The VSCode extension in `editor/vscode/` launches it over
stdio. All feature logic reuses the engine's own parser + validator.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from lsprotocol import types as lsp
from pygls.server import LanguageServer
from pygls.uris import to_fs_path

from harel.lsp.diagnostics import Diagnostic, analyze
from harel.lsp.preview import render_text
from harel.lsp.symbols import (
    KEYWORDS,
    Symbol,
    SymbolIndex,
    WordRef,
    category_at,
    import_at,
    index,
    resolve_state,
    word_at,
)

server = LanguageServer("harel-lsp", "v0.1")

_SEVERITY = {
    "error": lsp.DiagnosticSeverity.Error,
    "warning": lsp.DiagnosticSeverity.Warning,
}

_COMPLETION_KIND = {
    "state": lsp.CompletionItemKind.Class,
    "event": lsp.CompletionItemKind.Event,
    "guard": lsp.CompletionItemKind.Variable,
    "fragment": lsp.CompletionItemKind.Module,
    "machine": lsp.CompletionItemKind.Class,
}


def _to_lsp(d: Diagnostic) -> lsp.Diagnostic:
    """A 1-based `Diagnostic` → an LSP diagnostic (0-based range)."""
    return lsp.Diagnostic(
        range=lsp.Range(
            start=lsp.Position(line=d.line - 1, character=d.column - 1),
            end=lsp.Position(line=d.end_line - 1, character=d.end_column - 1),
        ),
        severity=_SEVERITY.get(d.severity, lsp.DiagnosticSeverity.Error),
        source="harel",
        message=d.message,
    )


def _publish(ls: LanguageServer, uri: str, text: str) -> None:
    fs_path = to_fs_path(uri)
    base = Path(fs_path).parent if fs_path else None
    diags = [_to_lsp(d) for d in analyze(text, base_path=base)]
    ls.publish_diagnostics(uri, diags)


@server.feature(lsp.TEXT_DOCUMENT_DID_OPEN)
def did_open(ls: LanguageServer, params: lsp.DidOpenTextDocumentParams) -> None:
    _publish(ls, params.text_document.uri, params.text_document.text)


@server.feature(lsp.TEXT_DOCUMENT_DID_CHANGE)
def did_change(ls: LanguageServer, params: lsp.DidChangeTextDocumentParams) -> None:
    doc = ls.workspace.get_text_document(params.text_document.uri)
    _publish(ls, params.text_document.uri, doc.source)


@server.feature(lsp.TEXT_DOCUMENT_DID_SAVE)
def did_save(ls: LanguageServer, params: lsp.DidSaveTextDocumentParams) -> None:
    doc = ls.workspace.get_text_document(params.text_document.uri)
    _publish(ls, params.text_document.uri, doc.source)


def _symbol_range(sym: Symbol) -> lsp.Range:
    start = lsp.Position(line=sym.line - 1, character=sym.column - 1)
    end = lsp.Position(line=sym.line - 1, character=sym.column - 1 + len(sym.name))
    return lsp.Range(start=start, end=end)


def _doc_base(uri: str) -> Optional[Path]:
    fs_path = to_fs_path(uri)
    return Path(fs_path).parent if fs_path else None


def _resolve(idx: SymbolIndex, text: str, ref: WordRef) -> Optional[Symbol]:
    """Resolve the reference under the cursor to a declaration. State references are
    resolved scope-aware (the engine's relative resolution); a repeated name falls
    back to the first match. Other kinds resolve by name + category."""
    if ref.category == "state":
        return resolve_state(idx, text, ref.line, ref.start, ref.word) or idx.lookup(ref.word, "state")
    return idx.lookup(ref.word, ref.category)


@server.feature(lsp.TEXT_DOCUMENT_HOVER)
def hover(ls: LanguageServer, params: lsp.HoverParams):
    uri = params.text_document.uri
    doc = ls.workspace.get_text_document(uri)
    ref = word_at(doc.source, params.position.line, params.position.character)
    if ref is None:
        return None
    sym = _resolve(index(doc.source, base_path=_doc_base(uri), uri=uri), doc.source, ref)
    if sym is None:
        return None
    rng = lsp.Range(
        start=lsp.Position(line=ref.line, character=ref.start),
        end=lsp.Position(line=ref.line, character=ref.end),
    )
    return lsp.Hover(contents=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=sym.detail), range=rng)


@server.feature(lsp.TEXT_DOCUMENT_DEFINITION)
def definition(ls: LanguageServer, params: lsp.DefinitionParams):
    uri = params.text_document.uri
    doc = ls.workspace.get_text_document(uri)
    # a click on an `import` path/alias opens the imported file (at its start)
    rel = import_at(doc.source, params.position.line, params.position.character)
    if rel is not None:
        base = _doc_base(uri)
        resolved = (base / rel) if base is not None else Path(rel)
        if not resolved.exists():
            return None
        start = lsp.Range(start=lsp.Position(line=0, character=0), end=lsp.Position(line=0, character=0))
        return lsp.Location(uri=resolved.resolve().as_uri(), range=start)
    ref = word_at(doc.source, params.position.line, params.position.character)
    if ref is None:
        return None
    sym = _resolve(index(doc.source, base_path=_doc_base(uri), uri=uri), doc.source, ref)
    if sym is None:
        return None
    # a symbol pulled from an import carries its own file uri; local ones use this doc
    return lsp.Location(uri=sym.uri or uri, range=_symbol_range(sym))


@server.feature(lsp.TEXT_DOCUMENT_COMPLETION)
def completion(ls: LanguageServer, params: lsp.CompletionParams) -> lsp.CompletionList:
    uri = params.text_document.uri
    doc = ls.workspace.get_text_document(uri)
    # infer the category from the line prefix (works even when not on a word yet)
    category = category_at(doc.source, params.position.line, params.position.character)
    idx = index(doc.source, base_path=_doc_base(uri), uri=uri)
    items = [
        lsp.CompletionItem(
            label=s.name,
            kind=_COMPLETION_KIND.get(s.kind, lsp.CompletionItemKind.Text),
            detail=s.kind,
            documentation=lsp.MarkupContent(kind=lsp.MarkupKind.Markdown, value=s.detail),
        )
        for s in idx.names(category)
    ]
    # offer keywords when the context is open, or as a fallback when no symbols
    # apply (e.g. the document is mid-edit and does not currently parse)
    if category is None or not items:
        items += [lsp.CompletionItem(label=k, kind=lsp.CompletionItemKind.Keyword) for k in KEYWORDS]
    return lsp.CompletionList(is_incomplete=False, items=items)


def _param(params: object, key: str) -> object:
    """Read one field from a custom-request params object (a dict or an attr bag)."""
    if isinstance(params, dict):
        return params.get(key)
    return getattr(params, key, None)


@server.feature("harel/render")
def render_preview(ls: LanguageServer, params: object) -> dict:
    """Custom request: render the document at `uri` to a Mermaid `stateDiagram-v2`
    for the live preview. Returns `{mermaid}` on success or `{error, line, column}`
    so the webview can report a parse error without dropping the last good diagram.
    Uses the in-memory buffer (the unsaved text), and resolves imports against the
    document's directory."""
    uri = _param(params, "uri")
    if not isinstance(uri, str):
        return {"error": "no document uri"}
    doc = ls.workspace.get_text_document(uri)
    machine = _param(params, "machine")
    result = render_text(
        doc.source,
        base_path=_doc_base(uri),
        machine=machine if isinstance(machine, str) else None,
    )
    return {
        "mermaid": result.mermaid,
        "machine": result.machine,
        "error": result.error,
        "line": result.line,
        "column": result.column,
        "isFragment": result.is_fragment,
        "note": result.note,
        "targets": result.targets,
    }


def main() -> None:
    """Entry point (`harel-lsp` / `python -m harel.lsp`): serve over stdio."""
    server.start_io()


if __name__ == "__main__":
    main()
