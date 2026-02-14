"""Pure DSL analysis for the language server — no LSP dependency.

`analyze(text)` runs the same `parse()` → `definition_from_dsl()` → `validate()`
pipeline an author would, and turns every failure into a `Diagnostic` (1-based
`line`/`column`, a severity and a message). The LSP server (`server.py`) maps
these onto `lsprotocol` ranges; keeping the analysis here means it is testable in
the ordinary suite without `pygls`.

Positions: parse errors and structural `DslError`s carry an exact source position
(the parser stashes `__pos__`; see `dsl.parser`). `validate()` findings reference
a node by its `full_path`, not a source span, so they are reported at the top of
the file with the path named in the message.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from harel.definition.builder import BuildError
from harel.definition.validate import validate
from harel.dsl.loader import definition_with_positions
from harel.dsl.parser import DslError, parse


@dataclass(frozen=True)
class Diagnostic:
    """One editor finding. Positions are 1-based (the server converts to LSP's
    0-based range). The span runs from (line, column) to (end_line, end_column)."""

    line: int
    column: int
    end_line: int
    end_column: int
    severity: str  # "error" | "warning"
    message: str


def _line_len(text: str, line: int) -> int:
    lines = text.splitlines()
    return len(lines[line - 1]) if 1 <= line <= len(lines) else 0


def _from_dslerror(e: DslError, text: str) -> Diagnostic:
    """A located DslError → a diagnostic spanning the rest of the offending line.
    Uses the bare message + hint (not the caret snippet — the editor draws the
    squiggle itself)."""
    line = e.line or 1
    column = e.column or 1
    end_column = max(_line_len(text, line) + 1, column + 1)
    message = e.message if not e.hint else f"{e.message}\nhint: {e.hint}"
    return Diagnostic(line, column, line, end_column, "error", message)


def _from_issue(text: str, code: str, path: str, severity: str, message: str, pos) -> Diagnostic:
    """A validate finding → a diagnostic. `pos` is the source position of the node
    `path` names (from the loader's full_path→pos index); when it falls inside the
    document the squiggle lands on that line, otherwise it anchors at the file head
    (a node spliced from an imported fragment has a position in another file)."""
    where = f" (at {path})" if path else ""
    sev = "error" if severity == "error" else "warning"
    line, column = pos if pos and 1 <= pos[0] <= max(1, len(text.splitlines())) else (1, 1)
    end_column = max(_line_len(text, line) + 1, column + 1)
    return Diagnostic(line, column, line, end_column, sev, f"{code}: {message}{where}")


def analyze(text: str, *, base_path: Optional[Path] = None) -> list[Diagnostic]:
    """Diagnostics for one DSL document. A parse error short-circuits (nothing else
    can run); otherwise every declared machine is built and validated, and each
    validate finding is mapped back to its source state. A file with only fragments
    /events (no machine) yields no diagnostics."""
    try:
        prog = parse(text)
    except DslError as e:
        return [_from_dslerror(e, text)]

    diags: list[Diagnostic] = []
    for name in prog.machines:
        try:
            defn, positions = definition_with_positions(text, name, base_path=base_path)
        except (DslError, BuildError) as e:
            # build/structural failures: DslError is already located; a bare
            # BuildError (no DSL position) lands at the top of the file.
            if isinstance(e, DslError):
                diags.append(_from_dslerror(e, text))
            else:
                diags.append(Diagnostic(1, 1, 1, _line_len(text, 1) + 1, "error", str(e)))
            continue
        for issue in validate(defn):
            diags.append(
                _from_issue(
                    text, issue.code, issue.path, issue.severity, issue.message, positions.get(issue.path)
                )
            )
    return diags
