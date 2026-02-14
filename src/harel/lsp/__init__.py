"""DSL language server (editor diagnostics).

`analyze` is the pure, dependency-free analysis (parse + validate → diagnostics);
`main` starts the `pygls` server over stdio (needs the `lsp` extra). Importing
`main` is lazy so `analyze` stays usable without `pygls` installed.
"""

from harel.lsp.diagnostics import Diagnostic, analyze

__all__ = ["analyze", "Diagnostic", "main"]


def main() -> None:
    """Start the language server over stdio (requires the `lsp` extra)."""
    from harel.lsp.server import main as _main

    _main()
