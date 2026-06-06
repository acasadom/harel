"""harel.tui — a Textual monitoring TUI for state-machine executions ("k9s for
statecharts"): list executions, drill into one to see the statechart tree with the
active state highlighted plus its data, and drive the control plane by keyboard.

Like `harel.lsp`, the package splits a PURE layer (model/tree/summary/resolve — no
textual, importable and testable anywhere) from the UI layer (`app`, which imports
textual). `main()` lazily imports the UI so this module stays importable without the
`tui` extra installed."""

from harel.tui.model import ExecutionDetail, MonitorModel
from harel.tui.resolve import DefinitionSource
from harel.tui.tree import NodeMark, RegionInfo, TreeModel, TreeNode, build_tree_model

__all__ = [
    "MonitorModel",
    "ExecutionDetail",
    "DefinitionSource",
    "build_tree_model",
    "TreeModel",
    "TreeNode",
    "NodeMark",
    "RegionInfo",
    "main",
]


def main(argv: list[str] | None = None) -> int:
    """Launch the monitor TUI (requires the `tui` extra: textual)."""
    from harel.tui.app import main as _main

    return _main(argv)
