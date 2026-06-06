"""The Textual monitor app + its `main()` entry point. The UI layer — the only part of
`harel.tui` that imports textual (mirroring `lsp/server.py`). It owns the `MonitorModel`,
a poll interval, and a clock; the screens (list/detail) do the rendering and actions."""

from __future__ import annotations

import argparse
import contextlib
import os
import time
from typing import Callable, Optional

from textual.app import App
from textual.binding import Binding

from harel.tui.model import MonitorModel
from harel.tui.resolve import DefinitionSource
from harel.tui.screens import ListScreen

_DEFAULT_INTERVAL = 1.0
_MIN_INTERVAL = 0.25


class MonitorApp(App):
    """k9s for statecharts: list executions, drill into one, drive the control plane."""

    CSS = """
    #detail-body { height: 1fr; }
    #statechart { width: 45%; border-right: solid $panel; padding: 0 1; }
    #panels { width: 55%; padding: 0 1; }
    #panels Static { margin-bottom: 1; }
    #filter { dock: top; }
    #confirm-box { width: 60; height: auto; padding: 1 2; border: thick $error; background: $surface; }
    #confirm-box Label { width: 100%; content-align: center middle; margin-bottom: 1; }
    #confirm-buttons { height: auto; align: center middle; }
    #confirm-buttons Button { margin: 0 1; }
    ModalScreen { align: center middle; }
    """

    BINDINGS = [Binding("q", "quit", "quit")]

    def __init__(
        self,
        model: MonitorModel,
        *,
        interval: float = _DEFAULT_INTERVAL,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__()
        self.model = model
        self.interval = max(interval, _MIN_INTERVAL)
        self.clock = clock
        self.title = "harel monitor"

    def on_mount(self) -> None:
        self.push_screen(ListScreen())


def _build_model(args: argparse.Namespace) -> MonitorModel:
    """Build the read model from flags/env: the store via the worker's backend factory,
    and a Definition source from a directory of `.stm` files (if given)."""
    from harel.worker import build_store

    store = build_store()
    defs_dir = args.definitions_dir or os.environ.get("STM_DEFINITIONS_DIR")
    source = DefinitionSource.from_dir(defs_dir) if defs_dir else DefinitionSource.empty()
    return MonitorModel(store, source)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="harel monitor", description="Monitor statechart executions in a TUI."
    )
    parser.add_argument(
        "--definitions-dir",
        help="directory of .stm files to resolve definitions for the statechart view "
        "(default: $STM_DEFINITIONS_DIR). Without it the monitor runs data-only.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("STM_TUI_INTERVAL_MS", "1000")) / 1000.0,
        help="auto-refresh interval in seconds (default 1.0; $STM_TUI_INTERVAL_MS).",
    )
    args = parser.parse_args(argv)

    model = _build_model(args)
    app = MonitorApp(model, interval=args.interval)
    try:
        app.run()
    finally:
        # the store may hold a connection (postgres/redis/…); release it on exit
        with contextlib.suppress(Exception):
            model.close()
    return 0
