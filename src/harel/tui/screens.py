"""The monitor's screens: a list of executions (auto-refreshing), a detail view (the
statechart tree + data panels + control-plane actions), and a confirm modal for the
destructive actions. All store I/O runs in thread workers so the UI never blocks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, cast

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.timer import Timer
from textual.widgets import (
    Button,
    Collapsible,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    OptionList,
    Static,
    Tree,
)
from textual.widgets.option_list import Option

from harel.engine.execution import Status
from harel.tui import summary, widgets

if TYPE_CHECKING:
    from harel.tui.app import MonitorApp

# the statuses the list shows by default (the "live" ones first); the filter narrows further
_LIST_LIMIT = 200


class _MonitorScreen(Screen):
    """Base for the monitor screens: a typed handle to the `MonitorApp` (its model,
    clock, interval) — `self.app` is the generic `App`, so cast once here."""

    @property
    def monitor(self) -> "MonitorApp":
        return cast("MonitorApp", self.app)


class ConfirmModal(ModalScreen[bool]):
    """A yes/no confirmation for a destructive action. Returns True only on confirm."""

    BINDINGS = [
        Binding("y", "confirm", "yes"),
        Binding("n,escape", "cancel", "no"),
    ]

    def __init__(self, question: str) -> None:
        super().__init__()
        self._question = question

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-box"):
            yield Label(self._question, id="confirm-q")
            with Horizontal(id="confirm-buttons"):
                yield Button("Yes (y)", variant="error", id="yes")
                yield Button("No (n)", variant="primary", id="no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "yes")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class ListScreen(_MonitorScreen):
    """The execution list: a DataTable auto-refreshed on a timer. `/` filters by a free
    substring (id/definition/status), `enter` opens the detail, `p` pauses refresh."""

    BINDINGS = [
        Binding("slash", "filter", "filter"),
        Binding("r", "refresh", "refresh"),
        Binding("p", "toggle_poll", "pause/resume"),
        Binding("escape", "clear_filter", "clear filter", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._filter = ""
        self._timer: Optional[Timer] = None
        self._paused = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield Input(placeholder="filter: id / definition / status…", id="filter")
        yield DataTable(id="executions", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#executions", DataTable)
        table.add_columns("ID", "DEFINITION", "STATUS", "OUTCOME", "ACTIVE", "VER")
        table.focus()  # so `enter`/arrows reach the table without a click
        self.query_one("#filter", Input).display = False
        self._timer = self.set_interval(self.monitor.interval, self.action_refresh)
        self.action_refresh()

    # --- refresh (store I/O off the UI thread) ---------------------------------------

    def action_refresh(self) -> None:
        self._fetch_list()

    @work(exclusive=True, thread=True, group="list")
    def _fetch_list(self) -> None:
        page = self.monitor.model.list_executions(limit=_LIST_LIMIT)
        self.app.call_from_thread(self._apply_list, page.items)

    def _apply_list(self, items) -> None:
        table = self.query_one("#executions", DataTable)
        sel = table.cursor_row
        flt = self._filter.lower()
        rows = [
            s
            for s in items
            if not flt
            or flt in s.id.lower()
            or flt in s.definition_id.lower()
            or flt in s.status.value.lower()
        ]
        table.clear()
        for s in rows:
            table.add_row(
                s.id[:12],
                summary.truncate(s.definition_id, 20),
                f"[{summary.status_color(s.status)}]{summary.status_label(s.status)}[/]",
                summary.verdict(s),
                summary.short_path(s.active_path, 28),
                str(s.version),
                key=s.id,
            )
        if sel is not None and table.row_count:
            table.move_cursor(row=min(sel, table.row_count - 1))
        self.sub_title = f"{len(rows)} executions" + (" (paused)" if self._paused else "")

    # --- actions ---------------------------------------------------------------------

    def action_filter(self) -> None:
        inp = self.query_one("#filter", Input)
        inp.display = True
        inp.focus()

    def action_clear_filter(self) -> None:
        self._filter = ""
        inp = self.query_one("#filter", Input)
        inp.value = ""
        inp.display = False
        self.query_one("#executions", DataTable).focus()
        self.action_refresh()

    def on_input_changed(self, event: Input.Changed) -> None:
        self._filter = event.value
        self.action_refresh()

    def action_toggle_poll(self) -> None:
        if self._timer is None:
            return
        self._paused = not self._paused
        if self._paused:
            self._timer.pause()
        else:
            self._timer.resume()
        self.action_refresh()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key is not None and event.row_key.value:
            self.app.push_screen(DetailScreen(event.row_key.value))


class DetailScreen(_MonitorScreen):
    """One execution: the statechart tree (active state highlighted) on the left; on the
    right a navigable **execution timeline** over a per-step detail (event in, transition,
    actions/guards, context in → out). Navigating a step highlights its node in the tree.
    A row of control-plane buttons mirrors the keyboard actions, enabled per status."""

    BINDINGS = [
        Binding("escape,h", "back", "back"),
        Binding("r", "refresh", "refresh"),
        Binding("s", "suspend", "suspend"),
        Binding("R", "resume", "resume"),
        Binding("c", "cancel", "cancel"),
        Binding("t", "terminate", "terminate"),
    ]

    def __init__(self, execution_id: str) -> None:
        super().__init__()
        self.execution_id = execution_id
        self._can_cancel = True
        self._steps: list = []  # TraceStep list, indexed by the OptionList option index
        self._tree_model = None  # cached so navigating the timeline re-highlights cheaply

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="detail-body"):
            with Vertical(id="left"):
                yield Tree("statechart", id="statechart")
                with Collapsible(title="DSL source", collapsed=True, id="source-box"):
                    with VerticalScroll(id="source-scroll"):
                        yield Static(id="source")
            with Vertical(id="right"):
                yield Static(id="status-header")
                with Horizontal(id="actions"):
                    yield Button("Suspend", id="btn-suspend")
                    yield Button("Resume", id="btn-resume")
                    yield Button("Cancel", id="btn-cancel")
                    yield Button("Terminate", id="btn-terminate")
                yield Label("Timeline", classes="section")
                yield OptionList(id="timeline")
                with VerticalScroll(id="step"):
                    yield Static(id="step-detail")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = self.execution_id
        self.query_one("#statechart", Tree).border_title = "statechart"
        self.query_one("#timeline", OptionList).border_title = "timeline (↑/↓ to navigate)"
        self.query_one("#step", VerticalScroll).border_title = "step detail"
        self.set_interval(self.monitor.interval, self.action_refresh)
        self.action_refresh()

    def action_refresh(self) -> None:
        self._fetch_detail()

    @work(exclusive=True, thread=True, group="detail")
    def _fetch_detail(self) -> None:
        detail = self.monitor.model.detail(self.execution_id)
        can_cancel = self.monitor.model.can_cancel(self.execution_id) if detail else False
        self.app.call_from_thread(self._apply_detail, detail, can_cancel)

    def _apply_detail(self, detail, can_cancel: bool) -> None:
        if detail is None:
            self.notify("execution no longer exists", severity="warning")
            self.app.pop_screen()
            return
        self._can_cancel = can_cancel
        self._tree_model = detail.tree
        widgets.populate_statechart(self.query_one("#statechart", Tree), detail.tree)
        self.query_one("#source", Static).update(
            detail.source or "[dim](source unavailable — pass --definitions-dir)[/]"
        )
        self.query_one("#status-header", Static).update(widgets.status_header_markup(detail))
        self._sync_buttons(detail.execution.status)

        timeline = self.query_one("#timeline", OptionList)
        keep = timeline.highlighted  # preserve the navigated step across a refresh
        timeline.clear_options()
        self._steps = list(detail.trace)
        for step in self._steps:
            timeline.add_option(Option(step.title(), id=str(step.index)))
        if self._steps:
            timeline.highlighted = (
                keep if keep is not None and keep < len(self._steps) else len(self._steps) - 1
            )
            self._show_step(timeline.highlighted)
        else:
            self.query_one("#step-detail", Static).update(
                "[dim](no execution trace — step-by-step recording is a future engine "
                "feature; this timeline fills in once a trace is present)[/]"
            )
        banner = "" if detail.tree.resolved else "  [yellow](data-only: definition unavailable)[/]"
        self.sub_title = f"{self.execution_id}{banner}"

    def _sync_buttons(self, status) -> None:
        """Enable each control button only where it applies (Resume needs SUSPENDED,
        Suspend needs RUNNING, cancel needs a resolvable Definition, …)."""
        terminal = status in (Status.DONE, Status.CANCELLED, Status.FAILED)
        self.query_one("#btn-suspend", Button).disabled = status is not Status.RUNNING
        self.query_one("#btn-resume", Button).disabled = status is not Status.SUSPENDED
        self.query_one("#btn-cancel", Button).disabled = terminal or not self._can_cancel
        self.query_one("#btn-terminate", Button).disabled = terminal

    def _show_step(self, index: Optional[int]) -> None:
        """Render the selected timeline step and highlight its target node in the tree."""
        if index is None or not (0 <= index < len(self._steps)):
            return
        step = self._steps[index]
        self.query_one("#step-detail", Static).update(widgets.step_markup(step))
        if self._tree_model is not None:
            widgets.populate_statechart(self.query_one("#statechart", Tree), self._tree_model, step.to_path)

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        self._show_step(event.option_index)

    # --- control plane (buttons + keys share the same actions) ------------------------

    def on_button_pressed(self, event: Button.Pressed) -> None:
        handler = {
            "btn-suspend": self.action_suspend,
            "btn-resume": self.action_resume,
            "btn-cancel": self.action_cancel,
            "btn-terminate": self.action_terminate,
        }.get(event.button.id or "")
        if handler is not None:
            handler()

    def action_back(self) -> None:
        self.app.pop_screen()

    def action_suspend(self) -> None:
        self._control("suspend")

    def action_resume(self) -> None:
        self._control("resume")

    def action_terminate(self) -> None:
        self._confirm("terminate", f"Terminate {self.execution_id}? (forceful)")

    def action_cancel(self) -> None:
        if not self._can_cancel:
            self.notify("cancel needs the definition (unavailable) — use terminate", severity="warning")
            return
        self._confirm("cancel", f"Cancel {self.execution_id}?")

    def _confirm(self, action: str, question: str) -> None:
        def then(ok: Optional[bool]) -> None:
            if ok:
                self._control(action)

        self.app.push_screen(ConfirmModal(question), then)

    @work(thread=True, group="control")
    def _control(self, action: str) -> None:
        try:
            getattr(self.monitor.model, action)(self.execution_id)
        except Exception as exc:  # noqa: BLE001 — a control failure is a toast, not a crash
            self.app.call_from_thread(self.notify, f"{action} failed: {exc}", severity="error")
        else:
            self.app.call_from_thread(self.notify, f"{action} ok")
        self.app.call_from_thread(self.action_refresh)
