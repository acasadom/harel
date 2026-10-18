"""The monitor's screens: a list of executions (auto-refreshing, filtered and paged by the
store), a detail view (the statechart tree, the execution's data in tabs, its children and the
control-plane actions), and a confirm modal for the destructive actions. All store I/O runs in
thread workers so the UI never blocks."""

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
    TabbedContent,
    TabPane,
    Tree,
)
from textual.widgets.option_list import Option

from harel.engine.execution import Status
from harel.tui import summary, widgets

if TYPE_CHECKING:
    from harel.tui.app import MonitorApp

_PAGE = 100  # executions per page of the list

# the list's status filter, cycled with `f`: a name and the statuses it asks the store for
_STATUS_FILTERS: tuple[tuple[str, Optional[list[Status]]], ...] = (
    ("any", None),
    ("live", [Status.PENDING, Status.RUNNING, Status.SUSPENDED, Status.CANCELLING]),
    ("failed", [Status.FAILED]),
    ("done", [Status.DONE]),
    ("cancelled", [Status.CANCELLED]),
)
_FINISHED = (Status.DONE, Status.CANCELLED)


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
    """The execution list: a DataTable auto-refreshed on a timer, a page at a time. The store
    filters it — by status (`f` cycles any/live/failed/done/cancelled) and to the roots of
    their trees unless `a` shows the children too — and pages it (`n` next, `b` back); `/`
    narrows the page shown by a substring of id/machine/status. `enter` opens one."""

    BINDINGS = [
        Binding("slash", "filter", "filter"),
        Binding("f", "cycle_status", "status"),
        Binding("a", "toggle_children", "children"),
        Binding("n", "next_page", "next page"),
        Binding("b", "previous_page", "prev page"),
        Binding("w", "queues", "queues & workers"),
        Binding("r", "refresh", "refresh"),
        Binding("p", "toggle_poll", "pause/resume"),
        Binding("escape", "clear_filter", "clear filter", show=False),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._filter = ""
        self._timer: Optional[Timer] = None
        self._paused = False
        self._status = 0  # index into _STATUS_FILTERS
        self._roots_only = True
        self._cursors: list[Optional[str]] = [None]  # the cursor of each page reached so far
        self._page = 0
        self._next: Optional[str] = None  # the store's cursor for the page after this one

    def compose(self) -> ComposeResult:
        yield Header()
        yield Input(placeholder="narrow this page: id / machine / status…", id="filter")
        yield DataTable(id="executions", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#executions", DataTable)
        table.add_columns("ID", "MACHINE", "STATUS", "OUTCOME", "ACTIVE", "WRITTEN")
        table.focus()  # so `enter`/arrows reach the table without a click
        self.query_one("#filter", Input).display = False
        self._timer = self.set_interval(self.monitor.interval, self.action_refresh)
        self.action_refresh()

    # --- refresh (store I/O off the UI thread) ---------------------------------------

    def action_refresh(self) -> None:
        self._fetch_list()

    @work(exclusive=True, thread=True, group="list")
    def _fetch_list(self) -> None:
        page = self.monitor.model.list_executions(
            status=_STATUS_FILTERS[self._status][1],
            roots_only=self._roots_only,
            limit=_PAGE,
            cursor=self._cursors[self._page],
        )
        self.app.call_from_thread(self._apply_list, page.items, page.next_cursor)

    def _apply_list(self, items, next_cursor: Optional[str]) -> None:
        self._next = next_cursor
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
        now = self.monitor.clock()
        table.clear()
        for s in rows:
            table.add_row(
                s.id,
                summary.machine(s.definition_id, s.definition_version),
                f"[{summary.status_color(s.status)}]{summary.status_label(s.status)}[/]",
                summary.verdict(s),
                summary.short_path(s.active_path, 28),
                summary.age(s.updated_at, now),
                key=s.id,
            )
        if sel is not None and table.row_count:
            table.move_cursor(row=min(sel, table.row_count - 1))
        bits = [
            f"{len(rows)} shown",
            f"page {self._page + 1}" + (" (more: n)" if next_cursor else ""),
            "roots" if self._roots_only else "roots + children",
            f"status: {_STATUS_FILTERS[self._status][0]}",
        ]
        if self._filter:
            bits.append(f"narrowed: {self._filter!r}")
        if self._paused:
            bits.append("paused")
        self.sub_title = " · ".join(bits)

    # --- actions ---------------------------------------------------------------------

    def _restart(self) -> None:
        """Back to the first page: a new filter makes the page cursors meaningless."""
        self._cursors, self._page, self._next = [None], 0, None
        self.action_refresh()

    def action_cycle_status(self) -> None:
        self._status = (self._status + 1) % len(_STATUS_FILTERS)
        self._restart()

    def action_toggle_children(self) -> None:
        self._roots_only = not self._roots_only
        self._restart()

    def action_next_page(self) -> None:
        if self._next is None:
            self.notify("this is the last page")
            return
        del self._cursors[self._page + 1 :]
        self._cursors.append(self._next)
        self._page += 1
        self.action_refresh()

    def action_previous_page(self) -> None:
        if self._page == 0:
            self.notify("this is the first page")
            return
        self._page -= 1
        self.action_refresh()

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

    def action_queues(self) -> None:
        self.app.push_screen(QueuesScreen())


class QueuesScreen(_MonitorScreen):
    """The work queued, by machine, and the workers: for each machine, how many messages wait,
    for how many executions, how many of those a worker holds, how long the oldest has waited,
    and which live workers claim it — a machine with work waiting and none is marked. Below,
    every worker's heartbeat: the machines it claims, when it started, when it was last seen;
    `x` forgets the selected one when it is gone (a worker that crashed never removes its own)."""

    BINDINGS = [
        Binding("escape,h", "back", "back"),
        Binding("r", "refresh", "refresh"),
        Binding("x", "forget_worker", "forget gone worker"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._gone: set[str] = set()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(id="queues-note")
        yield Label("Queued work, by machine", classes="section")
        yield DataTable(id="queues", cursor_type="row", zebra_stripes=True)
        yield Label("Workers", classes="section")
        yield DataTable(id="workers", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = "queues & workers"
        self.query_one("#queues", DataTable).add_columns(
            "MACHINE", "WAITING", "EXECUTIONS", "HELD", "OLDEST WAIT", "WORKERS"
        )
        self.query_one("#workers", DataTable).add_columns(
            "WORKER", "MACHINES", "STARTED", "LAST SEEN", "STATE"
        )
        # reading the backlog walks the whole queue: refreshed on its own, slower, interval
        self.set_interval(self.monitor.queue_interval, self.action_refresh)
        self.action_refresh()

    def action_refresh(self) -> None:
        self._fetch()

    @work(exclusive=True, thread=True, group="queues")
    def _fetch(self) -> None:
        now = self.monitor.clock()
        try:
            view = self.monitor.model.queues(now)
        except Exception as exc:  # noqa: BLE001 — an unreachable backend is a note, not a crash
            self.app.call_from_thread(self._apply_error, exc)
            return
        self.app.call_from_thread(self._apply, view, now)

    def _apply_error(self, exc: Exception) -> None:
        self.query_one("#queues-note", Static).update(f"[red]could not read the queues: {exc}[/]")

    def _apply(self, view, now: float) -> None:
        notes = []
        if view.unavailable:
            notes.append(f"[yellow]no backlog: {view.unavailable}[/] — workers only")
        elif not view.by_machine:
            notes.append("[dim]this transport counts its whole queue, not by machine[/]")
        unserved = [r for r in view.rows if r.unserved]
        if unserved:
            names = ", ".join(r.machine or "(no machine)" for r in unserved)
            notes.append(f"[b red]⚠ work waiting that no live worker claims: {names}[/]")
        self.query_one("#queues-note", Static).update("\n".join(notes))

        def count(n) -> str:
            return "—" if n is None else str(n)

        table = self.query_one("#queues", DataTable)
        table.clear()
        for r in view.rows:
            machine = r.machine or ("(all)" if not view.by_machine else "(no machine)")
            workers = (
                ", ".join(r.workers) if r.workers else ("[b red]none[/]" if r.messages else "[dim]none[/]")
            )
            table.add_row(
                f"[b red]⚠ {machine}[/]" if r.unserved else machine,
                str(r.messages),
                count(r.executions),
                count(r.held),
                summary.age(r.oldest_published_at, now) if r.oldest_published_at is not None else "—",
                workers,
            )
        table = self.query_one("#workers", DataTable)
        sel = table.cursor_row
        table.clear()
        self._gone = {w.worker_id for w in view.workers if not w.alive(now)}
        for w in view.workers:
            alive = w.alive(now)
            table.add_row(
                w.worker_id,
                ", ".join(w.machines) if w.machines is not None else "all",
                f"{summary.age(w.started_at, now)} ago",
                f"{summary.age(w.seen_at, now)} ago",
                "[#5fd75f]live[/]" if alive else "[dim]gone[/]",
                key=w.worker_id,
            )
        if sel is not None and table.row_count:
            table.move_cursor(row=min(sel, table.row_count - 1))

    def action_forget_worker(self) -> None:
        table = self.query_one("#workers", DataTable)
        if not table.row_count:
            return
        worker_id = table.coordinate_to_cell_key(table.cursor_coordinate).row_key.value
        if worker_id not in self._gone:
            self.notify("only a gone worker is forgotten — a live one keeps beating", severity="warning")
            return

        def then(ok: Optional[bool]) -> None:
            if ok:
                self._forget(worker_id)

        self.app.push_screen(ConfirmModal(f"Forget worker {worker_id}?"), then)

    @work(thread=True, group="control")
    def _forget(self, worker_id: str) -> None:
        try:
            self.monitor.model.forget_worker(worker_id)
        except Exception as exc:  # noqa: BLE001 — a failure is a toast, not a crash
            self.app.call_from_thread(self.notify, f"forget failed: {exc}", severity="error")
        self.app.call_from_thread(self.action_refresh)

    def action_back(self) -> None:
        self.app.pop_screen()


class DetailScreen(_MonitorScreen):
    """One execution. On the left, the statechart tree with the active state highlighted (and
    the machine's DSL source, folded). On the right, its header — status, machine version,
    tree and parent, error — the control-plane buttons, and tabs: its **timeline** (when the
    runners record one), its **context** and history, what is **pending** for it (timers,
    inbound events, spawns) and its **children** (`enter` opens one; `u` goes up to the
    parent). Redrive (`d`) brings a failed execution back at the leaf state selected in the
    tree."""

    BINDINGS = [
        Binding("escape,h", "back", "back"),
        Binding("r", "refresh", "refresh"),
        Binding("u", "parent", "parent"),
        Binding("s", "suspend", "suspend"),
        Binding("R", "resume", "resume"),
        Binding("c", "cancel", "cancel"),
        Binding("t", "terminate", "terminate"),
        Binding("d", "redrive", "redrive"),
    ]

    def __init__(self, execution_id: str) -> None:
        super().__init__()
        self.execution_id = execution_id
        self._can_cancel = True
        self._can_redrive = False
        self._parent_id: Optional[str] = None
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
                yield Static(id="error")
                with Horizontal(id="actions"):
                    yield Button("Suspend", id="btn-suspend")
                    yield Button("Resume", id="btn-resume")
                    yield Button("Cancel", id="btn-cancel")
                    yield Button("Terminate", id="btn-terminate")
                    yield Button("Redrive", id="btn-redrive")
                with TabbedContent(id="tabs"):
                    with TabPane("Timeline", id="tab-timeline"):
                        yield OptionList(id="timeline")
                        with VerticalScroll(id="step"):
                            yield Static(id="step-detail")
                    with TabPane("Context", id="tab-context"):
                        with VerticalScroll():
                            yield Static(id="context")
                    with TabPane("Pending", id="tab-pending"):
                        with VerticalScroll():
                            yield Static(id="pending")
                    with TabPane("Children", id="tab-children"):
                        yield DataTable(id="children", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = self.execution_id
        self.query_one("#statechart", Tree).border_title = "statechart"
        self.query_one("#timeline", OptionList).border_title = "timeline (↑/↓ to navigate)"
        self.query_one("#step", VerticalScroll).border_title = "step detail"
        self.query_one("#children", DataTable).add_columns("CHILD", "KIND", "BRANCH", "STATE")
        self.query_one("#error", Static).display = False
        self.set_interval(self.monitor.interval, self.action_refresh)
        self.action_refresh()

    def action_refresh(self) -> None:
        self._fetch_detail()

    @work(exclusive=True, thread=True, group="detail")
    def _fetch_detail(self) -> None:
        model = self.monitor.model
        detail = model.detail(self.execution_id)
        can_cancel = model.can_cancel(self.execution_id) if detail else False
        can_redrive = model.can_redrive(self.execution_id) if detail else False
        self.app.call_from_thread(self._apply_detail, detail, can_cancel, can_redrive)

    def _apply_detail(self, detail, can_cancel: bool, can_redrive: bool) -> None:
        if detail is None:
            self.notify("execution no longer exists", severity="warning")
            self.app.pop_screen()
            return
        exe = detail.execution
        now = self.monitor.clock()
        self._can_cancel, self._can_redrive, self._parent_id = can_cancel, can_redrive, exe.parent_id
        self._tree_model = detail.tree
        self._populate_tree(self._selected_step_path())
        self.query_one("#source", Static).update(
            detail.source or "[dim](source unavailable: no file given, and not published in the store)[/]"
        )
        self.query_one("#status-header", Static).update(widgets.status_header_markup(detail, now))
        error = self.query_one("#error", Static)
        error.display = bool(exe.error)
        if exe.error:
            error.update(widgets.error_markup(detail))
        self._sync_buttons(exe.status)
        self.query_one("#context", Static).update(
            widgets.context_markup(detail) + "\n\n[b]history[/]\n" + widgets.history_markup(detail)
        )
        self.query_one("#pending", Static).update(widgets.pending_markup(detail, now))
        self._apply_children(detail.children)
        self._apply_timeline(detail.trace)
        banner = "" if detail.tree.resolved else "  [yellow](data-only: definition unavailable)[/]"
        self.sub_title = f"{self.execution_id}{banner}"

    def _apply_children(self, children) -> None:
        table = self.query_one("#children", DataTable)
        sel = table.cursor_row
        table.clear()
        for child in children:
            state = f"✓ {child.outcome or 'done'}" if child.finished else "…"
            table.add_row(child.id, child.kind, child.root_path or "—", state, key=child.id)
        if sel is not None and table.row_count:
            table.move_cursor(row=min(sel, table.row_count - 1))
        self.query_one("#tab-children", TabPane).disabled = not children

    def _apply_timeline(self, trace) -> None:
        timeline = self.query_one("#timeline", OptionList)
        keep = timeline.highlighted  # preserve the navigated step across a refresh
        timeline.clear_options()
        self._steps = list(trace)
        for step in self._steps:
            timeline.add_option(Option(step.title(), id=str(step.index)))
        if self._steps:
            timeline.highlighted = (
                keep if keep is not None and keep < len(self._steps) else len(self._steps) - 1
            )
            self._show_step(timeline.highlighted)
        else:
            self.query_one("#step-detail", Static).update(
                "[dim](no timeline — the runners record one when tracing is on: HAREL_TRACE=1, or "
                "trace=True; the Context tab shows the execution's data either way)[/]"
            )

    def _selected_step_path(self) -> Optional[str]:
        timeline = self.query_one("#timeline", OptionList)
        index = timeline.highlighted
        if index is None or not (0 <= index < len(self._steps)):
            return None
        return self._steps[index].to_path

    def _populate_tree(self, selected_step: Optional[str]) -> None:
        """Re-render the tree, keeping the node the cursor was on (a redrive target)."""
        tree = self.query_one("#statechart", Tree)
        if self._tree_model is None:
            return
        cursor = tree.cursor_node.data if tree.cursor_node is not None else None
        widgets.populate_statechart(tree, self._tree_model, selected_step)
        if cursor is not None:
            node = widgets.find_node(tree, cursor)
            if node is not None:
                tree.move_cursor(node)

    def _sync_buttons(self, status) -> None:
        """Enable each control button only where it applies: Suspend on a running execution,
        Resume on a suspended one, Cancel on a live one whose definition resolves, Terminate on
        any unfinished one — a failed one included, which it closes — and Redrive on a failed
        one whose definition resolves."""
        self.query_one("#btn-suspend", Button).disabled = status is not Status.RUNNING
        self.query_one("#btn-resume", Button).disabled = status is not Status.SUSPENDED
        self.query_one("#btn-cancel", Button).disabled = (
            status in _FINISHED or status is Status.FAILED or not self._can_cancel
        )
        self.query_one("#btn-terminate", Button).disabled = status in _FINISHED
        self.query_one("#btn-redrive", Button).disabled = not self._can_redrive

    def _show_step(self, index: Optional[int]) -> None:
        """Render the selected timeline step and highlight its target node in the tree."""
        if index is None or not (0 <= index < len(self._steps)):
            return
        step = self._steps[index]
        self.query_one("#step-detail", Static).update(widgets.step_markup(step))
        self._populate_tree(step.to_path)

    def on_option_list_option_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        self._show_step(event.option_index)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.row_key is not None and event.row_key.value:
            self.app.push_screen(DetailScreen(event.row_key.value))

    # --- control plane (buttons + keys share the same actions) ------------------------

    def on_button_pressed(self, event: Button.Pressed) -> None:
        handler = {
            "btn-suspend": self.action_suspend,
            "btn-resume": self.action_resume,
            "btn-cancel": self.action_cancel,
            "btn-terminate": self.action_terminate,
            "btn-redrive": self.action_redrive,
        }.get(event.button.id or "")
        if handler is not None:
            handler()

    def action_back(self) -> None:
        self.app.pop_screen()

    def action_parent(self) -> None:
        if self._parent_id is None:
            self.notify("this execution is the root of its tree")
            return
        self.app.push_screen(DetailScreen(self._parent_id))

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

    def action_redrive(self) -> None:
        if not self._can_redrive:
            self.notify(
                "redrive brings back a failed execution whose definition is available", severity="warning"
            )
            return
        node = self.query_one("#statechart", Tree).cursor_node
        if node is None or node.data is None or node.allow_expand:
            self.notify("select a leaf state in the tree to redrive to", severity="warning")
            return
        target = node.data

        def then(ok: Optional[bool]) -> None:
            if ok:
                self._control("redrive", target)

        self.app.push_screen(ConfirmModal(f"Redrive {self.execution_id} at {target}?"), then)

    def _confirm(self, action: str, question: str) -> None:
        def then(ok: Optional[bool]) -> None:
            if ok:
                self._control(action)

        self.app.push_screen(ConfirmModal(question), then)

    @work(thread=True, group="control")
    def _control(self, action: str, *args: str) -> None:
        try:
            getattr(self.monitor.model, action)(self.execution_id, *args)
        except Exception as exc:  # noqa: BLE001 — a control failure is a toast, not a crash
            self.app.call_from_thread(self.notify, f"{action} failed: {exc}", severity="error")
        else:
            self.app.call_from_thread(self.notify, f"{action} ok")
        self.app.call_from_thread(self.action_refresh)
