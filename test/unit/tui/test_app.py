"""The Textual monitor app, driven through Textual's Pilot (`app.run_test()`). Guarded by
`pytest.importorskip("textual")` so the suite still runs without the `tui` extra; the pure
layer (tree/summary/model/resolve) is covered by the unguarded tests in this directory."""

import pytest

pytest.importorskip("textual")

from textual.widgets import Button, DataTable, OptionList, Static, Tree  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import DictStore  # noqa: E402
from harel.tui.app import MonitorApp  # noqa: E402
from harel.tui.model import MonitorModel  # noqa: E402
from harel.tui.resolve import DefinitionSource  # noqa: E402
from harel.tui.screens import ConfirmModal, DetailScreen  # noqa: E402

HIER = """
machine M {
   initial Idle
   state Idle {}
   state Work { initial S1  state S1 {}  state S2 {}  from S1 to S2 on Next }
   from Idle to Work on Go
}
"""
DEFN = definition_from_dsl(HIER, "M")
_S1 = next(p for p, n in DEFN.index.items() if n.name == "S1")


def _model(store, *, resolvable=True):
    source = DefinitionSource(registry={"M": DEFN}, sources={"M": HIER}) if resolvable else DefinitionSource()
    return MonitorModel(store, source)


_S2 = next(p for p, n in DEFN.index.items() if n.name == "S2")


def _seeded_store(*, with_trace=False):
    store = DictStore()
    store.save(Execution(id="run-1", definition_id="M", status=Status.RUNNING, active_path=_S1))
    store.save(Execution(id="run-2", definition_id="M", status=Status.DONE, outcome="success"))
    if with_trace:
        store.append_trace(
            "run-1",
            {
                "index": 0,
                "event_kind": "Go",
                "from_path": "Idle",
                "to_path": _S1,
                "context_in": {},
                "context_out": {"n": 1},
                "actions": ["start_work"],
                "guards": ["ready"],
            },
        )
        store.append_trace(
            "run-1",
            {
                "index": 1,
                "event_kind": "Next",
                "from_path": _S1,
                "to_path": _S2,
                "context_in": {"n": 1},
                "context_out": {"n": 2},
                "actions": ["advance"],
            },
        )
    return store


def _labels(node, acc=None):
    acc = [] if acc is None else acc
    acc.append(str(node.label))
    for c in node.children:
        _labels(c, acc)
    return acc


async def _settle(app, pilot):
    await app.workers.wait_for_complete()
    await pilot.pause()


async def test_list_renders_seeded_executions():
    app = MonitorApp(_model(_seeded_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        assert app.screen.query_one(DataTable).row_count == 2


async def test_filter_narrows_rows():
    app = MonitorApp(_model(_seeded_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        app.screen._filter = "run-2"  # the substring filter (id/definition/status)
        app.screen.action_refresh()
        await _settle(app, pilot)
        assert app.screen.query_one(DataTable).row_count == 1


async def test_enter_opens_detail_with_active_node_highlighted():
    app = MonitorApp(_model(_seeded_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        app.screen.query_one(DataTable).move_cursor(row=0)  # run-1 (sorted by id)
        await pilot.press("enter")
        await _settle(app, pilot)
        assert isinstance(app.screen, DetailScreen)
        labels = _labels(app.screen.query_one(Tree).root)
        assert any("S1" in label for label in labels)  # the active leaf is in the tree


async def test_suspend_then_resume_round_trips_the_store():
    store = _seeded_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")  # open run-1
        await _settle(app, pilot)
        await pilot.press("s")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.SUSPENDED
        await pilot.press("R")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.RUNNING


async def test_terminate_only_mutates_after_confirm():
    store = _seeded_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")
        await _settle(app, pilot)
        # `t` opens the confirm modal; dismissing with `n` must NOT terminate
        await pilot.press("t")
        await pilot.pause()
        assert isinstance(app.screen, ConfirmModal)
        await pilot.press("n")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.RUNNING
        # confirming with `y` terminates
        await pilot.press("t")
        await pilot.pause()
        await pilot.press("y")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.CANCELLED


async def test_timeline_navigation_shows_step_and_marks_tree():
    app = MonitorApp(_model(_seeded_store(with_trace=True)), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")  # open run-1
        await _settle(app, pilot)
        timeline = app.screen.query_one(OptionList)
        assert timeline.option_count == 2  # two seeded steps
        timeline.highlighted = 0  # navigate to the first step
        await pilot.pause()
        detail = str(app.screen.query_one("#step-detail", Static).render())
        assert "start_work" in detail and "ready" in detail  # step's actions/guards
        # the tree marks the navigated step's target node (S1) with the ◀ marker
        labels = _labels(app.screen.query_one(Tree).root)
        assert any("◀" in label for label in labels)


async def test_dsl_source_collapsible_shows_the_machine():
    app = MonitorApp(_model(_seeded_store()), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")
        await _settle(app, pilot)
        from textual.widgets import Collapsible

        assert app.screen.query_one(Collapsible).collapsed is True  # folded by default
        src = str(app.screen.query_one("#source", Static).render())
        assert "machine M" in src  # the .stm source is loaded into the collapsible


async def test_buttons_reflect_status():
    store = _seeded_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")  # run-1 is RUNNING
        await _settle(app, pilot)
        disabled = {b.id: b.disabled for b in app.screen.query(Button)}
        assert disabled["btn-suspend"] is False and disabled["btn-resume"] is True
        # clicking the Suspend button suspends (same action as the `s` key)
        await pilot.click("#btn-suspend")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.SUSPENDED
        disabled = {b.id: b.disabled for b in app.screen.query(Button)}
        assert disabled["btn-suspend"] is True and disabled["btn-resume"] is False


async def test_data_only_when_definition_unresolved_disables_cancel():
    store = _seeded_store()
    app = MonitorApp(_model(store, resolvable=False), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        await pilot.press("enter")
        await _settle(app, pilot)
        assert isinstance(app.screen, DetailScreen)
        assert app.screen._can_cancel is False  # cancel disabled (needs the Definition)
        # the tree degrades to the data-only placeholder (no resolved root states)
        labels = _labels(app.screen.query_one(Tree).root)
        assert any("data-only" in label for label in labels)
        # `c` is a no-op (notifies); status unchanged
        await pilot.press("c")
        await _settle(app, pilot)
        assert store.load("run-1").status is Status.RUNNING


# --- the list: filtered and paged by the store ---------------------------------------


def _tree_store():
    """A root with a region child, a failed root (with context), and a done one."""
    store = DictStore()
    store.save(Execution(id="p", definition_id="M", status=Status.RUNNING, active_path=_S1, children={}))
    store.save(Execution(id="p:Work:0", definition_id="M", status=Status.RUNNING, parent_id="p"))
    store.save(
        Execution(
            id="dead",
            definition_id="M",
            status=Status.FAILED,
            active_path=_S2,
            error="RuntimeError: boom",
            context={"order": 42},
        )
    )
    store.save(Execution(id="done", definition_id="M", status=Status.DONE, outcome="success"))
    return store


def _row_ids(app):
    return [r.value for r in app.screen.query_one(DataTable).rows]


async def test_the_list_shows_roots_and_the_children_on_request():
    app = MonitorApp(_model(_tree_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        assert "p:Work:0" not in _row_ids(app)  # roots only by default
        await pilot.press("a")
        await _settle(app, pilot)
        assert "p:Work:0" in _row_ids(app)


async def test_the_list_filters_by_status_in_the_store():
    app = MonitorApp(_model(_tree_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        await pilot.press("f")  # live
        await _settle(app, pilot)
        assert _row_ids(app) == ["p"]
        await pilot.press("f")  # failed
        await _settle(app, pilot)
        assert _row_ids(app) == ["dead"]


async def test_the_list_pages(monkeypatch):
    from harel.tui import screens

    monkeypatch.setattr(screens, "_PAGE", 2)
    app = MonitorApp(_model(_tree_store()), interval=10.0)
    async with app.run_test() as pilot:
        await _settle(app, pilot)
        first = _row_ids(app)
        assert len(first) == 2
        await pilot.press("n")
        await _settle(app, pilot)
        second = _row_ids(app)
        assert second and not set(first) & set(second)
        await pilot.press("b")
        await _settle(app, pilot)
        assert _row_ids(app) == first


# --- the detail ----------------------------------------------------------------------


async def _open(app, pilot, execution_id):
    table = app.screen.query_one(DataTable)
    table.move_cursor(row=_row_ids(app).index(execution_id))
    await pilot.press("enter")
    await _settle(app, pilot)


async def test_a_failed_execution_shows_its_error_and_can_be_terminated_or_redriven():
    store = _tree_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await _open(app, pilot, "dead")
        assert "RuntimeError: boom" in str(app.screen.query_one("#error", Static).render())
        disabled = {b.id: b.disabled for b in app.screen.query(Button)}
        assert disabled["btn-terminate"] is False and disabled["btn-redrive"] is False
        assert disabled["btn-cancel"] is True and disabled["btn-suspend"] is True

        tree = app.screen.query_one(Tree)
        from harel.tui.widgets import find_node

        tree.move_cursor(find_node(tree, _S1))  # a leaf state to resume from
        await pilot.press("d")
        await pilot.pause()
        assert isinstance(app.screen, ConfirmModal)
        await pilot.press("y")
        await _settle(app, pilot)
        redriven = store.load("dead")
        assert (redriven.status, redriven.active_path, redriven.error) == (Status.RUNNING, _S1, None)


async def test_redrive_needs_a_leaf_selected():
    store = _tree_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await _open(app, pilot, "dead")
        tree = app.screen.query_one(Tree)
        from harel.tui.widgets import find_node

        tree.move_cursor(find_node(tree, next(p for p, n in DEFN.index.items() if n.name == "Work")))
        await pilot.press("d")
        await _settle(app, pilot)
        assert not isinstance(app.screen, ConfirmModal)
        assert store.load("dead").status is Status.FAILED


async def test_a_failed_execution_terminates_to_cancelled():
    store = _tree_store()
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await _open(app, pilot, "dead")
        await pilot.press("t")
        await pilot.pause()
        await pilot.press("y")
        await _settle(app, pilot)
        assert store.load("dead").status is Status.CANCELLED


async def test_the_context_shows_without_a_trace():
    app = MonitorApp(_model(_tree_store()), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await _open(app, pilot, "dead")
        assert "order" in str(app.screen.query_one("#context", Static).render())
        assert "HAREL_TRACE" in str(app.screen.query_one("#step-detail", Static).render())


async def test_children_open_and_u_goes_up_to_the_parent():
    store = _tree_store()
    parent = store.load("p")
    from harel.engine.execution import ChildState

    parent.children = {"p:Work:0": ChildState(root_path="Work")}
    store.save(parent)
    app = MonitorApp(_model(store), interval=10.0)
    async with app.run_test(size=(140, 45)) as pilot:
        await _settle(app, pilot)
        await _open(app, pilot, "p")
        children = app.screen.query_one("#children", DataTable)
        assert [r.value for r in children.rows] == ["p:Work:0"]
        app.screen.query_one("#tabs").active = "tab-children"
        await pilot.pause()
        children.focus()
        await pilot.press("enter")
        await _settle(app, pilot)
        assert isinstance(app.screen, DetailScreen) and app.screen.execution_id == "p:Work:0"
        await pilot.press("u")
        await _settle(app, pilot)
        assert app.screen.execution_id == "p"
