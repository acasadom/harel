"""The Textual monitor app, driven through Textual's Pilot (`app.run_test()`). Guarded by
`pytest.importorskip("textual")` so the suite still runs without the `tui` extra; the pure
layer (tree/summary/model/resolve) is covered by the unguarded tests in this directory."""

import pytest

pytest.importorskip("textual")

from textual.widgets import DataTable, Tree  # noqa: E402

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
    return MonitorModel(store, DefinitionSource(registry={"M": DEFN} if resolvable else {}))


def _seeded_store():
    store = DictStore()
    store.save(Execution(id="run-1", definition_id="M", status=Status.RUNNING, active_path=_S1))
    store.save(Execution(id="run-2", definition_id="M", status=Status.DONE, outcome="success"))
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
