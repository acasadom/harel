"""`ObservedStore`: every commit's `Step`, told to `on_step` once the commit has returned — what
caused it and how the execution moved, including what a store alone can't tell (the status
before: a step into FAILED, which carries no `finished_at`)."""

import sys
import types

import pytest

from harel import AsyncObservedStore, DictStore, ObservedStore, definition_from_dsl
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event
from harel.testing import assert_listing_contract, assert_outbox_contract, assert_purge_contract

DSL = """
event Go {}
event Boom {}
machine M {
  initial A
  state A { on enter observe_actions.hello }
  state B {}
  state C { on enter observe_actions.boom }
  from A to B on Go
  from B to C on Boom
}
"""


@pytest.fixture(autouse=True)
def actions(monkeypatch):
    def boom(stm, event, **kw):
        raise RuntimeError("boom")

    monkeypatch.setitem(
        sys.modules, "observe_actions", types.SimpleNamespace(hello=lambda stm, event, **kw: None, boom=boom)
    )


def _definitions():
    defn = definition_from_dsl(DSL, "M")
    return {defn.id: defn}


def _seen(steps):
    return [
        (s.cause, s.command or s.event_kind, s.from_status, s.to_status, s.from_path, s.to_path, s.actions)
        for s, _ in steps
    ]


def test_each_commit_is_told_with_its_step():
    steps = []
    store = ObservedStore(DictStore(), lambda step, exe: steps.append((step, exe)))
    runner = DurableRunner(store, _definitions())
    exe = runner.create("M")
    runner.process(exe.id, Event(kind="Go", id="go-1"))
    runner.suspend(exe.id)
    runner.resume(exe.id)
    runner.process(exe.id, Event(kind="Boom", id="boom-1"))

    assert _seen(steps) == [
        ("start", None, Status.PENDING, Status.RUNNING, None, "A", ("observe_actions.hello",)),
        ("event", "Go", Status.RUNNING, Status.RUNNING, "A", "B", ()),
        ("control", "suspend", Status.RUNNING, Status.SUSPENDED, "B", "B", ()),
        ("control", "resume", Status.SUSPENDED, Status.RUNNING, "B", "B", ()),
        ("event", "Boom", Status.RUNNING, Status.FAILED, "B", "C", ()),  # a failed step's effects are dropped
    ]
    assert steps[1][0].event_id == "go-1"
    failed = steps[-1][1]
    assert failed.status is Status.FAILED and failed.finished_at is None  # only the step tells
    assert failed.error == "RuntimeError: boom"  # what failed


def test_the_distributed_create_and_the_workers_start():
    steps = []
    store = ObservedStore(DictStore(), lambda step, exe: steps.append((step, exe)))
    runner = DistributedRunner(store, InMemoryTransport(), _definitions())
    exe = runner.create("M")
    worker = runner.worker()
    while worker.step():
        pass

    (create, _), (start, _) = steps
    assert (create.cause, create.event_kind, create.from_status, create.to_status) == (
        "create",
        "Start",
        Status.PENDING,
        Status.PENDING,
    )
    assert (start.cause, start.event_kind, start.event_id) == ("event", "Start", create.event_id)
    assert (start.from_status, start.to_status, start.to_path) == (Status.PENDING, Status.RUNNING, "A")
    assert store.load(exe.id).active_path == "A"


def test_an_observer_that_raises_doesnt_undo_the_step(caplog):
    def broken(step, exe):
        raise ValueError("observer bug")

    runner = DurableRunner(ObservedStore(DictStore(), broken), _definitions())
    exe = runner.create("M")
    assert runner.process(exe.id, Event(kind="Go")).active_path == "B"
    assert "on_step raised" in caplog.text


def test_the_observer_gets_a_copy():
    def meddle(step, exe):
        exe.context["meddled"] = True

    store = ObservedStore(DictStore(), meddle)
    runner = DurableRunner(store, _definitions())
    exe = runner.create("M")
    assert "meddled" not in runner.process(exe.id, Event(kind="Go")).context


async def test_the_async_store_awaits_a_coroutine_observer():
    steps = []

    async def on_step(step, exe):
        steps.append(step.cause)

    runner = AsyncDurableRunner(AsyncObservedStore(AsyncDictStore(), on_step), _definitions())
    exe = await runner.create("M")
    await runner.process(exe.id, Event(kind="Go"))
    assert steps == ["start", "event"]


def test_it_is_still_the_wrapped_store():
    store = ObservedStore(DictStore(), lambda step, exe: None)
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)


def test_a_forceful_cancel_is_told_as_the_cancel_it_was():
    steps = []
    runner = DurableRunner(ObservedStore(DictStore(), lambda step, exe: steps.append(step)), _definitions())
    exe = runner.create("M")
    runner.cancel(exe.id)  # nothing models `on Cancel`: it terminates
    last = steps[-1]
    assert (last.cause, last.command, last.from_status, last.to_status) == (
        "control",
        "cancel",
        Status.RUNNING,
        Status.CANCELLED,
    )
