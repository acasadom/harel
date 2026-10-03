"""The sync runners' (and the bare `Driver`'s) execution models: `execution="background"` (the shared background loop)
and `execution="inline"` (the caller's own thread, no event loop), and the action-error
policies (`on_action_error="fail"` / `"raise"`). The whole suite also runs with inline as
the default (`pytest --execution=inline`); these pin down what only inline guarantees."""

import sys
import threading
import types

import pytest

from harel import DictStore, Event, SqliteStore, definition_from_dsl
from harel.engine.aio_store import AsyncDictStore
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.runtime import Driver
from harel.engine.transport import SqliteTransport

DSL = """
event Go {}
event Stop {}
machine M {
  initial A
  state A {}
  state B { on enter execution_models_actions.act }
  final C success {}
  from A to B on Go
  from B to C on Stop
}
"""


@pytest.fixture
def actions(monkeypatch):
    mod = types.SimpleNamespace(threads=[], boom=False)

    def act(stm, event, **kw):
        mod.threads.append(threading.current_thread())
        if stm.execution_ctx.get("boom"):
            raise RuntimeError("boom")

    mod.act = act
    monkeypatch.setitem(sys.modules, "execution_models_actions", mod)
    return mod


class ThreadBoundStore(DictStore):
    """A store that, like a Django connection, may only be used from the thread that made it."""

    def __init__(self) -> None:
        super().__init__()
        self.owner = threading.current_thread()

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if callable(attr) and not name.startswith("_"):
            if threading.current_thread() is not object.__getattribute__(self, "owner"):
                raise AssertionError(f"store.{name} called from another thread")
        return attr


def _defn():
    return definition_from_dsl(DSL, "M")


def test_inline_runs_the_store_and_the_actions_in_the_callers_thread(actions):
    defn = _defn()
    runner = DurableRunner(ThreadBoundStore(), {defn.id: defn}, execution="inline")
    exe = runner.create(defn.id)
    exe = runner.process(exe.id, Event(kind="Go"))
    assert exe.active_path == "B"
    assert actions.threads == [threading.current_thread()]


def test_background_runs_them_off_the_callers_thread(actions):
    defn = _defn()
    with pytest.raises(AssertionError, match="another thread"):
        DurableRunner(ThreadBoundStore(), {defn.id: defn}, execution="background").create(defn.id)


def test_raise_leaves_the_step_uncommitted(tmp_path, actions):
    defn = _defn()
    store = SqliteStore(tmp_path / "stm.db")  # serializes, so the stored copy is what was committed
    runner = DurableRunner(store, {defn.id: defn}, execution="inline", on_action_error="raise")
    exe = runner.create(defn.id, context={"boom": True})
    with pytest.raises(RuntimeError, match="boom"):
        runner.process(exe.id, Event(kind="Go"))
    stored = store.load(exe.id)
    assert (stored.active_path, stored.status, stored.version) == ("A", Status.RUNNING, 1)
    store.close()


@pytest.mark.parametrize("execution", ["background", "inline"])
def test_fail_dead_letters_the_execution(execution, actions):
    defn = _defn()
    runner = DurableRunner(DictStore(), {defn.id: defn}, execution=execution)
    exe = runner.create(defn.id, context={"boom": True})
    exe = runner.process(exe.id, Event(kind="Go"))
    assert exe.status is Status.FAILED and exe.error == "RuntimeError: boom"


def test_raise_with_the_background_loop_too(actions):
    defn = _defn()
    runner = DurableRunner(DictStore(), {defn.id: defn}, execution="background", on_action_error="raise")
    exe = runner.create(defn.id, context={"boom": True})
    with pytest.raises(RuntimeError, match="boom"):
        runner.process(exe.id, Event(kind="Go"))


def test_inline_refuses_an_async_store_and_a_coroutine_action(monkeypatch):
    defn = _defn()
    with pytest.raises(TypeError, match="needs a sync store"):
        DurableRunner(AsyncDictStore(), {defn.id: defn}, execution="inline")

    async def act(stm, event, **kw):
        return None

    monkeypatch.setitem(sys.modules, "execution_models_actions", types.SimpleNamespace(act=act))
    runner = DurableRunner(DictStore(), {defn.id: defn}, execution="inline")
    exe = runner.process(runner.create(defn.id).id, Event(kind="Go"))
    assert exe.status is Status.FAILED and "coroutine function" in exe.error


def test_the_choices_are_checked():
    defn = _defn()
    with pytest.raises(ValueError, match="execution must be one of"):
        DurableRunner(DictStore(), {defn.id: defn}, execution="threads")
    with pytest.raises(ValueError, match="on_action_error must be one of"):
        DurableRunner(DictStore(), {defn.id: defn}, on_action_error="ignore")


def test_a_distributed_runner_and_its_worker_inline(tmp_path, actions):
    defn = _defn()
    store, transport = SqliteStore(tmp_path / "s.db"), SqliteTransport(tmp_path / "q.db")
    runner = DistributedRunner(store, transport, {defn.id: defn}, execution="inline")
    worker = runner.worker()
    assert worker.execution == "inline"

    exe = runner.create(defn.id)
    while worker.step():
        pass
    runner.send(exe.id, Event(kind="Go"))
    while worker.step():
        pass
    runner.suspend(exe.id)
    assert store.load(exe.id).status is Status.SUSPENDED
    runner.resume(exe.id)
    runner.send(exe.id, Event(kind="Stop"))
    while worker.step():
        pass
    final = store.load(exe.id)
    assert (final.active_path, final.status, final.outcome) == ("C", Status.DONE, "success")
    assert actions.threads == [threading.current_thread()]  # the worker ran the action right here
    store.close()
    transport.close()


def test_the_bare_driver_inline(actions):
    defn = _defn()
    store = ThreadBoundStore()
    driver = Driver(defn, store, execution="inline")
    exe = Execution(definition_id=defn.id)
    driver.register(exe)
    driver.start(exe)
    driver.inject(exe, Event(kind="Go"))
    assert exe.active_path == "B" and driver.get(exe.id).active_path == "B"
    assert actions.threads == [threading.current_thread()]


def test_the_bare_driver_inline_defaults_to_a_dict_store_and_propagates_action_errors(actions):
    defn = _defn()
    driver = Driver(defn, execution="inline")
    exe = Execution(definition_id=defn.id, context={"boom": True})
    driver.start(exe)
    with pytest.raises(RuntimeError, match="boom"):
        driver.inject(exe, Event(kind="Go"))


def test_the_bare_driver_inline_refuses_an_async_store():
    defn = _defn()
    with pytest.raises(TypeError, match="needs a sync store"):
        Driver(defn, AsyncDictStore(), execution="inline")
    with pytest.raises(ValueError, match="execution must be one of"):
        Driver(defn, execution="threads")
