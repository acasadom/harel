"""Unhandled action errors (programming bugs, not modelled failures).

A *controlled* failure is modelled in the statechart (a selector/transition). An
*unhandled* exception is a bug: the production runtime (durable + distributed) must
not propagate it (that would crash the worker) nor retry forever (a deterministic
bug loops) — it fails the execution terminally (status FAILED + the error), acks,
and moves on. The bare Driver (the test harness) still raises so test bugs surface.
"""

import pytest

from harel import engine
from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.runtime import Driver
from harel.engine.store import DictStore, SqliteStore
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

# A.enter is fine; Boom.on_enter raises (an unhandled bug) when we transition to it.
BOOM = """
machine M {
  initial A
  state A { on enter stm_actions.rec(at: "A.enter") }
  state Boom { on enter stm_actions.boom }
  from A to Boom on Go
}
"""


def test_durable_unhandled_error_fails_the_execution(tmp_path):
    store = SqliteStore(tmp_path / "stm.db")
    defn = definition_from_dsl(BOOM, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at A (no error yet)
    assert exe.status is Status.RUNNING

    # Go -> Boom.on_enter raises: the run is failed terminally, NOT propagated
    failed = runner.process(exe.id, Event(kind="Go"))
    assert failed.status is Status.FAILED
    assert failed.error == "RuntimeError: boom"

    # FAILED is terminal: further domain events are ignored (status != RUNNING), no loop
    again = runner.process(exe.id, Event(kind="Go"))
    assert again.status is Status.FAILED
    store.close()


def test_bare_driver_still_raises(tmp_path):
    # the test/scenario harness (bare Driver) must surface bugs, not swallow them
    store = DictStore()
    defn = definition_from_dsl(BOOM, "M")
    driver = Driver(defn, store=store)
    exe = Execution(definition_id=defn.id)
    driver.start(exe)
    with pytest.raises(RuntimeError, match="boom"):
        driver._run(exe, engine.process(defn, exe, Event(kind="Go")))


def test_distributed_worker_survives_an_unhandled_error():
    store = DictStore()
    transport = InMemoryTransport()
    defn = definition_from_dsl(BOOM, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    worker = runner.worker()
    exe = runner.create(defn.id)  # at A

    runner.send(exe.id, Event(kind="Go"))  # -> Boom.on_enter raises
    assert worker.step() is True  # the worker handles it (acks), does NOT crash
    assert store.load(exe.id).status is Status.FAILED
    assert store.load(exe.id).error == "RuntimeError: boom"

    # no infinite loop: the message was acked, the queue is now empty
    assert worker.step() is False


def test_failed_status_round_trips(tmp_path):
    store = SqliteStore(tmp_path / "stm.db")
    defn = definition_from_dsl(BOOM, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))
    reloaded = store.load(exe.id)  # a fresh deserialize from sqlite
    assert reloaded.status is Status.FAILED
    assert reloaded.error == "RuntimeError: boom"
    store.close()


def test_reset_clears_a_failed_error():
    store = DictStore()
    defn = definition_from_dsl(BOOM, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.FAILED

    after = runner.process(exe.id, Event(kind="Reset"))
    assert after.status is Status.RUNNING
    assert after.error is None
    assert after.active_path == "A"
