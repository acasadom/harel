"""A machine changed under a running execution — a state it is parked in was renamed or
removed. The engine would hit a node that isn't there; the runner checks first and, by its
policy, fails the execution with `DefinitionChanged` (a hosted runner) or raises it (the bare
driver, `on_action_error="raise"`). Before, the engine raised a `KeyError`, which a worker
logged and retried for ever. The event is consumed with the dead letter; `redrive` brings the
execution back to a state the new definition has."""

import pytest

from harel import DefinitionChanged, DictStore, Event, definition_from_dsl
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.runtime import Driver
from harel.engine.transport import InMemoryTransport

V1 = """
event Go {}
event Ok {}
machine order {
  initial Draft
  state Draft {}
  state Review {}
  final Done success
  from Draft to Review on Go
  from Review to Done on Ok
}
"""
V2 = V1.replace("Review", "Checking")  # a state renamed


def _defs(source):
    defn = definition_from_dsl(source, "order")
    return {defn.id: defn}


def _parked_in_review(store, runner_cls=DurableRunner, **kwargs):
    runner = runner_cls(store, _defs(V1), **kwargs)
    exe = runner.create("order")
    assert runner.process(exe.id, Event(kind="Go")).active_path == "Review"
    return exe.id


def test_a_hosted_runner_fails_the_execution_with_a_clear_error():
    store = DictStore()
    exe_id = _parked_in_review(store)
    runner = DurableRunner(store, _defs(V2))  # V2 deployed over the same store

    exe = runner.process(exe_id, Event(kind="Ok", id="ok-1"))

    assert exe.status is Status.FAILED
    assert exe.active_path == "Review"  # where it was left
    assert exe.error.startswith("DefinitionChanged: ")
    assert "'Review'" in exe.error and "'order'" in exe.error and "redrive" in exe.error
    assert store.is_processed(exe_id, "ok-1")  # consumed with the dead letter, not retried


def test_redrive_brings_it_back_into_the_new_definition():
    store = DictStore()
    exe_id = _parked_in_review(store)
    runner = DurableRunner(store, _defs(V2))
    runner.process(exe_id, Event(kind="Ok"))

    exe = runner.redrive(exe_id, "Checking")
    assert (exe.status, exe.active_path, exe.error) == (Status.RUNNING, "Checking", None)
    exe = runner.process(exe_id, Event(kind="Ok"))
    assert (exe.status, exe.outcome) == (Status.DONE, "success")


def test_under_raise_the_error_reaches_the_caller_and_nothing_commits():
    store = DictStore()
    exe_id = _parked_in_review(store)
    version = store.load(exe_id).version
    runner = DurableRunner(store, _defs(V2), on_action_error="raise")

    with pytest.raises(DefinitionChanged, match="no longer has"):
        runner.process(exe_id, Event(kind="Ok", id="ok-1"))

    stored = store.load(exe_id)
    assert (stored.status, stored.version) == (Status.RUNNING, version)
    assert not store.is_processed(exe_id, "ok-1")


def test_the_bare_driver_raises():
    store = DictStore()
    exe_id = _parked_in_review(store)
    driver = Driver(_defs(V2)["order"], store)
    with pytest.raises(DefinitionChanged):
        driver.inject(store.load(exe_id), Event(kind="Ok"))


async def test_the_async_runner_fails_it_too():
    store = AsyncDictStore()
    runner = AsyncDurableRunner(store, _defs(V1))
    exe = await runner.create("order")
    await runner.process(exe.id, Event(kind="Go"))
    exe = await AsyncDurableRunner(store, _defs(V2)).process(exe.id, Event(kind="Ok"))
    assert exe.status is Status.FAILED and exe.error.startswith("DefinitionChanged: ")


def test_a_worker_fails_it_once_instead_of_retrying_for_ever():
    store, transport = DictStore(), InMemoryTransport()
    old = DistributedRunner(store, transport, _defs(V1))
    exe = old.create("order")
    worker = old.worker()
    while worker.step():
        pass
    old.send(exe.id, Event(kind="Go"))
    while worker.step():
        pass
    assert store.load(exe.id).active_path == "Review"

    new = DistributedRunner(store, transport, _defs(V2))
    new.send(exe.id, Event(kind="Ok"))
    new_worker = new.worker()
    assert new_worker.step() is True  # handled: the event was consumed...
    assert new_worker.step() is False  # ...and nothing is left to come back
    stored = store.load(exe.id)
    assert stored.status is Status.FAILED and stored.error.startswith("DefinitionChanged: ")


def test_a_compatible_change_leaves_a_running_execution_alone():
    store = DictStore()
    exe_id = _parked_in_review(store)
    v2 = V1.replace("state Draft {}", "state Draft {}\n  state Spare {}")  # a state added, none removed
    exe = DurableRunner(store, _defs(v2)).process(exe_id, Event(kind="Ok"))
    assert (exe.status, exe.outcome) == (Status.DONE, "success")


def test_finished_and_suspended_executions_are_not_failed_by_it():
    store = DictStore()
    exe_id = _parked_in_review(store)
    runner = DurableRunner(store, _defs(V2))
    runner.suspend(exe_id)
    assert runner.process(exe_id, Event(kind="Ok")).status is Status.SUSPENDED  # not ours to fail
    runner.resume(exe_id)

    done_id = _parked_in_review(store)
    DurableRunner(store, _defs(V1)).process(done_id, Event(kind="Ok"))  # finishes on V1
    assert DurableRunner(store, _defs(V2)).process(done_id, Event(kind="Ok")).status is Status.DONE


def test_cancel_terminates_an_execution_whose_state_is_gone():
    store = DictStore()
    exe_id = _parked_in_review(store)
    exe = DurableRunner(store, _defs(V2)).cancel(exe_id)  # no `KeyError` looking for a handler
    assert exe.status is Status.CANCELLED
