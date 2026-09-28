"""Creating an Execution is not starting it — for `DistributedRunner` only.

`DurableRunner` is a synchronous, single-process host: running the initial
`on enter` inline on `create()` is simply what it's for, and stays unchanged.

`DistributedRunner.create()` never runs a model's actions inline on whoever
called it — a web handler, a script, anything. Its actions run on whichever
worker claims the published `Start`, never on the caller — the entire point of
a distributed host is that the work reaches a worker.

`create()` defaults to `start_on_create=True`: it publishes `Start` itself, in the
same call, so there's no window where the record exists but nothing will ever
claim it. `start_on_create=False` reopens that window deliberately, for a caller
that has a reason to delay starting; `start(execution_id)` is there for that case.
A domain event addressed to a not-yet-started execution is discarded on arrival
(only `RUNNING` processes domain events) rather than parked — parking it would
hold the execution's single-active-consumer group lock and block `Start` itself
from ever being claimed behind it.
"""

import logging

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

FLAT = """
event Go {}
machine M {
   initial A
   state A { on enter stm_actions.rec(at: "A.enter") }
   state B { on enter stm_actions.rec(at: "B.enter") }
   from A to B on Go
}
"""


def _drain(worker):
    while worker.step():
        pass


def test_durable_create_still_starts_inline():
    """Unchanged: DurableRunner is a synchronous single-process host."""
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DurableRunner(store, {defn.id: defn})

    exe = runner.create(defn.id)

    assert exe.status is Status.RUNNING
    assert exe.active_path == "A"
    assert exe.context["trace"] == ["A.enter"]


def test_distributed_create_publishes_start_by_default():
    """The returned Execution is still PENDING (create() never mutates it further
    itself — a worker does, later), but Start has already been published: a worker
    picks it up with no further action from the caller."""
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

    exe = runner.create(defn.id)
    assert exe.status is Status.PENDING
    assert exe.active_path is None

    _drain(runner.worker())
    started = store.load(exe.id)
    assert started.status is Status.RUNNING
    assert started.active_path == "A"
    assert started.context["trace"] == ["A.enter"]


def test_distributed_create_does_not_start_when_opted_out():
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

    exe = runner.create(defn.id, start_on_create=False)

    assert exe.status is Status.PENDING
    assert exe.active_path is None
    assert exe.context == {}  # on_enter never ran — nothing to run it in this process
    assert runner.worker().step() is False  # nothing published — nothing to claim
    assert store.load(exe.id).status is Status.PENDING  # still, since nothing was ever sent


def test_distributed_start_runs_on_the_worker_not_the_caller():
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})
    exe = runner.create(defn.id, start_on_create=False)

    runner.start(exe.id)
    # start() only published the event — no worker has run yet
    still_pending = store.load(exe.id)
    assert still_pending.status is Status.PENDING
    assert still_pending.active_path is None

    _drain(runner.worker())
    started = store.load(exe.id)
    assert started.status is Status.RUNNING
    assert started.active_path == "A"
    assert started.context["trace"] == ["A.enter"]


def test_starting_an_already_started_execution_warns_and_is_a_noop(caplog):
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})
    exe = runner.create(defn.id)  # start_on_create=True (default)
    _drain(runner.worker())
    assert store.load(exe.id).status is Status.RUNNING

    with caplog.at_level(logging.WARNING):
        runner.start(exe.id)  # already RUNNING — must not republish/reset

    assert "already" in caplog.text
    unchanged = store.load(exe.id)
    assert unchanged.status is Status.RUNNING
    assert unchanged.active_path == "A"
    assert unchanged.context["trace"] == ["A.enter"]  # not re-run


def test_distributed_create_rejects_an_unknown_definition_id():
    """create() validates definition_id eagerly, synchronously, before persisting
    anything: a bad id must fail fast for the caller, not silently produce a
    PENDING execution that only fails later, asynchronously, on whichever worker
    claims its Start."""
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

    with pytest.raises(KeyError):
        runner.create("no-such-definition-id")


def test_an_event_sent_right_after_create_is_not_lost_behind_start():
    """Start and a fast-following domain event both land in the same transport
    group. Start must win regardless of timing — the default start_on_create=True
    publishes it first, in the same call as create(), so there's no window for a
    domain event to queue-jump ahead of it."""
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

    exe = runner.create(defn.id)  # publishes Start immediately
    runner.send(exe.id, Event(kind="Go"))  # right behind it, same group

    _drain(runner.worker())
    final = store.load(exe.id)
    assert final.active_path == "B"
    assert final.context["trace"] == ["A.enter", "B.enter"]


def test_an_event_sent_before_the_deferred_start_is_discarded_not_lost_later():
    """With start_on_create=False, a domain event addressed to the still-PENDING
    execution is discarded on arrival (see AsyncWorker._handle's PENDING branch) —
    not parked (parking would deadlock the group against Start itself) and not
    recorded as processed either, so a caller-level retry with a fresh publish of
    the same logical event succeeds normally once the execution has started."""
    store = DictStore()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})
    exe = runner.create(defn.id, start_on_create=False)

    go = Event(kind="Go")
    runner.send(exe.id, go)  # arrives while PENDING -> discarded
    _drain(runner.worker())
    assert store.load(exe.id).status is Status.PENDING  # discarded, not queued

    runner.start(exe.id)
    runner.send(exe.id, Event(kind="Go"))  # a fresh publish of the same logical event
    _drain(runner.worker())
    final = store.load(exe.id)
    assert final.active_path == "B"
    assert final.context["trace"] == ["A.enter", "B.enter"]
