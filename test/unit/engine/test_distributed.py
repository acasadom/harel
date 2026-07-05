"""Wiring tests for distributed execution (single worker, deterministic drain).

These prove the store + transport + worker pipeline end-to-end without thread
timing: one worker drains the queue by calling `step()` until it is empty. A flat
machine advances on an event; an orthogonal machine fans the event out to its
regions, each region's `Finished` travels back through the transport, and the
parent joins. The genuinely-concurrent multi-thread variant lives in
test/integration. Both store/transport backends are exercised.
"""

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore, SqliteStore
from harel.engine.transport import InMemoryTransport, SqliteTransport
from harel.spec.states import Event

FLAT = """
machine M {
   initial A
   state A { on enter stm_actions.rec(at: "A.enter") }
   state B { on enter stm_actions.rec(at: "B.enter") }
   state C { on enter stm_actions.rec(at: "C.enter") }
   from A to B
   from B to C on Go
}
"""

ORTHO = """
machine M {
   initial Fork
   orthogonal Fork {
      state A {
         initial A1
         state A1 { on enter stm_actions.rec(at: "A1") }
         state A2 { on enter stm_actions.rec(at: "A2") }
         from A1 to A2 on Go
      }
      state B {
         initial B1
         state B1 { on enter stm_actions.rec(at: "B1") }
         state B2 { on enter stm_actions.rec(at: "B2") }
         from B1 to B2 on Go
      }
   }
   state Done { on enter stm_actions.rec(at: "Done") }
   from Fork to Done
}
"""


@pytest.fixture(params=["memory", "sqlite"])
def backend(request, tmp_path):
    if request.param == "memory":
        yield DictStore(), InMemoryTransport()
    else:
        store = SqliteStore(tmp_path / "stm.db")
        transport = SqliteTransport(tmp_path / "q.db")
        yield store, transport
        store.close()
        transport.close()


def _drain(worker):
    while worker.step():
        pass


def test_flat_advances_through_the_transport(backend):
    store, transport = backend
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})

    exe = runner.create(defn.id)  # A -> B inline, parked at B
    assert exe.active_path == "B"

    runner.send(exe.id, Event(kind="Go"))
    _drain(runner.worker())

    final = store.load(exe.id)
    assert final.active_path == "C"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


def test_orthogonal_fans_out_and_joins_through_the_transport(backend):
    store, transport = backend
    defn = definition_from_dsl(ORTHO, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})

    exe = runner.create(defn.id)  # fork: two regions parked, parent at Fork
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    assert len(child_ids) == 2

    # Go is published to the parent, fanned out to both regions; each region
    # finishes and its Finished returns through the transport; the parent joins.
    runner.send(exe.id, Event(kind="Go"))
    _drain(runner.worker())

    final = store.load(exe.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["Done"]

    children = [store.load(cid) for cid in child_ids]
    assert all(c is not None and c.status is Status.DONE for c in children)
    assert sorted(c.context["trace"] for c in children) == [["A1", "A2"], ["B1", "B2"]]


def test_duplicate_send_is_processed_once(backend):
    store, transport = backend
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)

    go = Event(kind="Go")  # same id both times
    runner.send(exe.id, go)
    runner.send(exe.id, go)
    _drain(runner.worker())

    final = store.load(exe.id)
    assert final.active_path == "C"
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]  # not C.enter twice


def test_send_publishes_with_execution_priority():
    """runner.send() must publish the event with the execution's own priority so
    that a transport claim filtered at that priority level can pick it up."""
    store = DictStore()
    transport = InMemoryTransport()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})

    exe = runner.create(defn.id, priority=3)
    runner.send(exe.id, Event(kind="Go"))

    # the event must be visible at min_priority=3; if send() published at 0 this returns None
    lease = transport.claim("w", visibility=30, min_priority=3)
    assert lease is not None and lease.event.kind == "Go"


def test_worker_high_ratio_drains_high_priority_first():
    """Worker with high_ratio=1.0 processes the high-priority execution before low-priority ones.
    With high_ratio=1.0, random() < 1.0 always, so the first claim attempt always uses
    min_priority=threshold; falls back only if there's no high-priority work."""
    store = DictStore()
    transport = InMemoryTransport()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})

    # create 3 low-priority executions and send Go to each
    low_ids = [runner.create(defn.id, priority=0).id for _ in range(3)]
    for eid in low_ids:
        runner.send(eid, Event(kind="Go"))

    # create one high-priority execution and send Go
    high_id = runner.create(defn.id, priority=2).id
    runner.send(high_id, Event(kind="Go"))

    worker = runner.worker(high_ratio=1.0, priority_threshold=2)
    worker.step()  # must pick the high-priority execution

    high_final = store.load(high_id)
    assert high_final.status is Status.DONE  # fully processed (C → terminal)

    # low-priority executions untouched (still at B)
    for eid in low_ids:
        assert store.load(eid).active_path == "B"
