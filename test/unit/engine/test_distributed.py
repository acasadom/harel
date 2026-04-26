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
