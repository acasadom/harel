"""MongoStore unit tests, backed by mongomock (no server, no Docker).

Same ExecutionStore contract as the SQL backends — version/CAS, transactional
outbox, dedupe, spawns, timers — but over a document store: everything for one
Execution lives in its single document, so a `commit` is one atomic `update_one`
(no replica set / multi-document transaction needed). The last two tests drive
the full DistributedRunner + Worker pipeline over a **pure-Mongo** backend
(MongoStore + MongoTransport), the document-store sibling of the pure-Redis path.
"""

import pytest

mongomock = pytest.importorskip("mongomock")

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.distributed import DistributedRunner  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import MongoStore, StoreConflict, TimerOp  # noqa: E402
from harel.engine.transport import MongoTransport  # noqa: E402
from harel.spec.states import Event  # noqa: E402

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


@pytest.fixture
def client():
    return mongomock.MongoClient()


@pytest.fixture
def store(client):
    return MongoStore(client)


def test_first_save_inserts_and_bumps_version(store):
    e = Execution(definition_id="d")
    store.save(e)
    assert e.version == 1
    assert store.load(e.id).version == 1


def test_sequential_saves_increment_version(store):
    e = Execution(definition_id="d")
    store.save(e)
    store.save(e)
    store.save(e)
    assert e.version == 3
    assert store.load(e.id).version == 3


def test_json_round_trip_preserves_context(store):
    e = Execution(definition_id="d", context={"n": 1, "items": ["a", "b"]})
    store.save(e)
    loaded = store.load(e.id)
    assert loaded.context == {"n": 1, "items": ["a", "b"]}


def test_stale_write_raises_conflict(store):
    e = Execution(definition_id="d")
    store.save(e)  # version -> 1

    other = store.load(e.id)  # a second view at version 1
    other.context["w"] = "won"
    store.save(other)  # commits version 2

    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        store.save(e)  # still at version 1 -> loses
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1  # in-memory bump rolled back
    assert store.load(e.id).context["w"] == "won"


def test_commit_outbox_and_dedupe(store):
    e = Execution(definition_id="d")
    ev = Event(kind="Finished")
    store.commit(e, [("parent-1", ev)], processed_event_id="evt-42")

    pending = store.pending_outbox()
    assert len(pending) == 1
    assert pending[0].target_id == "parent-1" and pending[0].event.kind == "Finished"
    assert store.is_processed(e.id, "evt-42")
    assert not store.is_processed(e.id, "other")

    store.ack_outbox(pending[0].seq)
    assert store.pending_outbox() == []


def test_commit_outbox_on_first_insert(store):
    # a region that finishes on start emits its Finished on the very first commit
    # (version 0 -> insert path), so the outbox must be carried into the insert
    e = Execution(definition_id="d")
    store.commit(e, [("parent-1", Event(kind="Finished"))], processed_event_id="evt-1")
    assert e.version == 1
    pending = store.pending_outbox()
    assert len(pending) == 1 and pending[0].target_id == "parent-1"
    assert store.is_processed(e.id, "evt-1")


def test_outbox_ordered_across_executions(store):
    a, b = Execution(definition_id="d"), Execution(definition_id="d")
    store.commit(a, [(None, Event(kind="E1"))])
    store.commit(b, [(None, Event(kind="E2"))])
    store.commit(a, [(None, Event(kind="E3"))])
    kinds = [o.event.kind for o in store.pending_outbox()]
    assert kinds == ["E1", "E2", "E3"]  # monotonic seq, oldest first


def test_spawns_committed_and_acked(store):
    e = Execution(definition_id="d")
    store.commit(e, [], spawns=(("child-1", "Fork.A", {"x": 1}),))
    pending = store.pending_spawns()
    assert len(pending) == 1
    s = pending[0]
    assert s.parent_id == e.id and s.child_id == "child-1" and s.root_path == "Fork.A"
    assert s.context == {"x": 1}
    store.ack_spawn(s.seq)
    assert store.pending_spawns() == []


def test_timers_schedule_due_and_cancel(store):
    e = Execution(definition_id="d")
    # paths carry dots (the node separator) — exercises the key encoding
    store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=100.0),))
    assert store.due_timers(50.0) == []  # not due yet
    assert store.due_timers(150.0) == [(e.id, "Fork.A", 100.0)]

    # re-schedule the same path replaces the fire_at (not a duplicate)
    store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=200.0),))
    assert store.due_timers(150.0) == []
    assert store.due_timers(250.0) == [(e.id, "Fork.A", 200.0)]

    # cancel disarms it
    store.commit(e, [], timers=(TimerOp("cancel", "Fork.A"),))
    assert store.due_timers(250.0) == []


def test_delete_timer_is_guarded_on_fire_at(store):
    e = Execution(definition_id="d")
    store.commit(e, [], timers=(TimerOp("schedule", "S", fire_at=100.0),))
    store.delete_timer(e.id, "S", fire_at=99.0)  # stale time -> no-op
    assert store.due_timers(150.0) == [(e.id, "S", 100.0)]
    store.delete_timer(e.id, "S", fire_at=100.0)  # matching time -> removed
    assert store.due_timers(150.0) == []


# --- the full pipeline over a PURE-MONGO backend (MongoStore + MongoTransport) ---------------


def test_pipeline_flat_pure_mongo(client):
    defn = definition_from_dsl(FLAT, "M")
    store = MongoStore(client)
    runner = DistributedRunner(store, MongoTransport(client), {defn.id: defn})

    exe = runner.create(defn.id)
    assert exe.active_path == "B"
    runner.send(exe.id, Event(kind="Go"))
    w = runner.worker()
    while w.step():
        pass

    final = store.load(exe.id)
    assert final.active_path == "C"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


def test_pipeline_orthogonal_pure_mongo(client):
    defn = definition_from_dsl(ORTHO, "M")
    store = MongoStore(client)
    runner = DistributedRunner(store, MongoTransport(client), {defn.id: defn})

    exe = runner.create(defn.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    runner.send(exe.id, Event(kind="Go"))
    w = runner.worker()
    while w.step():
        pass

    final = store.load(exe.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["Done"]
    regions = [store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
