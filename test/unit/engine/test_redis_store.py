"""RedisStore unit tests, backed by fakeredis (no server, no Docker).

Same ExecutionStore contract as SqliteStore — version/CAS, transactional outbox,
dedupe — but all-network (no shared filesystem). The last two tests drive the
full DistributedRunner + Worker pipeline over a **pure-Redis** backend (RedisStore
+ RedisTransport), the alternative to pure-sqlite for multi-machine deployments.
"""

import pytest

fakeredis = pytest.importorskip("fakeredis")

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.distributed import DistributedRunner  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import RedisStore, StoreConflict  # noqa: E402
from harel.engine.transport import RedisTransport  # noqa: E402
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
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def store(server):
    return RedisStore(fakeredis.FakeStrictRedis(server=server))


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


# --- the full pipeline over a PURE-REDIS backend (RedisStore + RedisTransport) ---------------


def test_pipeline_flat_pure_redis(server):
    client = fakeredis.FakeStrictRedis(server=server)
    defn = definition_from_dsl(FLAT, "M")
    store = RedisStore(client)
    runner = DistributedRunner(store, RedisTransport(client), {defn.id: defn})

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


def test_pipeline_orthogonal_pure_redis(server):
    client = fakeredis.FakeStrictRedis(server=server)
    defn = definition_from_dsl(ORTHO, "M")
    store = RedisStore(client)
    runner = DistributedRunner(store, RedisTransport(client), {defn.id: defn})

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


def test_trace_recorded_in_commit_and_ring_capped(store):
    e = Execution(definition_id="d")
    store.save(e)
    for i in range(3):
        store.commit(e, [], processed_event_id=f"t{i}", trace={"event_kind": "Go", "to_path": f"S{i}"})
    trace = store.read_trace(e.id)
    assert [s["index"] for s in trace] == [0, 1, 2]
    assert [s["to_path"] for s in trace] == ["S0", "S1", "S2"]
    store.trace_max = 2  # ring keeps the last 2, indices stay monotonic
    store.commit(e, [], trace={"event_kind": "Go", "to_path": "S3"})
    assert [s["index"] for s in store.read_trace(e.id)] == [2, 3]
