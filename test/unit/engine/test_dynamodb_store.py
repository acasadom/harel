"""DynamoDBStore unit tests, backed by moto (in-process AWS mock — no Docker).

Same ExecutionStore contract as the other backends — version/CAS, transactional
outbox, dedupe, spawns, timers — over DynamoDB: conditional writes are the CAS and
`TransactWriteItems` makes the whole commit atomic. The pipeline tests drive the
full DistributedRunner + Worker over DynamoDBStore (paired with InMemoryTransport
— the store is what's under test; SqsTransport is the AWS transport, covered in
the stack, and moto's SQS FIFO emulation is not a faithful transport test).
"""

import pytest

moto = pytest.importorskip("moto")
import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.distributed import DistributedRunner  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import DynamoDBStore, StoreConflict, TimerOp  # noqa: E402
from harel.engine.transport import InMemoryTransport  # noqa: E402
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
def store():
    with mock_aws():
        yield DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))


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
    assert store.load(e.id).context == {"n": 1, "items": ["a", "b"]}


def test_load_missing_is_none(store):
    assert store.load("nope") is None


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
    store.commit(e, [("parent-1", Event(kind="Finished"))], processed_event_id="evt-42")

    pending = store.pending_outbox()
    assert len(pending) == 1
    assert pending[0].target_id == "parent-1" and pending[0].event.kind == "Finished"
    assert store.is_processed(e.id, "evt-42")
    assert not store.is_processed(e.id, "other")

    store.ack_outbox(pending[0].seq)
    assert store.pending_outbox() == []


def test_commit_outbox_on_first_insert(store):
    # a region that finishes on start emits its Finished on the first commit
    # (version 0 -> the attribute_not_exists insert branch), so it must be carried
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
    assert [o.event.kind for o in store.pending_outbox()] == ["E1", "E2", "E3"]
    assert store.pending_outbox()[0].target_id is None  # None target round-trips


def test_stale_write_does_not_leak_outbox(store):
    # the CAS condition must cancel the whole TransactWriteItems, including the outbox
    e = Execution(definition_id="d")
    store.save(e)  # version -> 1
    store.save(store.load(e.id))  # version -> 2 (e is now stale at 1)

    with pytest.raises(StoreConflict):
        store.commit(e, [("p", Event(kind="Leaked"))])
    assert store.pending_outbox() == []  # nothing committed


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
    store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=100.0),))
    assert store.due_timers(50.0) == []
    assert store.due_timers(150.0) == [(e.id, "Fork.A", 100.0)]

    # re-schedule the same (execution, path) replaces the fire_at
    store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=200.0),))
    assert store.due_timers(150.0) == []
    assert store.due_timers(250.0) == [(e.id, "Fork.A", 200.0)]

    store.commit(e, [], timers=(TimerOp("cancel", "Fork.A"),))
    assert store.due_timers(250.0) == []


def test_delete_timer_is_guarded_on_fire_at(store):
    e = Execution(definition_id="d")
    store.commit(e, [], timers=(TimerOp("schedule", "S", fire_at=100.0),))
    store.delete_timer(e.id, "S", fire_at=99.0)  # stale time -> no-op
    assert store.due_timers(150.0) == [(e.id, "S", 100.0)]
    store.delete_timer(e.id, "S", fire_at=100.0)  # matching time -> removed
    assert store.due_timers(150.0) == []


# --- the full pipeline over DynamoDBStore (InMemoryTransport drives it deterministically) ----


def test_pipeline_flat_over_dynamodb():
    with mock_aws():
        defn = definition_from_dsl(FLAT, "M")
        store = DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

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


def test_pipeline_orthogonal_over_dynamodb():
    with mock_aws():
        defn = definition_from_dsl(ORTHO, "M")
        store = DynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})

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
