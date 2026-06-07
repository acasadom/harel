"""Async DynamoDB store + SQS transport (moto mock_aws, in-process — no Docker).

AsyncDynamoDBStore wraps DynamoDBStore via asyncio.to_thread: event-loop non-blocking,
moto patches botocore process-wide so the mock applies in threads too.
AsyncSqsTransport does the same for SqsTransport.

Store contract mirrors test_dynamodb_store.py. Pipeline tests use AsyncDistributedRunner
with AsyncInMemoryTransport (store under test, same as the sync pipeline tests). A
separate SQS pipeline test uses AsyncSqsTransport end-to-end.
"""

import pytest

moto = pytest.importorskip("moto")
import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner  # noqa: E402
from harel.engine.aio_store import AsyncDynamoDBStore  # noqa: E402
from harel.engine.aio_transport import AsyncInMemoryTransport, AsyncSqsTransport  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import StoreConflict, TimerOp  # noqa: E402
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
        yield AsyncDynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))


# ---------------------------------------------------------------------------
# AsyncDynamoDBStore contract
# ---------------------------------------------------------------------------


async def test_first_save_inserts_and_bumps_version(store):
    e = Execution(definition_id="d")
    await store.save(e)
    assert e.version == 1
    assert (await store.load(e.id)).version == 1


async def test_sequential_saves_increment_version(store):
    e = Execution(definition_id="d")
    await store.save(e)
    await store.save(e)
    await store.save(e)
    assert e.version == 3
    assert (await store.load(e.id)).version == 3


async def test_json_round_trip_preserves_context(store):
    e = Execution(definition_id="d", context={"n": 1, "items": ["a", "b"]})
    await store.save(e)
    assert (await store.load(e.id)).context == {"n": 1, "items": ["a", "b"]}


async def test_load_missing_is_none(store):
    assert await store.load("nope") is None


async def test_stale_write_raises_conflict(store):
    e = Execution(definition_id="d")
    await store.save(e)  # version -> 1

    other = await store.load(e.id)
    other.context["w"] = "won"
    await store.save(other)  # commits version 2

    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        await store.save(e)  # still at version 1 -> loses
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1  # in-memory bump rolled back
    assert (await store.load(e.id)).context["w"] == "won"


async def test_commit_outbox_and_dedupe(store):
    e = Execution(definition_id="d")
    await store.commit(e, [("parent-1", Event(kind="Finished"))], processed_event_id="evt-42")

    pending = await store.pending_outbox()
    assert len(pending) == 1
    assert pending[0].target_id == "parent-1" and pending[0].event.kind == "Finished"
    assert await store.is_processed(e.id, "evt-42")
    assert not await store.is_processed(e.id, "other")

    await store.ack_outbox(pending[0].seq)
    assert await store.pending_outbox() == []


async def test_commit_outbox_on_first_insert(store):
    e = Execution(definition_id="d")
    await store.commit(e, [("parent-1", Event(kind="Finished"))], processed_event_id="evt-1")
    assert e.version == 1
    pending = await store.pending_outbox()
    assert len(pending) == 1 and pending[0].target_id == "parent-1"
    assert await store.is_processed(e.id, "evt-1")


async def test_outbox_ordered_across_executions(store):
    a, b = Execution(definition_id="d"), Execution(definition_id="d")
    await store.commit(a, [(None, Event(kind="E1"))])
    await store.commit(b, [(None, Event(kind="E2"))])
    await store.commit(a, [(None, Event(kind="E3"))])
    assert [o.event.kind for o in await store.pending_outbox()] == ["E1", "E2", "E3"]
    assert (await store.pending_outbox())[0].target_id is None


async def test_stale_write_does_not_leak_outbox(store):
    e = Execution(definition_id="d")
    await store.save(e)  # version -> 1
    await store.save(await store.load(e.id))  # version -> 2 (e is now stale at 1)

    with pytest.raises(StoreConflict):
        await store.commit(e, [("p", Event(kind="Leaked"))])
    assert await store.pending_outbox() == []


async def test_spawns_committed_and_acked(store):
    e = Execution(definition_id="d")
    await store.commit(e, [], spawns=(("child-1", "Fork.A", {"x": 1}),))
    pending = await store.pending_spawns()
    assert len(pending) == 1
    s = pending[0]
    assert s.parent_id == e.id and s.child_id == "child-1" and s.root_path == "Fork.A"
    assert s.context == {"x": 1}
    await store.ack_spawn(s.seq)
    assert await store.pending_spawns() == []


async def test_timers_schedule_due_and_cancel(store):
    e = Execution(definition_id="d")
    await store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=100.0),))
    assert await store.due_timers(50.0) == []
    assert await store.due_timers(150.0) == [(e.id, "Fork.A", 100.0)]

    await store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=200.0),))
    assert await store.due_timers(150.0) == []
    assert await store.due_timers(250.0) == [(e.id, "Fork.A", 200.0)]

    await store.commit(e, [], timers=(TimerOp("cancel", "Fork.A"),))
    assert await store.due_timers(250.0) == []


async def test_delete_timer_is_guarded_on_fire_at(store):
    e = Execution(definition_id="d")
    await store.commit(e, [], timers=(TimerOp("schedule", "S", fire_at=100.0),))
    await store.delete_timer(e.id, "S", fire_at=99.0)  # stale time -> no-op
    assert await store.due_timers(150.0) == [(e.id, "S", 100.0)]
    await store.delete_timer(e.id, "S", fire_at=100.0)  # matching time -> removed
    assert await store.due_timers(150.0) == []


# ---------------------------------------------------------------------------
# Full async distributed pipeline over AsyncDynamoDBStore
# ---------------------------------------------------------------------------


async def test_pipeline_flat_async_dynamodb():
    with mock_aws():
        defn = definition_from_dsl(FLAT, "M")
        store = AsyncDynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

        exe = await runner.create(defn.id)
        assert exe.active_path == "B"
        await runner.send(exe.id, Event(kind="Go"))
        w = runner.worker()
        while await w.step():
            pass

        final = await store.load(exe.id)
        assert final.active_path == "C"
        assert final.status is Status.DONE
        assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


async def test_pipeline_orthogonal_async_dynamodb():
    with mock_aws():
        defn = definition_from_dsl(ORTHO, "M")
        store = AsyncDynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

        exe = await runner.create(defn.id)
        assert exe.active_path == "Fork"
        child_ids = list(exe.children)
        await runner.send(exe.id, Event(kind="Go"))
        w = runner.worker()
        while await w.step():
            pass

        final = await store.load(exe.id)
        assert final.active_path == "Done"
        assert final.status is Status.DONE
        assert final.context["trace"] == ["Done"]
        regions = [await store.load(cid) for cid in child_ids]
        assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]


# ---------------------------------------------------------------------------
# AsyncSqsTransport pipeline (moto mock_aws, no Docker)
# ---------------------------------------------------------------------------


async def test_pipeline_flat_async_sqs():
    with mock_aws():
        sqs_client = boto3.client(
            "sqs",
            region_name="us-east-1",
            endpoint_url=None,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        resp = sqs_client.create_queue(QueueName="stm.fifo", Attributes={"FifoQueue": "true"})
        transport = AsyncSqsTransport(sqs_client, resp["QueueUrl"], wait_seconds=0)

        defn = definition_from_dsl(FLAT, "M")
        store = AsyncDynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        runner = AsyncDistributedRunner(store, transport, {defn.id: defn})

        exe = await runner.create(defn.id)
        assert exe.active_path == "B"
        await runner.send(exe.id, Event(kind="Go"))
        w = runner.worker()
        while await w.step():
            pass

        final = await store.load(exe.id)
        assert final.active_path == "C"
        assert final.status is Status.DONE
        assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


# ---------------------------------------------------------------------------
# Concurrent CAS — asyncio.to_thread means truly parallel threads, so this
# exercises the DynamoDB TransactWriteItems ConditionExpression under real
# thread concurrency (moto patches botocore globally, safe from threads).
# ---------------------------------------------------------------------------


async def test_concurrent_writers_only_one_wins():
    import asyncio

    with mock_aws():
        store = AsyncDynamoDBStore(boto3.client("dynamodb", region_name="us-east-1"))
        e = Execution(definition_id="d")
        await store.save(e)  # version -> 1

        a = await store.load(e.id)
        b = await store.load(e.id)
        a.context["w"] = "a"
        b.context["w"] = "b"

        results = await asyncio.gather(store.save(a), store.save(b), return_exceptions=True)
        conflicts = [r for r in results if isinstance(r, StoreConflict)]
        successes = [r for r in results if r is None]
        assert len(conflicts) == 1, f"expected 1 conflict, got: {results}"
        assert len(successes) == 1

        final = await store.load(e.id)
        assert final.version == 2
        assert final.context["w"] in ("a", "b")
        loser = a if a.version == 1 else b
        assert loser.version == 1  # version rolled back on the loser
