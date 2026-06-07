"""Async Surreal backends (AsyncSurrealStore + AsyncSurrealTransport) over `mem://` — no Docker.

Mirrors test_surreal_store.py + test_surreal_transport.py but async. The in-process
`mem://` engine validates the CAS/THROW semantics and the full distributed pipeline
without a server. Concurrency tests require a real SurrealDB server.
"""

import time

import pytest

surrealdb = pytest.importorskip("surrealdb")

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner  # noqa: E402
from harel.engine.aio_store import AsyncSurrealStore  # noqa: E402
from harel.engine.aio_transport import AsyncSurrealTransport  # noqa: E402
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
async def client():
    from surrealdb import AsyncSurreal

    db = AsyncSurreal("mem://")
    await db.connect()
    await db.use("test", "test")
    yield db
    await db.close()


@pytest.fixture
async def store(client):
    return AsyncSurrealStore(client)


@pytest.fixture
async def transport(client):
    return AsyncSurrealTransport(client)


# ---------------------------------------------------------------------------
# AsyncSurrealStore contract
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
    loaded = await store.load(e.id)
    assert loaded.context == {"n": 1, "items": ["a", "b"]}


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
    kinds = [o.event.kind for o in await store.pending_outbox()]
    assert kinds == ["E1", "E2", "E3"]


async def test_stale_write_does_not_leak_outbox(store):
    e = Execution(definition_id="d")
    await store.save(e)  # version -> 1
    other = await store.load(e.id)
    await store.save(other)  # version -> 2 (e is now stale at 1)

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
# AsyncSurrealTransport contract
# ---------------------------------------------------------------------------


async def test_fifo_within_a_group(transport):
    await transport.publish("G", Event(kind="e1"))
    await transport.publish("G", Event(kind="e2"))

    first = await transport.claim("w", visibility=30)
    assert first.event.kind == "e1"
    await transport.ack(first)
    second = await transport.claim("w", visibility=30)
    assert second.event.kind == "e2"


async def test_one_in_flight_per_group_but_other_groups_proceed(transport):
    await transport.publish("G", Event(kind="g1"))
    await transport.publish("G", Event(kind="g2"))
    await transport.publish("H", Event(kind="h1"))

    a = await transport.claim("w1", visibility=30)
    b = await transport.claim("w2", visibility=30)
    assert {a.group_id, b.group_id} == {"G", "H"}

    assert await transport.claim("w3", visibility=30) is None

    g_lease = a if a.group_id == "G" else b
    await transport.ack(g_lease)
    nxt = await transport.claim("w3", visibility=30)
    assert nxt.group_id == "G" and nxt.event.kind == "g2"


async def test_ack_removes_the_message(transport):
    await transport.publish("G", Event(kind="only"))
    await transport.ack(await transport.claim("w", visibility=30))
    assert await transport.claim("w", visibility=30) is None


async def test_nack_returns_the_message_immediately(transport):
    await transport.publish("G", Event(kind="e1"))
    await transport.nack(await transport.claim("w", visibility=30))
    again = await transport.claim("w", visibility=30)
    assert again.event.kind == "e1"


async def test_nack_with_delay_parks_the_message(transport):
    await transport.publish("G", Event(kind="e1"))
    await transport.nack(await transport.claim("w", visibility=30), delay=0.2)
    assert await transport.claim("w", visibility=30) is None
    time.sleep(0.3)
    again = await transport.claim("w", visibility=30)
    assert again is not None and again.event.kind == "e1"


async def test_a_held_lease_blocks_other_claims(transport):
    await transport.publish("G", Event(kind="e1"))
    held = await transport.claim("w1", visibility=30)
    assert held.event.kind == "e1"
    assert await transport.claim("w2", visibility=30) is None


async def test_ack_by_a_stale_owner_is_a_noop(transport):
    await transport.publish("G", Event(kind="e1"))
    stale = await transport.claim("w1", visibility=0.05)
    time.sleep(0.25)
    fresh = await transport.claim("w2", visibility=30)
    assert fresh is not None and fresh.event.kind == "e1"
    await transport.ack(stale)
    assert await transport.claim("w3", visibility=30) is None


async def test_lease_expiry_makes_a_message_claimable_again(transport):
    await transport.publish("G", Event(kind="e1"))
    assert (await transport.claim("w1", visibility=0.05)).event.kind == "e1"
    time.sleep(0.25)
    recovered = await transport.claim("w2", visibility=30)
    assert recovered is not None and recovered.event.kind == "e1"


# ---------------------------------------------------------------------------
# Full async distributed pipeline over pure-Surreal backend
# ---------------------------------------------------------------------------


async def test_pipeline_flat_async_surreal(client):
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncSurrealStore(client)
    transport = AsyncSurrealTransport(client)
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


async def test_pipeline_orthogonal_async_surreal(client):
    defn = definition_from_dsl(ORTHO, "M")
    store = AsyncSurrealStore(client)
    transport = AsyncSurrealTransport(client)
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})

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
