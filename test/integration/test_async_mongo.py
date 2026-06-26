"""Async Mongo store + transport contract tests, run against a real MongoDB in the stack.

mongomock is not faithfully atomic under concurrency, so these run against a real
MongoDB as the compose `test` service when STM_STORE_BACKEND=mongo (skipped
otherwise). Mirrors test_mongo_store.py + test_mongo_transport.py unit tests but
async: AsyncMongoStore + AsyncMongoTransport over motor. Also drives the full
AsyncDistributedRunner pipeline end-to-end over the pure-MongoDB backend.
"""

import os
import time

import pytest

pytestmark = pytest.mark.stack


def _skip_if_not_mongo():
    if os.environ.get("STM_STORE_BACKEND") != "mongo":
        pytest.skip("not the mongo backend")
    url = os.environ.get("STM_MONGO_URL")
    if not url:
        pytest.skip("STM_MONGO_URL not set")
    return url


@pytest.fixture
async def store():
    url = _skip_if_not_mongo()
    db = os.environ.get("STM_MONGO_DB", "harel")
    from harel.engine.aio_store import AsyncMongoStore

    s = await AsyncMongoStore.from_url(url, db)
    yield s
    await s.close()


@pytest.fixture
async def transport():
    url = _skip_if_not_mongo()
    db = os.environ.get("STM_MONGO_DB", "harel")
    from harel.engine.aio_transport import AsyncMongoTransport

    t = await AsyncMongoTransport.from_url(url, db)
    # reset state so each test starts with empty collections (group names are reused across tests)
    await t._msgs.drop()
    await t._locks.drop()
    await t._counters.drop()
    await t._locks.create_index("available_at")
    yield t
    await t.close()


# ---------------------------------------------------------------------------
# AsyncMongoStore contract
# ---------------------------------------------------------------------------


async def test_first_save_inserts_and_bumps_version(store):
    from harel.engine.execution import Execution

    e = Execution(definition_id="d", context={"n": 1})
    await store.save(e)
    assert e.version == 1
    loaded = await store.load(e.id)
    assert loaded is not None and loaded.version == 1 and loaded.context == {"n": 1}


async def test_sequential_saves_increment_version(store):
    from harel.engine.execution import Execution

    e = Execution(definition_id="d")
    await store.save(e)
    await store.save(e)
    assert e.version == 2
    assert (await store.load(e.id)).version == 2


async def test_stale_write_raises_conflict(store):
    from harel.engine.execution import Execution
    from harel.engine.store import StoreConflict

    e = Execution(definition_id="d")
    await store.save(e)  # version -> 1

    other = await store.load(e.id)
    other.context["w"] = "won"
    await store.save(other)  # commits version 2

    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        await store.save(e)
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1
    assert (await store.load(e.id)).context["w"] == "won"


async def test_commit_outbox_and_dedupe(store):
    from harel.engine.execution import Execution
    from harel.spec.states import Event

    e = Execution(definition_id="d")
    await store.commit(e, [("parent-x", Event(kind="Finished"))], processed_event_id="evt-1")

    pending = [p for p in await store.pending_outbox() if p.target_id == "parent-x"]
    assert len(pending) == 1 and pending[0].event.kind == "Finished"
    assert await store.is_processed(e.id, "evt-1")
    assert not await store.is_processed(e.id, "nope")

    await store.ack_outbox(pending[0].seq)
    assert all(p.target_id != "parent-x" for p in await store.pending_outbox())


async def test_timers_schedule_and_cancel(store):
    from harel.engine.execution import Execution
    from harel.engine.store import TimerOp

    e = Execution(definition_id="d")
    await store.commit(e, [], timers=(TimerOp("schedule", "Fork.A", fire_at=100.0),))
    due = [t for t in await store.due_timers(150.0) if t[0] == e.id]
    assert due == [(e.id, "Fork.A", 100.0)]
    await store.commit(e, [], timers=(TimerOp("cancel", "Fork.A"),))
    assert [t for t in await store.due_timers(150.0) if t[0] == e.id] == []


# ---------------------------------------------------------------------------
# AsyncMongoTransport exclusivity contract
# ---------------------------------------------------------------------------


async def test_fifo_within_a_group(transport):
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="e1"))
    await transport.publish("G", Event(kind="e2"))

    first = await transport.claim("w", visibility=30)
    assert first.event.kind == "e1"
    await transport.ack(first)
    second = await transport.claim("w", visibility=30)
    assert second.event.kind == "e2"


async def test_one_in_flight_per_group_but_other_groups_proceed(transport):
    from harel.spec.states import Event

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
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="only"))
    await transport.ack(await transport.claim("w", visibility=30))
    assert await transport.claim("w", visibility=30) is None


async def test_nack_returns_the_message_immediately(transport):
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="e1"))
    await transport.nack(await transport.claim("w", visibility=30))
    again = await transport.claim("w", visibility=30)
    assert again.event.kind == "e1"


async def test_a_held_lease_blocks_other_claims(transport):
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="e1"))
    held = await transport.claim("w1", visibility=30)
    assert held.event.kind == "e1"
    assert await transport.claim("w2", visibility=30) is None


async def test_ack_by_a_stale_owner_is_a_noop(transport):
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="e1"))
    stale = await transport.claim("w1", visibility=0.05)
    time.sleep(0.25)
    fresh = await transport.claim("w2", visibility=30)
    assert fresh is not None and fresh.event.kind == "e1"
    await transport.ack(stale)
    assert await transport.claim("w3", visibility=30) is None


async def test_lease_expiry_makes_a_message_claimable_again(transport):
    from harel.spec.states import Event

    await transport.publish("G", Event(kind="e1"))
    assert (await transport.claim("w1", visibility=0.05)).event.kind == "e1"
    time.sleep(0.25)
    recovered = await transport.claim("w2", visibility=30)
    assert recovered is not None and recovered.event.kind == "e1"


# ---------------------------------------------------------------------------
# Full async distributed pipeline over pure-MongoDB backend
# ---------------------------------------------------------------------------

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


async def _mongo_runner():
    url = _skip_if_not_mongo()
    db = os.environ.get("STM_MONGO_DB", "harel")
    from harel.engine.aio_store import AsyncMongoStore
    from harel.engine.aio_transport import AsyncMongoTransport

    store = await AsyncMongoStore.from_url(url, db)
    transport = await AsyncMongoTransport.from_url(url, db)
    return store, transport


async def test_pipeline_flat_async_mongo():
    from harel.dsl import definition_from_dsl
    from harel.engine.aio.distributed import AsyncDistributedRunner
    from harel.engine.execution import Status
    from harel.spec.states import Event

    store, transport = await _mongo_runner()
    try:
        defn = definition_from_dsl(FLAT, "M")
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
    finally:
        await store.close()
        await transport.close()


async def test_pipeline_orthogonal_async_mongo():
    from harel.dsl import definition_from_dsl
    from harel.engine.aio.distributed import AsyncDistributedRunner
    from harel.engine.execution import Status
    from harel.spec.states import Event

    store, transport = await _mongo_runner()
    try:
        defn = definition_from_dsl(ORTHO, "M")
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
    finally:
        await store.close()
        await transport.close()
