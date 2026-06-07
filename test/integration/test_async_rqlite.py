"""Async Rqlite store + transport contract tests, run against a real rqlite in the stack.

No in-process rqlite fake — these run as the compose `test` service when
STM_STORE_BACKEND=rqlite (skipped otherwise). Mirrors test_rqlite_store.py but async:
AsyncRqliteStore + AsyncRqliteTransport via httpx.AsyncClient. Also drives the full
AsyncDistributedRunner pipeline end-to-end over the pure-rqlite backend.
"""

import os

import pytest

pytestmark = pytest.mark.stack


@pytest.fixture
async def store():
    if os.environ.get("STM_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("STM_RQLITE_URL")
    if not url:
        pytest.skip("STM_RQLITE_URL not set")

    from harel.engine.aio_store import AsyncRqliteStore

    s = await AsyncRqliteStore.from_url(url)
    yield s
    await s.close()


@pytest.fixture
async def transport():
    if os.environ.get("STM_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("STM_RQLITE_URL")
    if not url:
        pytest.skip("STM_RQLITE_URL not set")

    from harel.engine.aio_transport import AsyncRqliteTransport

    t = await AsyncRqliteTransport.from_url(url)
    yield t
    await t.close()


# ---------------------------------------------------------------------------
# AsyncRqliteStore contract
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
    await store.commit(e, [("parent-r", Event(kind="Finished"))], processed_event_id="evt-r")

    pending = [p for p in await store.pending_outbox() if p.target_id == "parent-r"]
    assert len(pending) == 1 and pending[0].event.kind == "Finished"
    assert await store.is_processed(e.id, "evt-r")
    assert not await store.is_processed(e.id, "nope")

    await store.ack_outbox(pending[0].seq)
    assert all(p.target_id != "parent-r" for p in await store.pending_outbox())


async def test_conflict_leaves_outbox_untouched(store):
    from harel.engine.execution import Execution
    from harel.engine.store import StoreConflict
    from harel.spec.states import Event

    e = Execution(definition_id="d")
    await store.save(e)  # version 1
    before = len(await store.pending_outbox())

    winner = await store.load(e.id)
    winner.context["who"] = "winner"
    await store.save(winner)  # commits version 2 with distinct data

    e.context["who"] = "stale"
    with pytest.raises(StoreConflict):
        await store.commit(e, [("p", Event(kind="X"))], processed_event_id="ev")
    assert len(await store.pending_outbox()) == before
    assert not await store.is_processed(e.id, "ev")


# ---------------------------------------------------------------------------
# Full async distributed pipeline over pure-rqlite backend
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


async def _rqlite_runner():
    if os.environ.get("STM_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("STM_RQLITE_URL")
    if not url:
        pytest.skip("STM_RQLITE_URL not set")
    from harel.engine.aio_store import AsyncRqliteStore
    from harel.engine.aio_transport import AsyncRqliteTransport

    store = await AsyncRqliteStore.from_url(url)
    transport = await AsyncRqliteTransport.from_url(url)
    return store, transport


async def test_pipeline_flat_async_rqlite():
    from harel.dsl import definition_from_dsl
    from harel.engine.aio.distributed import AsyncDistributedRunner
    from harel.engine.execution import Status
    from harel.spec.states import Event

    store, transport = await _rqlite_runner()
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


async def test_pipeline_orthogonal_async_rqlite():
    from harel.dsl import definition_from_dsl
    from harel.engine.aio.distributed import AsyncDistributedRunner
    from harel.engine.execution import Status
    from harel.spec.states import Event

    store, transport = await _rqlite_runner()
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
