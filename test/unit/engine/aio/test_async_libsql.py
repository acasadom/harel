"""libSQL backend (the `libsql` package) in local `file:` mode — no Docker.

libSQL is SQLite-compatible, so the store/transport mirror the SQLite ones; the `libsql`
driver is synchronous, so AsyncLibsqlStore/AsyncLibsqlTransport off-load to a thread. Proven
in-process: parity vs the sync oracle on every scenario, a distributed pipeline (flat +
orthogonal) over the libSQL store AND transport, the version-CAS conflict, and load_for_event.
The same backend, by connection args, also targets a `sqld` server or an embedded Turso replica.
"""

import os
import tempfile
import uuid

import pytest

pytest.importorskip("libsql")

from scenarios import SCENARIOS, run_new  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner  # noqa: E402
from harel.engine.aio.driver import AsyncDriver  # noqa: E402
from harel.engine.aio_store import AsyncLibsqlStore  # noqa: E402
from harel.engine.aio_transport import AsyncLibsqlTransport  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.engine.store import LibsqlStore, StoreConflict  # noqa: E402
from harel.spec.states import Event  # noqa: E402


def _tmp() -> str:
    return os.path.join(tempfile.gettempdir(), f"harel_libsql_{uuid.uuid4().hex}.db")


async def _fresh_store() -> AsyncLibsqlStore:
    return await AsyncLibsqlStore.create(_tmp())


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_async_libsql_matches_sync(scenario):
    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))
    store = await _fresh_store()
    driver = AsyncDriver(defn, store)
    await driver.start(exe)
    exe = await store.load(exe.id)  # serializing store -> reload to observe persisted state
    trace = [{"event": "Start", "end_state": exe.active_path}]
    for ev in scenario["events"]:
        await driver.inject(exe, Event(kind=ev["kind"], data=dict(ev.get("data", {}))))
        exe = await store.load(exe.id)
        trace.append({"event": ev["kind"], "end_state": exe.active_path})
    await store.close()
    assert {"trace": trace, "context": dict(exe.context), "status": exe.status.value} == run_new(scenario)


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


async def test_async_libsql_pipeline_flat():
    defn = definition_from_dsl(FLAT, "M")
    store = await _fresh_store()
    transport = await AsyncLibsqlTransport.create(_tmp())  # libSQL store AND transport
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
    exe = await runner.create(defn.id)
    assert exe.active_path == "B"
    await runner.send(exe.id, Event(kind="Go"))
    w = runner.worker()
    while await w.step():
        pass
    final = await store.load(exe.id)
    assert final.active_path == "C" and final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]
    await store.close()
    await transport.close()


async def test_async_libsql_pipeline_orthogonal():
    defn = definition_from_dsl(ORTHO, "M")
    store = await _fresh_store()
    transport = await AsyncLibsqlTransport.create(_tmp())
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
    exe = await runner.create(defn.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    await runner.send(exe.id, Event(kind="Go"))
    w = runner.worker()
    while await w.step():
        pass
    final = await store.load(exe.id)
    assert final.active_path == "Done" and final.status is Status.DONE
    regions = [await store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
    await store.close()
    await transport.close()


async def test_async_libsql_stale_write_conflict():
    store = await _fresh_store()
    e = Execution(definition_id="d")
    await store.save(e)  # version -> 1
    other = await store.load(e.id)
    other.context["w"] = "won"
    await store.save(other)  # version -> 2
    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        await store.save(e)
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1
    assert (await store.load(e.id)).context["w"] == "won"
    assert await store.pending_outbox() == []  # the stale commit left nothing behind
    await store.close()


async def test_async_libsql_load_for_event():
    store = await _fresh_store()
    e = Execution(definition_id="d")
    await store.commit(e, [], processed_event_id="e1")
    exe1, p1 = await store.load_for_event(e.id, "e1")
    assert exe1 is not None and p1 is True
    exe2, p2 = await store.load_for_event(e.id, "e2")
    assert exe2 is not None and p2 is False
    none, pn = await store.load_for_event("nope", "e1")
    assert none is None and pn is False
    await store.close()


def test_sync_libsql_store_contract():
    """The synchronous LibsqlStore directly (the async wrapper delegates to it)."""
    store = LibsqlStore(_tmp())
    e = Execution(definition_id="d", context={"n": 1})
    store.commit(e, [(None, Event(kind="E"))], processed_event_id="e1")
    assert store.load(e.id).context == {"n": 1}
    assert store.is_processed(e.id, "e1") and not store.is_processed(e.id, "e2")
    pend = store.pending_outbox()
    assert len(pend) == 1 and pend[0].event.kind == "E"
    page = store.list_executions()
    assert any(s.id == e.id for s in page.items)
    store.close()
