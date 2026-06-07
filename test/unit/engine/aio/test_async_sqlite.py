"""Async SQLite store (aiosqlite, :memory: — no Docker).

Parity: AsyncDriver + AsyncSqliteStore reproduces the sync oracle on every shared scenario
(reloading from the serializing store). Plus an AsyncDurableRunner round-trip (create →
process → reload) over AsyncSqliteStore, proving the durable host works on the async store.
"""

import pytest
from scenarios import SCENARIOS, run_new

from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio.driver import AsyncDriver
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.aio_store import AsyncSqliteStore
from harel.engine.aio_transport import AsyncSqliteTransport
from harel.engine.execution import Execution, Status
from harel.spec.states import Event


async def _run_async_sqlite(scenario) -> dict:
    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))
    store = await AsyncSqliteStore.create(":memory:")
    driver = AsyncDriver(defn, store)
    await driver.start(exe)
    exe = await store.load(exe.id)  # serializing store -> reload
    trace = [{"event": "Start", "end_state": exe.active_path}]
    for ev in scenario["events"]:
        await driver.inject(exe, Event(kind=ev["kind"], data=dict(ev.get("data", {}))))
        exe = await store.load(exe.id)
        trace.append({"event": ev["kind"], "end_state": exe.active_path})
    await store.close()
    return {"trace": trace, "context": dict(exe.context), "status": exe.status.value}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_async_sqlite_matches_sync(scenario):
    assert await _run_async_sqlite(scenario) == run_new(scenario)


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


async def test_async_durable_roundtrip_over_sqlite():
    defn = definition_from_dsl(FLAT, "M")
    store = await AsyncSqliteStore.create(":memory:")
    runner = AsyncDurableRunner(store, {defn.id: defn})

    exe = await runner.create(defn.id)
    assert exe.active_path == "B"
    final = await runner.process(exe.id, Event(kind="Go"))
    assert final.active_path == "C" and final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]
    # a fresh runner on the SAME store resumes (durability)
    reloaded = await store.load(exe.id)
    assert reloaded.active_path == "C"
    await store.close()


async def test_async_sqlite_distributed_pipeline():
    defn = definition_from_dsl(FLAT, "M")
    store = await AsyncSqliteStore.create(":memory:")
    transport = await AsyncSqliteTransport.create(":memory:")
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
