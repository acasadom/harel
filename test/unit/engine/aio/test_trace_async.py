"""Execution trace on the async path: AsyncDriver(trace=True) records a step per event in the
async store's commit; default-off records nothing. Mirrors test_trace.py on the async core."""

from harel.dsl import definition_from_dsl
from harel.engine.aio.driver import AsyncDriver
from harel.engine.aio_store import AsyncSqliteStore
from harel.engine.execution import Execution
from harel.spec.states import Event

PING_PONG = """
machine M {
  initial A
  state A {}
  state B {}
  from A to B on Go
  from B to A on Go
}
"""


async def test_async_trace_records_transitions():
    store = await AsyncSqliteStore.create(":memory:")
    defn = definition_from_dsl(PING_PONG, "M")
    driver = AsyncDriver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id)
    await driver.start(exe)
    await driver.inject(exe, Event(kind="Go"))
    trace = await store.read_trace(exe.id)
    assert [s["index"] for s in trace] == [0, 1]
    assert [s["event_kind"] for s in trace] == ["Start", "Go"]
    assert [s["to_path"] for s in trace] == ["A", "B"]
    await store.close()


async def test_async_trace_default_off():
    store = await AsyncSqliteStore.create(":memory:")
    defn = definition_from_dsl(PING_PONG, "M")
    driver = AsyncDriver(defn, store)
    exe = Execution(definition_id=defn.id)
    await driver.start(exe)
    assert await store.read_trace(exe.id) == []
    await store.close()


async def test_async_trace_ring_cap():
    store = await AsyncSqliteStore.create(":memory:")
    store.trace_max = 2
    defn = definition_from_dsl(PING_PONG, "M")
    driver = AsyncDriver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id)
    await driver.start(exe)
    for _ in range(4):
        await driver.inject(exe, Event(kind="Go"))
    trace = await store.read_trace(exe.id)
    assert [s["index"] for s in trace] == [3, 4]
    await store.close()
