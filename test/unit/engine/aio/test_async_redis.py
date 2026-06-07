"""Async Redis backends (fakeredis.aioredis, in-process — no Docker).

Two checks:
1. Parity: `AsyncDriver` + `AsyncRedisStore` reproduces the sync oracle on every shared
   scenario (reloading from the serializing store, like the sync redis parity test).
2. Distributed pipeline: AsyncDistributedRunner + AsyncWorker over `AsyncRedisStore` +
   `AsyncRedisTransport` (the ZSET-claim transport) drains flat + orthogonal machines.
"""

import fakeredis.aioredis
import pytest
from scenarios import SCENARIOS, run_new

from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio.driver import AsyncDriver
from harel.engine.aio_store import AsyncRedisStore
from harel.engine.aio_transport import AsyncRedisTransport
from harel.engine.execution import Execution, Status
from harel.spec.states import Event


async def _run_async_redis(scenario) -> dict:
    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))
    store = AsyncRedisStore(fakeredis.aioredis.FakeRedis())
    driver = AsyncDriver(defn, store)
    await driver.start(exe)
    exe = await store.load(exe.id)  # serializing store -> reload to observe persisted state
    trace = [{"event": "Start", "end_state": exe.active_path}]
    for ev in scenario["events"]:
        await driver.inject(exe, Event(kind=ev["kind"], data=dict(ev.get("data", {}))))
        exe = await store.load(exe.id)
        trace.append({"event": ev["kind"], "end_state": exe.active_path})
    await store.close()
    return {"trace": trace, "context": dict(exe.context), "status": exe.status.value}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_async_redis_store_matches_sync(scenario):
    assert await _run_async_redis(scenario) == run_new(scenario)


def _h(label: str) -> str:
    return f'stm_actions.rec(at: "{label}")'


FLAT = f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")} }}
  state C {{ on enter {_h("C.enter")} }}
  from A to B
  from B to C on Go
}}
"""

ORTHO = f"""
machine M {{
  initial Fork
  orthogonal Fork {{
    state A {{ initial A1  state A1 {{ on enter {_h("A1")} }}  state A2 {{ on enter {_h("A2")} }}  from A1 to A2 on Go }}
    state B {{ initial B1  state B1 {{ on enter {_h("B1")} }}  state B2 {{ on enter {_h("B2")} }}  from B1 to B2 on Go }}
  }}
  state Done {{ on enter {_h("Done")} }}
  from Fork to Done
}}
"""


async def _runner(defn):
    return AsyncDistributedRunner(
        AsyncRedisStore(fakeredis.aioredis.FakeRedis()),
        AsyncRedisTransport(fakeredis.aioredis.FakeRedis()),
        {defn.id: defn},
    )


async def _drain(runner):
    w = runner.worker()
    while await w.step():
        pass


async def test_async_redis_pipeline_flat():
    defn = definition_from_dsl(FLAT, "M")
    runner = await _runner(defn)
    exe = await runner.create(defn.id)
    assert exe.active_path == "B"
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)
    final = await runner.store.load(exe.id)
    assert final.active_path == "C" and final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


async def test_async_redis_pipeline_orthogonal():
    defn = definition_from_dsl(ORTHO, "M")
    runner = await _runner(defn)
    exe = await runner.create(defn.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)
    final = await runner.store.load(exe.id)
    assert final.active_path == "Done" and final.status is Status.DONE
    regions = [await runner.store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
