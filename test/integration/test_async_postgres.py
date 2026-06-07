"""AsyncPostgresStore / AsyncPostgresTransport contract, against a real Postgres.

`stack`-marked (deselected by default): needs a running Postgres reachable via
STM_POSTGRES_DSN (the compose `test` service, or an ad-hoc container). Mirrors the sync
`test_postgres_store` but over the async backends: parity vs the sync oracle + a distributed
pipeline (flat + orthogonal) over AsyncDistributedRunner + AsyncWorker.
"""

import os
import uuid

import pytest

pytestmark = pytest.mark.stack

from scenarios import SCENARIOS, run_new  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner  # noqa: E402
from harel.engine.aio.driver import AsyncDriver  # noqa: E402
from harel.engine.aio_store import AsyncPostgresStore  # noqa: E402
from harel.engine.aio_transport import AsyncPostgresTransport  # noqa: E402
from harel.engine.execution import Execution, Status  # noqa: E402
from harel.spec.states import Event  # noqa: E402


def _dsn() -> str:
    dsn = os.environ.get("STM_POSTGRES_DSN")
    if not dsn:
        pytest.skip("STM_POSTGRES_DSN not set (not the postgres stack)")
    return dsn


async def _fresh_store() -> AsyncPostgresStore:
    store = await AsyncPostgresStore.from_dsn(_dsn())
    async with store._pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE executions, outbox, processed_events, timers, spawns")
        await conn.commit()
    return store


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_async_postgres_matches_sync(scenario):
    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))
    store = await _fresh_store()
    driver = AsyncDriver(defn, store)
    await driver.start(exe)
    exe = await store.load(exe.id)
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


async def test_async_postgres_distributed_pipeline():
    defn = definition_from_dsl(FLAT, "M")
    store = await _fresh_store()
    transport = await AsyncPostgresTransport.from_dsn(_dsn(), prefix=f"t{uuid.uuid4().hex[:8]}")
    async with transport._pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE transport_messages")
        await conn.commit()
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
