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


@pytest.fixture
async def pg_transport():
    """Fresh AsyncPostgresTransport with an isolated prefix; truncates on setup."""
    t = await AsyncPostgresTransport.from_dsn(_dsn(), prefix=f"t{uuid.uuid4().hex[:8]}")
    async with t._pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE transport_messages, transport_groups")
        await conn.commit()
    yield t
    await t.close()


async def test_async_postgres_distributed_pipeline():
    defn = definition_from_dsl(FLAT, "M")
    store = await _fresh_store()
    transport = await AsyncPostgresTransport.from_dsn(_dsn(), prefix=f"t{uuid.uuid4().hex[:8]}")
    async with transport._pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE transport_messages, transport_groups")
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


# ---------------------------------------------------------------------------
# AsyncPostgresTransport fairness and priority contract
# ---------------------------------------------------------------------------


async def test_async_postgres_transport_round_robin(pg_transport):
    """After acking A, a fresh group B (lock_expiry NULL → coalesced 0) must
    sort before A (lock_expiry = now > 0) in the next claim."""
    for i in range(5):
        await pg_transport.publish("A", Event(kind=f"a{i}"))

    lease_a = await pg_transport.claim("w", visibility=30)
    assert lease_a is not None and lease_a.group_id == "A"
    await pg_transport.ack(lease_a)  # A's lock_expiry = now

    # B is fresh: lock_expiry NULL → COALESCE → 0 < A's lock_expiry
    await pg_transport.publish("B", Event(kind="b0"))

    lease_b = await pg_transport.claim("w", visibility=30)
    assert lease_b is not None and lease_b.group_id == "B"


async def test_async_postgres_transport_min_priority_filters(pg_transport):
    """claim(min_priority=N) skips groups whose priority < N; fallback to 0 picks them."""
    await pg_transport.publish("lo", Event(kind="e1"), priority=0)
    await pg_transport.publish("hi", Event(kind="e2"), priority=2)

    lease = await pg_transport.claim("w", visibility=30, min_priority=2)
    assert lease is not None and lease.group_id == "hi"
    await pg_transport.ack(lease)

    assert await pg_transport.claim("w", visibility=30, min_priority=2) is None

    lo = await pg_transport.claim("w", visibility=30)
    assert lo is not None and lo.group_id == "lo"


async def test_async_postgres_transport_group_row_deleted_on_drain(pg_transport):
    """When a group drains, harel_ack must DELETE the transport_groups row so a
    re-publish can set a fresh (higher) priority.  Without the DELETE, ON CONFLICT
    DO NOTHING leaves the stale priority and claim(min_priority=2) returns None."""
    await pg_transport.publish("G", Event(kind="e1"), priority=0)
    lease = await pg_transport.claim("w", visibility=30)
    assert lease is not None
    await pg_transport.ack(lease)  # group drained: transport_groups row must be deleted

    await pg_transport.publish("G", Event(kind="e2"), priority=2)

    hi = await pg_transport.claim("w", visibility=30, min_priority=2)
    assert hi is not None and hi.group_id == "G"
