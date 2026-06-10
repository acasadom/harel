"""The async-native worker: AsyncWorker.run (the concurrent semaphore loop) + worker.py's
async backend builders. In-memory / aiosqlite / mem:// / aiomoto — no Docker."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from harel import worker
from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio_store import AsyncDictStore, AsyncDynamoDBStore, AsyncRedisStore, AsyncSqliteStore
from harel.engine.aio_transport import (
    AsyncInMemoryTransport,
    AsyncRedisTransport,
    AsyncSqliteTransport,
    AsyncSqsTransport,
)
from harel.engine.execution import Status
from harel.spec.states import Event

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


async def test_async_worker_run_drains_many_concurrently():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    ids = []
    for _ in range(25):
        exe = await runner.create(defn.id)  # parked at B
        await runner.send(exe.id, Event(kind="Go"))
        ids.append(exe.id)

    stop = asyncio.Event()
    worker_task = asyncio.create_task(runner.worker(concurrency=8).run(stop, idle_sleep=0.001))
    # wait until every execution has drained to C (DONE)
    for _ in range(1000):
        loaded = [await store.load(i) for i in ids]
        if all(e.status is Status.DONE for e in loaded):
            break
        await asyncio.sleep(0.002)
    stop.set()
    await worker_task

    finals = [await store.load(i) for i in ids]
    assert all(e.active_path == "C" and e.status is Status.DONE for e in finals)
    assert all(e.context["trace"] == ["A.enter", "B.enter", "C.enter"] for e in finals)


async def test_worker_build_store_async_sqlite(monkeypatch):
    monkeypatch.setenv("STM_STORE_BACKEND", "sqlite")
    monkeypatch.setenv("STM_STORE_DB", ":memory:")
    store = await worker.build_store_async()
    assert isinstance(store, AsyncSqliteStore)
    await store.close()


async def test_worker_build_store_async_redis(monkeypatch):
    monkeypatch.setenv("STM_STORE_BACKEND", "redis")
    monkeypatch.setenv("STM_REDIS_URL", "redis://localhost:6379/0")
    store = await worker.build_store_async()
    assert isinstance(store, AsyncRedisStore)


async def test_worker_build_transport_async(monkeypatch):
    monkeypatch.setenv("STM_TRANSPORT_BACKEND", "sqlite")
    monkeypatch.setenv("STM_TRANSPORT_DB", ":memory:")
    transport = await worker.build_transport_async()
    assert isinstance(transport, AsyncSqliteTransport)
    await transport.close()

    monkeypatch.setenv("STM_TRANSPORT_BACKEND", "redis")
    monkeypatch.setenv("STM_REDIS_URL", "redis://localhost:6379/0")
    assert isinstance(await worker.build_transport_async(), AsyncRedisTransport)


# ---------------------------------------------------------------------------
# Routing tests for the 5 backends wired in fix/async-worker-wiring
# ---------------------------------------------------------------------------


async def test_worker_build_store_async_dynamodb(monkeypatch):
    aiomoto = pytest.importorskip("aiomoto")

    monkeypatch.setenv("STM_STORE_BACKEND", "dynamodb")
    monkeypatch.setenv("STM_AWS_REGION", "us-east-1")
    async with aiomoto.mock_aws():
        store = await worker.build_store_async()
        assert isinstance(store, AsyncDynamoDBStore)
        await store.close()


async def test_worker_build_store_async_mongo(monkeypatch):
    monkeypatch.setenv("STM_STORE_BACKEND", "mongo")
    monkeypatch.setenv("STM_MONGO_URL", "mongodb://localhost:27017")
    from harel.engine.aio_store import AsyncMongoStore

    sentinel = object()
    with patch.object(AsyncMongoStore, "from_url", new=AsyncMock(return_value=sentinel)):
        result = await worker.build_store_async()
    assert result is sentinel


async def test_worker_build_store_async_rqlite(monkeypatch):
    monkeypatch.setenv("STM_STORE_BACKEND", "rqlite")
    monkeypatch.setenv("STM_RQLITE_URL", "http://localhost:4001")
    from harel.engine.aio_store import AsyncRqliteStore

    sentinel = object()
    with patch.object(AsyncRqliteStore, "from_url", new=AsyncMock(return_value=sentinel)):
        result = await worker.build_store_async()
    assert result is sentinel


async def test_worker_build_transport_async_mongo(monkeypatch):
    monkeypatch.setenv("STM_TRANSPORT_BACKEND", "mongo")
    monkeypatch.setenv("STM_MONGO_URL", "mongodb://localhost:27017")
    from harel.engine.aio_transport import AsyncMongoTransport

    sentinel = object()
    with patch.object(AsyncMongoTransport, "from_url", new=AsyncMock(return_value=sentinel)):
        result = await worker.build_transport_async()
    assert result is sentinel


async def test_worker_build_transport_async_rqlite(monkeypatch):
    monkeypatch.setenv("STM_TRANSPORT_BACKEND", "rqlite")
    monkeypatch.setenv("STM_RQLITE_URL", "http://localhost:4001")
    from harel.engine.aio_transport import AsyncRqliteTransport

    sentinel = object()
    with patch.object(AsyncRqliteTransport, "from_url", new=AsyncMock(return_value=sentinel)):
        result = await worker.build_transport_async()
    assert result is sentinel


async def test_worker_build_transport_async_sqs(monkeypatch):
    aiomoto = pytest.importorskip("aiomoto")

    monkeypatch.setenv("STM_TRANSPORT_BACKEND", "sqs")
    monkeypatch.setenv("STM_AWS_REGION", "us-east-1")
    monkeypatch.setenv("STM_SQS_QUEUE", "stm.fifo")
    monkeypatch.delenv("STM_SQS_ENDPOINT", raising=False)
    async with aiomoto.mock_aws():
        # create() makes the FIFO queue itself (idempotent), so no pre-seeding needed
        transport = await worker.build_transport_async()
        assert isinstance(transport, AsyncSqsTransport)
        await transport.close()
