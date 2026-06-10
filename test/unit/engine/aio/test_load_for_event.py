"""`load_for_event` folds the worker's per-event dedupe check into the load (one round-trip
instead of load + is_processed). Verify it matches `(load, is_processed)` across the async
stores that run in-process (no Docker). The networked ones (postgres/rqlite/mongo) are exercised
by their `stack` integration tests, which drive the worker that uses `load_for_event`.
"""

import pytest

from harel.engine.execution import Execution


async def _check(store) -> None:
    e = Execution(definition_id="d", context={"k": 1})
    await store.commit(e, [], processed_event_id="e1")  # the execution + processed event "e1"

    exe1, processed1 = await store.load_for_event(e.id, "e1")
    assert exe1 is not None and exe1.id == e.id and processed1 is True

    exe2, processed2 = await store.load_for_event(e.id, "e2")  # same execution, unprocessed event
    assert exe2 is not None and processed2 is False

    missing, processed_missing = await store.load_for_event("nope", "e1")  # unknown execution
    assert missing is None and processed_missing is False

    # the combined result agrees with the two-call path it replaces
    assert processed1 == await store.is_processed(e.id, "e1")
    assert processed2 == await store.is_processed(e.id, "e2")


async def test_sqlite_load_for_event():
    from harel.engine.aio_store import AsyncSqliteStore

    store = await AsyncSqliteStore.create(":memory:")
    try:
        await _check(store)
    finally:
        await store.close()


async def test_redis_load_for_event():
    pytest.importorskip("fakeredis")
    import fakeredis.aioredis

    from harel.engine.aio_store import AsyncRedisStore

    store = AsyncRedisStore(fakeredis.aioredis.FakeRedis())
    try:
        await _check(store)
    finally:
        await store.close()


async def test_surreal_load_for_event():
    pytest.importorskip("surrealdb")
    from surrealdb import AsyncSurreal

    from harel.engine.aio_store import AsyncSurrealStore

    db = AsyncSurreal("mem://")
    await db.connect()
    await db.use("test", "test")
    try:
        await _check(AsyncSurrealStore(db))
    finally:
        await db.close()


async def test_dynamodb_load_for_event():
    aiomoto = pytest.importorskip("aiomoto")

    from harel.engine.aio_store import AsyncDynamoDBStore

    async with aiomoto.mock_aws():
        store = await AsyncDynamoDBStore.create(region="us-east-1")
        try:
            await _check(store)
        finally:
            await store.close()
