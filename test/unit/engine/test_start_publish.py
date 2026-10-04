"""A new execution's `Start` publish failing: by default `create()` tolerates it (the `Start`
committed to the outbox stays queued for a later flush); a transport that writes in the
store's transaction (`shares_store_transaction = True`) has it reach the caller instead, under
every execution model."""

import pytest

from harel import DictStore, definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.distributed import DistributedRunner
from harel.engine.transport import InMemoryTransport

DSL = """
machine M {
  initial A
  state A {}
}
"""


class PublishDown(RuntimeError):
    pass


class FailingPublish(InMemoryTransport):
    def __init__(self, shares: bool) -> None:
        super().__init__()
        self.shares_store_transaction = shares

    def publish(self, group_id, event, priority=0):
        raise PublishDown("publish failed")


class AsyncFailingPublish(AsyncInMemoryTransport):
    def __init__(self, shares: bool) -> None:
        super().__init__()
        self.shares_store_transaction = shares

    async def publish(self, group_id, event, priority=0):
        raise PublishDown("publish failed")


def _defn():
    defn = definition_from_dsl(DSL, "M")
    return {defn.id: defn}, defn.id


@pytest.mark.parametrize("execution", ["inline", "background"])
def test_by_default_the_start_stays_queued(execution):
    definitions, defn_id = _defn()
    store = DictStore()
    runner = DistributedRunner(store, FailingPublish(shares=False), definitions, execution=execution)
    exe = runner.create(defn_id)
    assert store.load(exe.id) is not None
    assert [e.event.kind for e in store.pending_outbox()] == ["Start"]


@pytest.mark.parametrize("execution", ["inline", "background"])
def test_a_transport_in_the_store_transaction_has_it_raise(execution):
    definitions, defn_id = _defn()
    runner = DistributedRunner(DictStore(), FailingPublish(shares=True), definitions, execution=execution)
    with pytest.raises(PublishDown):
        runner.create(defn_id)


async def test_the_async_runner_too():
    definitions, defn_id = _defn()
    tolerant = AsyncDistributedRunner(AsyncDictStore(), AsyncFailingPublish(shares=False), definitions)
    await tolerant.create(defn_id)
    strict = AsyncDistributedRunner(AsyncDictStore(), AsyncFailingPublish(shares=True), definitions)
    with pytest.raises(PublishDown):
        await strict.create(defn_id)
