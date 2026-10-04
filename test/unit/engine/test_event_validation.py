"""A declared event is checked against its declaration when it comes in — `process()` and
`send()`, sync and async — in the caller's stack: a missing required field or a value of the
wrong type raises `EventError`, and nothing runs, commits or queues. Undeclared kinds and the
engine's own events pass unchecked."""

import pytest

from harel import DictStore, EventError, definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

DSL = """
event Pay { token: string  amount: int  note: string? }
machine M {
  initial Unpaid
  state Unpaid {}
  final Paid success
  from Unpaid to Paid on Pay
}
"""


@pytest.fixture
def definitions():
    defn = definition_from_dsl(DSL, "M")
    return {defn.id: defn}


@pytest.mark.parametrize(
    "data,problem",
    [
        ({"amount": 5}, "missing required field 'token'"),
        ({"token": "t", "amount": "5"}, "field 'amount' must be int"),
        ({"token": "t", "amount": 5, "note": 3}, "field 'note' must be string"),
    ],
)
def test_process_refuses_an_event_that_does_not_fit(definitions, data, problem):
    store = DictStore()
    runner = DurableRunner(store, definitions)
    exe = runner.create("M")
    with pytest.raises(EventError, match=problem):
        runner.process(exe.id, Event(kind="Pay", data=data))
    stored = store.load(exe.id)
    assert (stored.active_path, stored.version) == ("Unpaid", exe.version)  # nothing ran


def test_process_takes_one_that_fits(definitions):
    runner = DurableRunner(DictStore(), definitions)
    exe = runner.create("M")
    exe = runner.process(exe.id, Event(kind="Pay", data={"token": "t", "amount": 5, "note": None}))
    assert exe.active_path == "Paid"


def test_undeclared_kinds_and_engine_events_pass(definitions):
    runner = DurableRunner(DictStore(), definitions)
    exe = runner.create("M")
    runner.process(exe.id, Event(kind="Unknown", data={"anything": 1}))  # no transition, no error
    assert runner.process(exe.id, Event(kind="Reset")).active_path == "Unpaid"


def test_send_refuses_it_before_it_queues(definitions):
    transport = InMemoryTransport()
    runner = DistributedRunner(DictStore(), transport, definitions)
    exe = runner.create("M")
    worker = runner.worker()
    while worker.step():
        pass
    with pytest.raises(EventError, match="missing required field 'token'"):
        runner.send(exe.id, Event(kind="Pay", data={"amount": 5}))
    assert transport.claim("w", 30) is None  # nothing was queued


async def test_the_async_runners_too(definitions):
    durable = AsyncDurableRunner(AsyncDictStore(), definitions)
    exe = await durable.create("M")
    with pytest.raises(EventError):
        await durable.process(exe.id, Event(kind="Pay", data={"amount": 5}))

    transport = AsyncInMemoryTransport()
    distributed = AsyncDistributedRunner(AsyncDictStore(), transport, definitions)
    exe = await distributed.create("M")
    while (lease := await transport.claim("w", 30)) is not None:  # drain the Start
        await transport.ack(lease)
    with pytest.raises(EventError):
        await distributed.send(exe.id, Event(kind="Pay", data={"token": 1, "amount": 5}))
    assert await transport.claim("w", 30) is None
