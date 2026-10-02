"""Async distributed pipeline over in-memory async backends.

Drives the full AsyncDistributedRunner + AsyncWorker loop (claim→route→ack) over
`AsyncDictStore` + `AsyncInMemoryTransport`, for a flat machine and an orthogonal one
(fork → two regions → join). Mirrors the sync `test_redis_transport` pipeline tests but
fully async (single worker draining deterministically via `step()`).
"""

import logging

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.execution import Status
from harel.spec.states import Event


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
    state A {{
      initial A1
      state A1 {{ on enter {_h("A1")} }}
      state A2 {{ on enter {_h("A2")} }}
      from A1 to A2 on Go
    }}
    state B {{
      initial B1
      state B1 {{ on enter {_h("B1")} }}
      state B2 {{ on enter {_h("B2")} }}
      from B1 to B2 on Go
    }}
  }}
  state Done {{ on enter {_h("Done")} }}
  from Fork to Done
}}
"""


async def _drain(runner: AsyncDistributedRunner) -> None:
    w = runner.worker()
    while await w.step():
        pass


async def test_async_pipeline_flat():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    exe = await runner.create(defn.id)  # start_on_create=True (default)
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)

    final = await store.load(exe.id)
    assert final.active_path == "C"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


async def test_async_pipeline_orthogonal():
    defn = definition_from_dsl(ORTHO, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    exe = await runner.create(defn.id)  # start_on_create=True (default)
    await _drain(runner)  # fork happens on start: parent parks at Fork, two regions spawned
    exe = await store.load(exe.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)

    final = await store.load(exe.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    regions = [await store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]


class _FlakyOncePublish(AsyncInMemoryTransport):
    """Fails the very next `publish()` once, then behaves normally — simulates a
    transient transport outage between create()'s commit and its delivery attempt."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = True

    async def publish(self, *args, **kwargs):
        if self.armed:
            self.armed = False
            raise RuntimeError("transient transport outage")
        return await super().publish(*args, **kwargs)


async def test_create_survives_a_transient_publish_failure(caplog):
    """create() commits the Start into the durable outbox atomically with the
    Execution's own first save, so a failed immediate delivery only delays it
    (recoverable by any later flush, anywhere in the fleet) rather than losing it
    — and create() must still return the Execution to the caller either way, with
    its id, regardless of whether the immediate delivery attempt succeeded."""
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    transport = _FlakyOncePublish()
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})

    with caplog.at_level(logging.WARNING):
        exe = await runner.create(defn.id)  # the flaky publish() raises internally...
    assert exe.id  # ...but create() still returns the Execution, id included
    assert exe.status is Status.PENDING
    assert "could not immediately publish the Start" in caplog.text

    # nothing claimable yet — the one and only publish attempt failed
    assert await runner.worker().step() is False

    # still PENDING (the commit succeeded, only the publish attempt failed), so a
    # caller-level retry via start() is exactly what the docstring promises: safe,
    # since the engine only ever acts on a Start while status is still PENDING
    await runner.start(exe.id)

    await _drain(runner)
    final = await store.load(exe.id)
    assert final.status is Status.RUNNING  # started, and parked at B awaiting Go
    assert final.active_path == "B"
    assert final.context["trace"] == ["A.enter", "B.enter"]


async def test_create_does_not_drain_unrelated_backlog():
    """create()'s best-effort Start delivery must publish only its own, already-
    known entry — not call the generic _flush(), which drains the store's ENTIRE
    pending outbox/spawns fleet-wide. Piggybacking on that global scan would make
    every create() pay for O(backlog) work to deliver one known entry, and would
    misattribute a failure publishing someone else's backlog to this exe's Start."""
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    # an unrelated, already-pending outbox entry — as if some other execution's
    # own delivery attempt failed earlier and is still awaiting a flush
    other = await store.load((await runner.create(defn.id, start_on_create=False)).id)
    await store.commit(other, [(other.id, Event(kind="Marker"))])
    assert len(await store.pending_outbox()) == 1

    exe = await runner.create(defn.id)  # healthy transport — delivers its own Start fine
    await _drain(runner)
    assert (await store.load(exe.id)).status is Status.RUNNING

    # the unrelated entry was never touched (published or acked) by create()'s
    # targeted delivery of its own, unrelated Start
    marker_entries = [e for e in await store.pending_outbox() if e.event.kind == "Marker"]
    assert len(marker_entries) == 1


async def _relay(runner: AsyncDistributedRunner, execution_id: str) -> None:
    """A flush, as any later step anywhere in the fleet runs one."""
    driver, _ = await runner._driver_for(execution_id)
    await driver._flush()


async def test_create_and_start_leave_no_start_in_the_outbox():
    """A Start delivered directly is acked, as the outbox relay acks what it delivers:
    nothing is left for a later flush to publish again."""
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    for _ in range(3):
        await runner.create(defn.id)
    later = await runner.create(defn.id, start_on_create=False)
    await runner.start(later.id)
    assert await store.pending_outbox() == []

    await _drain(runner)
    assert await store.pending_outbox() == []


async def test_a_failed_publish_leaves_the_start_for_the_relay(caplog):
    """When the direct publish fails, the Start stays in the outbox, and a later flush
    delivers (and acks) it."""
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, _FlakyOncePublish(), {defn.id: defn})

    with caplog.at_level(logging.WARNING):
        exe = await runner.create(defn.id)
    assert [(e.target_id, e.event.kind) for e in await store.pending_outbox()] == [(exe.id, "Start")]

    await _relay(runner, exe.id)
    assert await store.pending_outbox() == []
    await _drain(runner)
    assert (await store.load(exe.id)).active_path == "B"


class _AckFailsOnce(AsyncDictStore):
    def __init__(self) -> None:
        super().__init__()
        self.armed = True

    async def ack_outbox(self, seq: int) -> None:
        if self.armed:
            self.armed = False
            raise RuntimeError("store outage")
        await super().ack_outbox(seq)


async def test_a_failed_ack_does_not_raise_and_the_republished_start_is_dropped(caplog):
    """A Start published but not acked is published again by a later flush; the
    execution processes it once."""
    defn = definition_from_dsl(FLAT, "M")
    store = _AckFailsOnce()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    with caplog.at_level(logging.WARNING):
        exe = await runner.create(defn.id)  # published; the ack raises internally
    assert "could not ack its outbox entry" in caplog.text
    assert len(await store.pending_outbox()) == 1

    await _relay(runner, exe.id)  # publishes the same Start again
    assert await store.pending_outbox() == []
    await _drain(runner)
    final = await store.load(exe.id)
    assert final.active_path == "B"
    assert final.context["trace"] == ["A.enter", "B.enter"]  # started once


async def test_start_survives_a_transient_publish_failure(caplog):
    """start() shares create()'s _persist_start helper, so it gets the same
    durability guarantee: a failed immediate delivery leaves the Start durably
    committed to the outbox (not lost), and start() itself must not raise — the
    caller already has the execution_id, so a plain retry is always available."""
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    transport = _FlakyOncePublish()
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
    exe = await runner.create(defn.id, start_on_create=False)

    with caplog.at_level(logging.WARNING):
        await runner.start(exe.id)  # the flaky publish() raises internally...
    assert (await store.load(exe.id)).status is Status.PENDING  # ...but doesn't raise
    assert "could not immediately publish the Start" in caplog.text
    assert await runner.worker().step() is False  # nothing claimable yet

    await runner.start(exe.id)  # retry, now that the transport works again
    await _drain(runner)
    final = await store.load(exe.id)
    assert final.status is Status.RUNNING
    assert final.active_path == "B"


async def test_async_send_refuses_a_caller_supplied_start_event():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})
    exe = await runner.create(defn.id, start_on_create=False)

    with pytest.raises(ValueError):
        await runner.send(exe.id, Event(kind="Start"))

    assert await runner.worker().step() is False
    assert (await store.load(exe.id)).status is Status.PENDING


async def test_async_start_can_seed_the_context_with_its_own_data():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})
    exe = await runner.create(defn.id, start_on_create=False)

    await runner.start(exe.id, data={"tenant": "acme"})
    await _drain(runner)

    started = await store.load(exe.id)
    assert started.status is Status.RUNNING
    assert started.context["tenant"] == "acme"


async def test_a_tracing_runner_records_each_step_with_its_event():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn}, trace=True)

    exe = await runner.create(defn.id)
    await _drain(runner)
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)

    steps = await store.read_trace(exe.id)
    assert [(s["event_kind"], s["from_path"], s["to_path"]) for s in steps] == [
        ("Start", None, "B"),
        ("Go", "B", "C"),
    ]
    assert [s["context_out"]["trace"] for s in steps] == [
        ["A.enter", "B.enter"],
        ["A.enter", "B.enter", "C.enter"],
    ]


class _LoadFailsOnce(AsyncDictStore):
    """Once armed, the next `load` raises — a store outage while a message is handled."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False

    async def load(self, execution_id):
        if self.armed:
            self.armed = False
            raise RuntimeError("store outage")
        return await super().load(execution_id)


class _NackFails(AsyncInMemoryTransport):
    async def nack(self, lease, delay: float = 0.0) -> None:
        raise RuntimeError("transport outage")


async def _run_until(worker, done, timeout: float = 5.0) -> None:
    import asyncio

    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop, idle_sleep=0.001))
    try:
        async with asyncio.timeout(timeout):
            while not await done():
                await asyncio.sleep(0.005)
    finally:
        stop.set()
        await task


async def test_run_logs_a_failed_message_and_retries_it(caplog):
    """A message whose handling raises is logged with its traceback and nacked to come back
    after `suspend_recheck`; the execution then moves on."""
    defn = definition_from_dsl(FLAT, "M")
    store = _LoadFailsOnce()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})
    exe = await runner.create(defn.id)
    await _drain(runner)
    await runner.send(exe.id, Event(kind="Go"))
    store.armed = True  # the worker's load of the execution for that Go fails

    worker = AsyncWorker(store, runner.transport, {defn.id: defn}, suspend_recheck=0.01)
    with caplog.at_level(logging.ERROR):
        await _run_until(worker, lambda: _is_at(store, exe.id, "C"))

    failures = [r for r in caplog.records if "failed handling Go event" in r.getMessage()]
    assert len(failures) == 1 and failures[0].exc_info is not None
    assert "store outage" in caplog.text


async def test_run_logs_a_failed_nack_and_the_lease_brings_the_message_back(caplog):
    defn = definition_from_dsl(FLAT, "M")
    store = _LoadFailsOnce()
    transport = _NackFails()
    runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
    exe = await runner.create(defn.id)
    await _drain(runner)
    await runner.send(exe.id, Event(kind="Go"))
    store.armed = True  # the worker's load of the execution for that Go fails

    worker = AsyncWorker(store, transport, {defn.id: defn}, visibility=0.05)
    with caplog.at_level(logging.ERROR):
        await _run_until(worker, lambda: _is_at(store, exe.id, "C"))

    assert "could not nack Go event" in caplog.text
    assert "transport outage" in caplog.text


async def _is_at(store, execution_id: str, path: str) -> bool:
    return (await AsyncDictStore.load(store, execution_id)).active_path == path  # not the armed load
