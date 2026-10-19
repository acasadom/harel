"""A worker's loop keeps going whatever one step does: a message whose handling raises is logged
and comes back after `suspend_recheck`; a claim or a timer sweep that raises is logged and the
loop carries on. Only `stop` ends it — the sync `Worker.run`, in both execution models, and the
async `AsyncWorker.run`."""

import asyncio
import logging
import threading
import time

import pytest

from harel import DictStore, definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.transport import InMemoryTransport

FLAT = """
event Go {}
machine m { initial A state A {} final B done from A to B on Go }
"""
TIMED = """
machine t { initial A state A { timeout 1 } final B done from A to B on Timeout }
"""


class _Outage(RuntimeError):
    pass


class _FlakyStore(DictStore):
    """`load` or `due_timers` raises once, when armed."""

    def __init__(self):
        super().__init__()
        self.fail_load = self.fail_timers = False

    def load(self, execution_id):
        if self.fail_load:
            self.fail_load = False
            raise _Outage("store outage on load")
        return super().load(execution_id)

    def due_timers(self, now):
        if self.fail_timers:
            self.fail_timers = False
            raise _Outage("store outage on due_timers")
        return super().due_timers(now)


class _FlakyTransport(InMemoryTransport):
    """`claim` raises once, when armed."""

    def __init__(self):
        super().__init__()
        self.fail_claim = False

    def claim(self, *args, **kwargs):
        if self.fail_claim:
            self.fail_claim = False
            raise _Outage("transport outage on claim")
        return super().claim(*args, **kwargs)


def _runner(execution, source=FLAT, name="m"):
    defn = definition_from_dsl(source, name)
    store, transport = _FlakyStore(), _FlakyTransport()
    return DistributedRunner(store, transport, {defn.id: defn}, execution=execution), store, transport


def _run_until(worker, done, timeout=5.0):
    stop = threading.Event()
    thread = threading.Thread(target=worker.run, args=(stop,), kwargs={"idle_sleep": 0.001})
    thread.start()
    try:
        deadline = time.time() + timeout
        while time.time() < deadline and not done():
            time.sleep(0.005)
    finally:
        stop.set()
        thread.join(timeout)
    assert not thread.is_alive(), "the loop stops when asked"
    return done()


@pytest.fixture(params=["inline", "background"])
def execution(request):
    return request.param


def test_a_message_whose_handling_raises_comes_back(execution, caplog):
    runner, store, _ = _runner(execution)
    exe = runner.create("m")
    store.fail_load = True  # the worker's load for the Start fails once
    worker = runner.worker(suspend_recheck=0.01)
    with caplog.at_level(logging.ERROR):
        assert _run_until(worker, lambda: store._by_id[exe.id].status is Status.RUNNING)
    assert "failed handling Start event" in caplog.text and "store outage on load" in caplog.text


def test_a_claim_that_raises_doesnt_stop_the_loop(execution, caplog):
    runner, store, transport = _runner(execution)
    exe = runner.create("m")
    transport.fail_claim = True
    worker = runner.worker(suspend_recheck=0.01)
    with caplog.at_level(logging.ERROR):
        assert _run_until(worker, lambda: store._by_id[exe.id].status is Status.RUNNING)
    assert "could not claim" in caplog.text


def test_a_timer_sweep_that_raises_doesnt_stop_the_loop(execution, caplog):
    runner, store, _ = _runner(execution, TIMED, "t")
    exe = runner.create("t")
    worker = runner.worker(suspend_recheck=0.01)
    assert _run_until(worker, lambda: store._by_id[exe.id].status is Status.RUNNING)
    store.fail_timers = True
    with caplog.at_level(logging.ERROR):
        assert _run_until(worker, lambda: store._by_id[exe.id].status is Status.DONE)  # 1s timeout
    assert "could not fire the due timers" in caplog.text


def test_a_version_the_worker_cant_run_is_left_and_the_loop_goes_on(execution, caplog):
    runner, store, _ = _runner(execution)
    exe = runner.create("m")
    stored = store.load(exe.id)
    stored.definition_version = 7  # a version nobody here has, nor the store
    store.save(stored)
    worker = runner.worker(suspend_recheck=0.01)
    other = runner.create("m")
    with caplog.at_level(logging.ERROR):
        assert _run_until(worker, lambda: store._by_id[other.id].status is Status.RUNNING)
    assert "hasn't loaded" in caplog.text
    assert store.load(exe.id).status is Status.PENDING


async def test_the_async_loop_survives_a_claim_that_raises(caplog):
    class _FlakyAsync(AsyncInMemoryTransport):
        fail = True

        async def claim(self, *args, **kwargs):
            if self.fail:
                self.fail = False
                raise _Outage("transport outage on claim")
            return await super().claim(*args, **kwargs)

    defn = definition_from_dsl(FLAT, "m")
    store, transport = AsyncDictStore(), _FlakyAsync()
    exe = await AsyncDistributedRunner(store, transport, {defn.id: defn}).create("m")
    worker = AsyncWorker(store, transport, {defn.id: defn}, suspend_recheck=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop, idle_sleep=0.001))
    with caplog.at_level(logging.ERROR):
        for _ in range(500):
            if (await store.load(exe.id)).status is Status.RUNNING:
                break
            await asyncio.sleep(0.005)
    stop.set()
    await task
    assert (await store.load(exe.id)).status is Status.RUNNING
    assert "could not claim" in caplog.text


# --- the heartbeat ---------------------------------------------------------------------------


def test_a_running_worker_has_its_heartbeat_in_the_store(execution):
    runner, store, _ = _runner(execution)
    worker = runner.worker("w-1", machines={"m"}, heartbeat=0.01)
    seen = []

    def recorded():
        seen.extend(store.list_workers())
        return bool(seen)

    assert _run_until(worker, recorded)
    [record] = seen[:1]
    assert (record.worker_id, record.machines, record.heartbeat) == ("w-1", ("m",), 0.01)
    assert store.list_workers() == []  # forgotten once stopped


def test_a_heartbeat_that_fails_doesnt_stop_the_loop(execution, caplog):
    runner, store, _ = _runner(execution)

    def boom(record):
        raise _Outage("store outage on record_worker")

    store.record_worker = boom
    exe = runner.create("m")
    with caplog.at_level(logging.ERROR):
        assert _run_until(
            runner.worker(heartbeat=0.01), lambda: store._by_id[exe.id].status is Status.RUNNING
        )
    assert "could not record its heartbeat" in caplog.text


async def test_the_async_loop_beats_and_forgets_too():
    defn = definition_from_dsl(FLAT, "m")
    store = AsyncDictStore()
    worker = AsyncWorker(store, AsyncInMemoryTransport(), {defn.id: defn}, worker_id="w-a", heartbeat=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop, idle_sleep=0.001))
    for _ in range(200):
        if await store.list_workers():
            break
        await asyncio.sleep(0.005)
    [record] = await store.list_workers()
    assert (record.worker_id, record.machines) == ("w-a", None)
    first = record.seen_at
    await asyncio.sleep(0.05)
    assert (await store.list_workers())[0].seen_at > first  # it keeps beating
    stop.set()
    await task
    assert await store.list_workers() == []
