"""The flow interpreters (`harel.engine.flow`): a flow yields IO requests; `run_async`
serves them with coroutines, `run_inline` in the caller's thread. Same flow, same result."""

import asyncio
import threading
import time

import pytest

from harel.engine.flow import CallAction, call, parallel, run_async, run_inline


class SyncPort:
    def __init__(self):
        self.calls = []

    def put(self, key, value=None):
        self.calls.append((key, value, threading.current_thread().name))
        return f"{key}={value}"

    def boom(self):
        raise KeyError("missing")


class AsyncPort:
    def __init__(self):
        self.calls = []

    async def put(self, key, value=None):
        await asyncio.sleep(0)
        self.calls.append((key, value))
        return f"{key}={value}"

    async def boom(self):
        raise KeyError("missing")


def _flow():
    first = yield from call("p", "put", "a", value=1)
    second = yield from call("p", "put", "b")
    return [first, second]


def test_the_same_flow_gives_the_same_result_under_both_interpreters():
    inline = SyncPort()
    assert run_inline(_flow(), {"p": inline}) == ["a=1", "b=None"]
    assert asyncio.run(run_async(_flow(), {"p": AsyncPort()})) == ["a=1", "b=None"]
    assert all(name == threading.current_thread().name for *_, name in inline.calls)  # the caller's thread


def _catching():
    try:
        yield from call("p", "boom")
    except KeyError as exc:
        return f"caught {exc}"


def _not_catching():
    yield from call("p", "boom")


def test_a_failing_request_is_raised_inside_the_flow():
    assert run_inline(_catching(), {"p": SyncPort()}) == "caught 'missing'"
    assert asyncio.run(run_async(_catching(), {"p": AsyncPort()})) == "caught 'missing'"
    with pytest.raises(KeyError):
        run_inline(_not_catching(), {"p": SyncPort()})
    with pytest.raises(KeyError):
        asyncio.run(run_async(_not_catching(), {"p": AsyncPort()}))


def _sync_action(proxy, event, n):
    return (threading.current_thread().name, proxy, event, n)


async def _async_action(proxy, event, n):
    await asyncio.sleep(0)
    return ("async", proxy, event, n)


def _action_flow(fn):
    return (yield CallAction(fn, "proxy", "event", {"n": 3}))


def test_actions_sync_and_coroutine():
    caller = threading.current_thread().name
    assert run_inline(_action_flow(_sync_action), {}) == (caller, "proxy", "event", 3)
    name, *rest = asyncio.run(run_async(_action_flow(_sync_action), {}))
    assert name != caller and rest == ["proxy", "event", 3]  # off the loop, in the thread pool
    assert asyncio.run(run_async(_action_flow(_async_action), {})) == ("async", "proxy", "event", 3)
    with pytest.raises(TypeError, match="coroutine function"):
        run_inline(_action_flow(_async_action), {})


def _sleeper(seconds):
    yield CallAction(lambda proxy, event: time.sleep(seconds), None, None, {})
    return seconds


def _parallel_flow():
    return (yield from parallel([_sleeper(0.2), _sleeper(0.2), _sleeper(0.2)]))


def test_parallel_work_overlaps_with_coroutines_and_runs_in_order_inline():
    t0 = time.perf_counter()
    assert asyncio.run(run_async(_parallel_flow(), {})) == [0.2, 0.2, 0.2]
    assert time.perf_counter() - t0 < 0.5  # overlapped

    t0 = time.perf_counter()
    assert run_inline(_parallel_flow(), {}) == [0.2, 0.2, 0.2]
    assert time.perf_counter() - t0 >= 0.6  # one after the other


def test_an_empty_parallel_yields_nothing():
    def empty():
        return (yield from parallel([]))

    assert run_inline(empty(), {}) == []


def test_a_flow_must_yield_requests():
    def bad():
        yield 42

    with pytest.raises(TypeError, match="not an IO request"):
        run_inline(bad(), {})
    with pytest.raises(TypeError, match="not an IO request"):
        asyncio.run(run_async(bad(), {}))
