"""The driver logic as sans-IO flows, and the interpreters that run them.

The engine (`core.py`) is pure: it yields effects and never does IO. A *flow* takes the
same approach one level up, for the code that drives the engine (load, run the engine,
call the actions, commit, relay the outbox, publish): a generator that yields **IO
requests** and receives their results — it never touches a store, a transport or an
action itself. Written once, a flow then runs under whichever execution model the caller
wants, by the interpreter that serves its requests:

- `run_async(flow, ports)` — with coroutines: each request is awaited; a sync action
  runs in the default thread pool so it doesn't block the loop, and `Parallel` work runs
  concurrently (`asyncio.gather`);
- `run_inline(flow, ports)` — in the caller's own thread, without an event loop: each
  request is a plain call, and `Parallel` work runs one after the other.

The semantics — what the flow does, in which order, what it commits — are the flow's,
identical under both. A request that raises is thrown back into the flow at the `yield`
that issued it, so a flow handles IO errors (an action raising, a `StoreConflict`) with
an ordinary `try`.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Generator

# a flow: yields requests, receives each result, returns its value
Flow = Generator[Any, Any, Any]


@dataclass(frozen=True)
class Call:
    """Call `method` on the port named `port` (`"store"`, `"transport"`) with `args`."""

    port: str
    method: str
    args: tuple = ()
    kwargs: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CallAction:
    """Call the user action `fn(proxy, event, **inputs)` — a plain function or a coroutine
    function — and return what it returns."""

    fn: Callable[..., Any]
    proxy: Any
    event: Any
    inputs: dict


@dataclass(frozen=True)
class Parallel:
    """Run independent sub-flows — concurrently if the interpreter can — and return their
    results, in order. They must not depend on each other's effects."""

    flows: tuple


@dataclass(frozen=True)
class Await:
    """Await `awaitable` — what a user callback that may be a coroutine function (a purge's
    `archive`) returned — and return its result. Only an interpreter with an event loop can."""

    awaitable: Any


def call(port: str, method: str, *args: Any, **kwargs: Any) -> Flow:
    """`yield from call("store", "load", eid)`: one IO call from inside a flow."""
    return (yield Call(port, method, args, kwargs))


def parallel(flows: list) -> Flow:
    """`yield from parallel([...])`: run independent sub-flows; their results, in order."""
    if not flows:
        return []
    return (yield Parallel(tuple(flows)))


# --- interpreters -------------------------------------------------------------------------
async def run_async(flow: Flow, ports: dict[str, Any]) -> Any:
    """Run `flow` with coroutines: `ports` hold async stores/transports."""
    try:
        request = next(flow)
        while True:
            try:
                result = await _serve_async(request, ports)
            except Exception as exc:
                request = flow.throw(exc)
            else:
                request = flow.send(result)
    except StopIteration as stop:
        return stop.value


async def _serve_async(request: Any, ports: dict[str, Any]) -> Any:
    if isinstance(request, Call):
        return await getattr(ports[request.port], request.method)(*request.args, **request.kwargs)
    if isinstance(request, CallAction):
        if inspect.iscoroutinefunction(request.fn):
            return await request.fn(request.proxy, request.event, **request.inputs)
        # a sync action goes to the default thread pool, so a blocking call doesn't freeze
        # the loop (FastAPI's sync-handler model)
        loop = asyncio.get_running_loop()
        bound = functools.partial(request.fn, request.proxy, request.event, **request.inputs)
        return await loop.run_in_executor(None, bound)
    if isinstance(request, Parallel):
        return list(await asyncio.gather(*[run_async(f, ports) for f in request.flows]))
    if isinstance(request, Await):
        return await request.awaitable
    raise TypeError(f"a flow yielded {request!r}, which is not an IO request")


def run_inline(flow: Flow, ports: dict[str, Any]) -> Any:
    """Run `flow` in the caller's thread, without an event loop: `ports` hold sync
    stores/transports. A coroutine action (or `Await`) is refused — there is no loop to await
    it on."""
    try:
        request = next(flow)
        while True:
            try:
                result = _serve_inline(request, ports)
            except Exception as exc:
                request = flow.throw(exc)
            else:
                request = flow.send(result)
    except StopIteration as stop:
        return stop.value


def _serve_inline(request: Any, ports: dict[str, Any]) -> Any:
    if isinstance(request, Call):
        return getattr(ports[request.port], request.method)(*request.args, **request.kwargs)
    if isinstance(request, CallAction):
        if inspect.iscoroutinefunction(request.fn):
            raise TypeError(
                f"action {getattr(request.fn, '__name__', request.fn)!r} is a coroutine function: "
                "it needs an event loop, which running in the caller's thread doesn't have"
            )
        return request.fn(request.proxy, request.event, **request.inputs)
    if isinstance(request, Parallel):
        return [run_inline(f, ports) for f in request.flows]
    if isinstance(request, Await):
        close = getattr(request.awaitable, "close", None)
        if close is not None:
            close()  # never awaited: closed, so it doesn't warn
        raise TypeError(
            "a callback returned an awaitable (a coroutine function?): it needs an event loop, "
            "which running in the caller's thread doesn't have"
        )
    raise TypeError(f"a flow yielded {request!r}, which is not an IO request")
