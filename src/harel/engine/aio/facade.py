"""The sync→async bridge for the public sync API.

The sync runners (`Driver`/`DurableRunner`/`DistributedRunner`/`Worker`), in their default
`execution="background"` model, run on the async core. They dispatch each sync call to a single **process-wide** anyio BlockingPortal
(one background event loop on a dedicated thread, lazily started, closed at interpreter
exit) via `run(coro_fn, *args)`. One shared loop — not one thread per runner — so a test
suite that builds many runners doesn't leak threads, and async connection pools (later
phases) stay bound to that one loop.

Calling a sync facade method from *within* a running event loop is refused with a clear
error (use the async API instead), the way `asgiref.async_to_sync` does — so we never nest
or fight a caller's loop.

A sync `ExecutionStore`/`Transport` passed to a facade (the common case — `DictStore`,
`SqliteStore("x.db")`) is wrapped so the async engine can await it, delegating to the SAME
object the caller holds (so code that introspects the store still sees its state). An async
backend passed directly is used as-is.
"""

from __future__ import annotations

import asyncio
import atexit
import functools
import inspect
import threading
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, TypeVar

from anyio.from_thread import BlockingPortal, start_blocking_portal

if TYPE_CHECKING:
    from harel.engine.execution import Execution
    from harel.engine.store import (
        DefinitionRecord,
        ExecutionStore,
        OutboxEntry,
        PendingWork,
        SpawnEntry,
        Step,
        TimerOp,
        WorkerRecord,
    )
    from harel.engine.transport import Backlog, Lease, Transport
    from harel.spec.states import Event

T = TypeVar("T")

_portal_cm: Any = None
_portal: BlockingPortal | None = None
_portal_lock = threading.Lock()


def _guard_no_running_loop() -> None:
    """Refuse a sync-facade call made from inside a running event loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return  # no loop in this thread -> safe
    raise RuntimeError(
        "A synchronous harel API was called from within a running event loop. "
        "Use the async API (harel.engine.aio) directly instead."
    )


def portal() -> BlockingPortal:
    """The shared background-loop portal, started on first use and closed at exit."""
    global _portal_cm, _portal
    if _portal is None:
        _guard_no_running_loop()
        with _portal_lock:
            if _portal is None:
                _portal_cm = start_blocking_portal(backend="asyncio")
                _portal = _portal_cm.__enter__()
                atexit.register(_close_portal)
    return _portal


def _close_portal() -> None:
    global _portal_cm, _portal
    if _portal_cm is not None:
        try:
            _portal_cm.__exit__(None, None, None)
        finally:
            _portal_cm = None
            _portal = None


def run(coro_fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
    """Run `coro_fn(*args, **kwargs)` on the shared loop and block until it completes (sync
    bridge). `BlockingPortal.call` takes only positional args, so kwargs ride via partial.
    Refused from inside a running event loop, on every call: blocking there would freeze that
    loop for the whole call."""
    _guard_no_running_loop()
    if kwargs:
        return portal().call(functools.partial(coro_fn, *args, **kwargs))
    return portal().call(coro_fn, *args)


# --- sync backend adapters (delegate to the same object the caller holds) -----------------


def _is_async(obj: Any) -> bool:
    """A backend is async if its `load`/`claim` is a coroutine function."""
    probe = getattr(obj, "load", None) or getattr(obj, "claim", None)
    return inspect.iscoroutinefunction(probe)


class _AsyncStoreAdapter:
    """Expose a sync `ExecutionStore` through the async `AsyncExecutionStore` interface,
    delegating to the wrapped sync store (same object → introspection by the caller works).
    The sync methods are called directly on the loop thread; in-memory backends don't block,
    durable ones do — acceptable for the single-threaded embedded facade path (the concurrency
    win is the async Worker, not this facade)."""

    def __init__(self, store: ExecutionStore) -> None:
        self._s = store

    @property
    def trace_max(self) -> int:
        return getattr(self._s, "trace_max", 0)

    @trace_max.setter
    def trace_max(self, value: int) -> None:
        if hasattr(self._s, "trace_max"):
            self._s.trace_max = value

    async def load(self, execution_id: str) -> Optional[Execution]:
        return self._s.load(execution_id)

    async def save(self, exe: Execution) -> None:
        self._s.save(exe)

    async def commit(
        self,
        exe: Execution,
        emits: list[tuple[Optional[str], Event]],
        processed_event_id: Optional[str] = None,
        timers: tuple[TimerOp, ...] = (),
        spawns: tuple[tuple[str, str, dict[str, Any]], ...] = (),
        trace: Optional[dict[str, Any]] = None,
        step: Optional[Step] = None,
    ) -> list[int]:
        return self._s.commit(
            exe,
            emits,
            processed_event_id=processed_event_id,
            timers=timers,
            spawns=spawns,
            trace=trace,
            step=step,
        )

    async def read_trace(self, execution_id: str) -> list[dict[str, Any]]:
        return self._s.read_trace(execution_id)

    async def append_trace(self, execution_id: str, entry: dict[str, Any]) -> None:
        # not part of the store protocol: the demo/test seam the in-memory stores have
        getattr(self._s, "append_trace")(execution_id, entry)

    async def is_processed(self, execution_id: str, event_id: str) -> bool:
        return self._s.is_processed(execution_id, event_id)

    async def pending_outbox(self) -> list[OutboxEntry]:
        return self._s.pending_outbox()

    async def ack_outbox(self, seq: int) -> None:
        self._s.ack_outbox(seq)

    async def pending_spawns(self) -> list[SpawnEntry]:
        return self._s.pending_spawns()

    async def ack_spawn(self, seq: int) -> None:
        self._s.ack_spawn(seq)

    async def due_timers(self, now: float) -> list[tuple[str, str, float]]:
        return self._s.due_timers(now)

    async def delete_timer(self, execution_id: str, path: str, fire_at: float) -> None:
        self._s.delete_timer(execution_id, path, fire_at)

    async def pending_work(self, execution_id: str) -> PendingWork:
        return self._s.pending_work(execution_id)

    async def record_worker(self, record: WorkerRecord) -> None:
        self._s.record_worker(record)

    async def list_workers(self) -> list[WorkerRecord]:
        return self._s.list_workers()

    async def forget_worker(self, worker_id: str) -> None:
        self._s.forget_worker(worker_id)

    async def ids_with_prefix(self, prefix: str) -> list[str]:
        return self._s.ids_with_prefix(prefix)

    async def purge(self, execution_id: str, expected_version: int) -> bool:
        return self._s.purge(execution_id, expected_version)

    async def publish_definition(self, record: DefinitionRecord) -> DefinitionRecord:
        return self._s.publish_definition(record)

    async def load_definition(self, definition_id: str, version: int) -> Optional[DefinitionRecord]:
        return self._s.load_definition(definition_id, version)

    async def close(self) -> None:
        self._s.close()


class _AsyncTransportAdapter:
    """Expose a sync `Transport` through the async `AsyncTransport` interface."""

    def __init__(self, transport: Transport) -> None:
        self._t = transport

    @property
    def shares_store_transaction(self) -> bool:
        return getattr(self._t, "shares_store_transaction", False)

    async def publish(
        self, group_id: str, event: Event, priority: int = 0, routing_key: Optional[str] = None
    ) -> None:
        self._t.publish(group_id, event, priority, routing_key=routing_key)

    async def claim(
        self,
        worker_id: str,
        visibility: float,
        min_priority: int = 0,
        routing_keys: Optional[frozenset[str]] = None,
    ) -> Optional[Lease]:
        if routing_keys is None:  # as the worker asks: a transport without routing still serves it
            return self._t.claim(worker_id, visibility, min_priority)
        return self._t.claim(worker_id, visibility, min_priority, routing_keys=routing_keys)

    async def ack(self, lease: Lease) -> None:
        self._t.ack(lease)

    async def nack(self, lease: Lease, delay: float = 0.0) -> None:
        self._t.nack(lease, delay)

    async def backlog(self) -> Backlog:
        return self._t.backlog()

    async def close(self) -> None:
        self._t.close()


def as_async_store(store: Any) -> Any:
    """The store as an async store: itself if already async, else a sync→async adapter."""
    return store if _is_async(store) else _AsyncStoreAdapter(store)


def as_async_transport(transport: Any) -> Any:
    return transport if _is_async(transport) else _AsyncTransportAdapter(transport)
