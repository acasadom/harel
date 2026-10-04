"""Async throughput benchmark — measures events/sec vs HAREL_CONCURRENCY.

Configures the backend from the same env vars as worker.py, so you can point it
at Redis, Postgres, or any other backend without touching the script.

Usage:
    HAREL_STORE_BACKEND=redis HAREL_REDIS_URL=redis://localhost:6379/0 \\
        python bench/bench_async.py

    HAREL_STORE_BACKEND=postgres HAREL_TRANSPORT_BACKEND=postgres \\
    HAREL_POSTGRES_DSN=postgresql://stm:stm@localhost:5432/stm \\
        python bench/bench_async.py --n-executions 100 --concurrency 1,4,16,64,256

The machine used has one async IO-bound action (anyio.sleep) so the benchmark
measures the real async speedup — not pure CPU overhead. Each execution takes two
events: its `Start` (which enters Working and runs the action) and a `Finish`.

Options:
    --n-executions N    number of parallel machines per run (default 200)
    --concurrency C     comma-separated concurrency levels to sweep (default 1,4,16,64,256)
    --no-sleep          skip the async sleep in the action (measures pure overhead)
    --pool-size N       Postgres connection pool size (default: (concurrency + producers) * 2 + 4)
    --e2e               time the enqueue too (end-to-end), not just the drain
    --producers K       with --e2e: enqueue from K concurrent coroutines (default 1)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any

# make sure src/ is importable when run directly from the repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker
from harel.spec.states import Event

# ---------------------------------------------------------------------------
# Benchmark machine — Start enters Working (async IO action), Finish ends it
# ---------------------------------------------------------------------------

_DSL = """
event Finish {}

machine Bench {
  initial Working
  state Working { on enter bench_actions.sleep_io }
  final Done success {}
  from Working to Done on Finish
}
"""

EVENTS_PER_EXECUTION = 2  # Start + Finish

# ---------------------------------------------------------------------------
# Action module injected at runtime
# ---------------------------------------------------------------------------


def _make_actions(use_sleep: bool) -> Any:
    """The `bench_actions` module the machine binds to; `calls` counts the action's runs."""
    import types

    import anyio

    mod = types.SimpleNamespace(calls=0)

    async def sleep_io(stm: Any, event: Event) -> None:
        mod.calls += 1
        if use_sleep:
            await anyio.sleep(0.01)

    mod.sleep_io = sleep_io
    return mod


async def _create_all(runner: AsyncDistributedRunner, defn: Any, n: int) -> list[str]:
    """Create n executions without starting them: nothing reaches the transport yet."""
    return [(await runner.create(defn.id, start_on_create=False)).id for _ in range(n)]


async def _enqueue_all(runner: AsyncDistributedRunner, exe_ids: list[str], producers: int = 1) -> None:
    """Start then Finish per execution; FIFO-per-group delivers the Start first, so each
    execution is RUNNING (in Working) by the time its Finish is processed. The executions
    are split across `producers` concurrent coroutines, each enqueueing its share in order
    — one producer is a single sequential client; more model many clients at once."""

    async def produce(ids: list[str]) -> None:
        for eid in ids:
            await runner.start(eid)
        for eid in ids:
            await runner.send(eid, Event(kind="Finish"))

    await asyncio.gather(*(produce(exe_ids[k::producers]) for k in range(producers)))


# ---------------------------------------------------------------------------
# Store / transport construction (reuses worker.py logic via env vars)
# ---------------------------------------------------------------------------


async def _build_store(pg_pool_size: int, redis_pool_size: int) -> Any:
    backend = os.environ.get("HAREL_STORE_BACKEND", "redis")
    if backend == "postgres":
        from harel.engine.aio_store import AsyncPostgresStore

        return await AsyncPostgresStore.from_dsn(os.environ["HAREL_POSTGRES_DSN"], pool_size=pg_pool_size)
    if backend == "redis":
        import redis.asyncio as aioredis

        from harel.engine.aio_store import AsyncRedisStore

        url = os.environ.get("HAREL_STORE_REDIS_URL") or os.environ["HAREL_REDIS_URL"]
        return AsyncRedisStore(aioredis.Redis.from_url(url, max_connections=redis_pool_size))
    from harel.worker import build_store_async

    return await build_store_async()


async def _build_transport(pg_pool_size: int, redis_pool_size: int) -> Any:
    backend = os.environ.get("HAREL_TRANSPORT_BACKEND", os.environ.get("HAREL_STORE_BACKEND", "redis"))
    if backend == "postgres":
        from harel.engine.aio_transport import AsyncPostgresTransport

        return await AsyncPostgresTransport.from_dsn(os.environ["HAREL_POSTGRES_DSN"], pool_size=pg_pool_size)
    if backend == "redis":
        import redis.asyncio as aioredis

        from harel.engine.aio_transport import AsyncRedisTransport

        url = os.environ["HAREL_REDIS_URL"]
        return AsyncRedisTransport(aioredis.Redis.from_url(url, max_connections=redis_pool_size))
    from harel.worker import build_transport_async

    return await build_transport_async()


# ---------------------------------------------------------------------------
# Minimal-overhead drain measurement
# ---------------------------------------------------------------------------


class _AckCounter:
    """Wraps a transport, counting `ack`s; fires `done` when `target` acks are seen.
    This is the whole measurement instrument — one increment per processed event, no
    polling probe and no sleeps in the measured path. Everything else is delegated."""

    def __init__(self, inner: Any, target: int) -> None:
        self._inner = inner
        self._target = target
        self.count = 0
        self.done = asyncio.Event()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def ack(self, lease: Any) -> None:
        await self._inner.ack(lease)
        self.count += 1
        if self.count >= self._target:
            self.done.set()


async def _run_once(
    defn: Any,
    store: Any,
    transport: Any,
    n: int,
    concurrency: int,
    e2e: bool = False,
    producers: int = 1,
) -> tuple[float, float]:
    """Throughput for n machines × 2 events (Start, Finish). `create` is always setup
    (not measured, and publishes nothing). By default we then pre-load the backlog (also
    setup) and time only the worker draining it. With `e2e=True` the start + send are
    *inside* the timed window too (enqueue + process), which is how a durable-execution
    engine like DBOS is measured (send + process) — use it for an apples-to-apples
    cross-engine comparison; `producers` sets how many coroutines enqueue at once (with
    one, the rate is bounded by that single client's round-trips, not by the worker). End
    detected by counting acks (no probe). Returns (elapsed_seconds, events_per_second)."""
    counting = _AckCounter(transport, target=n * EVENTS_PER_EXECUTION)
    runner = AsyncDistributedRunner(store, counting, {defn.id: defn})

    exe_ids = await _create_all(runner, defn, n)  # setup (never measured)
    if not e2e:
        await _enqueue_all(runner, exe_ids, producers)  # pre-load the backlog as setup (drain-only)

    stop = asyncio.Event()
    worker = AsyncWorker(store, counting, {defn.id: defn}, concurrency=concurrency)
    t0 = time.perf_counter()
    drain_task = asyncio.create_task(worker.run(stop))
    if e2e:
        await _enqueue_all(runner, exe_ids, producers)  # enqueue is timed too (end-to-end)
    await counting.done.wait()
    elapsed = time.perf_counter() - t0

    stop.set()
    await drain_task
    return elapsed, (n * EVENTS_PER_EXECUTION) / elapsed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_HEADER = "{:>12}  {:>12}  {:>12}  {:>12}".format("concurrency", "events/s", "elapsed_s", "events")
_ROW = "{:>12}  {:>12.0f}  {:>12.2f}  {:>12}".format


async def _main(args: argparse.Namespace) -> None:
    defn = definition_from_dsl(_DSL, "Bench", validate=True)

    backend_store = os.environ.get("HAREL_STORE_BACKEND", "redis")
    backend_transport = os.environ.get("HAREL_TRANSPORT_BACKEND", backend_store)
    mode = (
        f"end-to-end (enqueue+process timed, {args.producers} producers)"
        if args.e2e
        else "drain-only (enqueue is setup)"
    )
    print(
        f"store={backend_store}  transport={backend_transport}  n={args.n_executions}  "
        f"sleep={not args.no_sleep}  mode={mode}"
    )
    print(_HEADER)
    print("-" * len(_HEADER))

    levels = [int(c) for c in args.concurrency.split(",")]

    for level in levels:
        # the producers share the runner's store/transport pools with the worker
        pg_pool_size = args.pool_size if args.pool_size else (level + args.producers) * 2 + 4
        # Redis pipelines need one connection each; size for both worker concurrency and
        # the n_executions fan-out during setup (gather of sends).
        redis_pool_size = max(pg_pool_size, args.n_executions) + 10
        store = await _build_store(pg_pool_size, redis_pool_size)
        transport = await _build_transport(pg_pool_size, redis_pool_size)

        try:
            elapsed, eps = await _run_once(
                defn, store, transport, args.n_executions, level, args.e2e, args.producers
            )
        finally:
            await store.close()
            await transport.close()

        print(_ROW(level, eps, elapsed, args.n_executions * EVENTS_PER_EXECUTION))

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--n-executions", type=int, default=200, metavar="N")
    parser.add_argument("--concurrency", default="1,4,16,64,256", metavar="C[,C...]")
    parser.add_argument(
        "--no-sleep", action="store_true", help="disable the 10ms async sleep (pure overhead)"
    )
    parser.add_argument("--pool-size", type=int, default=0, metavar="N", help="Postgres pool size (0=auto)")
    parser.add_argument(
        "--e2e",
        action="store_true",
        help="time enqueue+process (end-to-end), not drain-only — apples-to-apples with DBOS",
    )
    parser.add_argument(
        "--producers",
        type=int,
        default=1,
        metavar="K",
        help="coroutines enqueueing at once (1 = one sequential client)",
    )
    args = parser.parse_args()
    if args.producers < 1:
        parser.error("--producers must be at least 1")

    # register bench_actions so the DSL runner resolves it
    bench_mod = _make_actions(not args.no_sleep)
    sys.modules["bench_actions"] = bench_mod  # type: ignore[assignment]

    import anyio

    anyio.run(_main, args)


if __name__ == "__main__":
    main()
