"""Worker-scaling benchmark — does aggregate throughput grow with the number of
**worker processes**, or does the backend cap it?

The single-worker bench (`bench_async.py`) sweeps `concurrency` on ONE event loop.
This one launches W independent worker *processes* (separate connections, true CPU
parallelism — what production scale-out looks like) all draining ONE shared backend,
and reports the aggregate events/s. If 2 workers ≈ 2× 1 worker, a single worker (one
loop) was the limit; if it plateaus, the backend is.

Configured from the same env vars as worker.py / bench_async.py.

Usage:
    HAREL_STORE_BACKEND=redis HAREL_TRANSPORT_BACKEND=redis \\
    HAREL_REDIS_URL=redis://localhost:6379/0 \\
        python bench/bench_workers.py --n-executions 3000 --workers 1,2,4 --concurrency 64

Method: the parent pre-loads the whole backlog (create + start + send Finish for every
execution — NOT measured). Then it spawns W worker processes that sync on a barrier and
drain the shared queue, each counting its own acks with wall-clock timestamps. Aggregate
throughput = total_events / (last_ack_across_workers − first_ack_across_workers): the
realized rate during the active drain window, excluding startup and the idle tail. No
polling probe — drain completion is detected per worker by watching its own ack counter
go quiet (`--grace` seconds with no progress).

With `--producers P` the enqueue is timed too (end-to-end): the parent only creates the
executions, and P producer *processes* (separate from the workers, like the clients of a
real deployment) start them and send their Finish while the workers drain. The window
then runs from the barrier release to the last ack.
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # import sibling bench_async

from bench_async import (  # noqa: E402
    _DSL,
    EVENTS_PER_EXECUTION,
    _build_store,
    _build_transport,
    _create_all,
    _enqueue_all,
    _make_actions,
)

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker  # noqa: E402


class _TimedAckCounter:
    """Transport wrapper: counts acks and stamps first/last ack with wall-clock time
    (`time.time()`, comparable across processes on one host). One increment per event,
    mirrored into `total` (shared by every worker process) when given; everything else is
    delegated."""

    def __init__(self, inner: Any, total: Any = None) -> None:
        self._inner = inner
        self._total = total
        self.count = 0
        self.first = 0.0
        self.last = 0.0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def ack(self, lease: Any) -> None:
        await self._inner.ack(lease)
        now = time.time()
        if self.count == 0:
            self.first = now
        self.last = now
        self.count += 1
        if self._total is not None:
            with self._total.get_lock():
                self._total.value += 1


def _redis_pool(concurrency: int) -> int:
    return concurrency * 2 + 16


_STORE_TABLES = tuple(f"harel_{t}" for t in ("executions", "outbox", "processed_events", "timers", "spawns"))


async def _flush(store: Any, transport: Any) -> None:
    """Empty the backend so each level starts clean — without this, executions and drained
    group rows accumulate across levels/runs and pollute the measurement. Covers every backend
    we worker-bench (redis, postgres, rqlite, mongo, sqlite)."""
    sb = os.environ.get("HAREL_STORE_BACKEND", "redis")
    tb = os.environ.get("HAREL_TRANSPORT_BACKEND", sb)
    if sb == "redis":
        await store._r.flushdb()
    elif sb == "postgres":
        async with store._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE {', '.join(_STORE_TABLES)}")
            await conn.commit()
    elif sb == "rqlite":
        await store._execute([[f"DELETE FROM {t}"] for t in _STORE_TABLES])
    elif sb == "mongo":
        await store._db.client.drop_database(store._db.name)  # one DB holds store + transport
    elif sb == "sqlite":
        for t in (*_STORE_TABLES, "harel_trace"):
            await store._conn.execute(f"DELETE FROM {t}")
        await store._conn.commit()

    if tb == "postgres":
        async with transport._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("TRUNCATE harel_transport_messages, harel_transport_groups")
            await conn.commit()
    elif tb == "redis" and transport is not store:
        await transport._r.flushdb()
    elif tb == "rqlite":
        await transport._execute([["DELETE FROM harel_transport_messages"]])
    elif tb == "sqlite":
        for t in ("harel_transport_messages", "harel_transport_groups"):
            await transport._conn.execute(f"DELETE FROM {t}")
        await transport._conn.commit()
    # mongo transport shares the dropped database (handled above)


async def _setup(n: int, concurrency: int, preload: bool = True) -> list[str]:
    """Create n executions and (with `preload`) pre-load the full backlog (Start, then
    Finish per group). Runs in the parent before any worker starts; not part of the
    measured window. Returns the execution ids."""
    defn = definition_from_dsl(_DSL, "Bench", validate=True)
    store = await _build_store(concurrency * 2 + 4, _redis_pool(concurrency))
    transport = await _build_transport(concurrency * 2 + 4, _redis_pool(concurrency))
    try:
        await _flush(store, transport)  # clean slate: no leftover execs/groups from a prior level
        runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
        ids = await _create_all(runner, defn, n)
        if preload:
            await _enqueue_all(runner, ids)
        return ids
    finally:
        await store.close()
        await transport.close()


def _producer_proc(env: dict[str, str], ids: list[str], barrier: Any, out: Any) -> None:
    """One producer process (a client): sync on the barrier, then start its executions and
    send their Finish, sequentially. Returns the wall-clock time it was released."""
    os.environ.update(env)
    import anyio

    async def main() -> None:
        defn = definition_from_dsl(_DSL, "Bench")
        store = await _build_store(4, 8)
        transport = await _build_transport(4, 8)
        runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
        barrier.wait()
        released = time.time()
        await _enqueue_all(runner, ids)
        await store.close()
        await transport.close()
        out.put(released)

    anyio.run(main)


def _worker_proc(
    env: dict[str, str],
    concurrency: int,
    grace: float,
    barrier: Any,
    out: Any,
    total: Any = None,
    target: int = 0,
) -> None:
    """One worker process: build its own store+transport, sync on the barrier, drain the
    shared queue, return (count, first, last). It stops once the acks of every worker
    (`total`, shared) reach `target`; failing that (no `total` given), once its own ack
    counter goes quiet for `grace`s."""
    os.environ.update(env)
    sys.modules["bench_actions"] = _make_actions(False)  # no-op action: backend-bound
    import anyio

    async def main() -> None:
        defn = definition_from_dsl(_DSL, "Bench")
        store = await _build_store(concurrency * 2 + 4, _redis_pool(concurrency))
        transport = await _build_transport(concurrency * 2 + 4, _redis_pool(concurrency))
        counter = _TimedAckCounter(transport, total)
        worker = AsyncWorker(store, counter, {defn.id: defn}, concurrency=concurrency)
        stop = asyncio.Event()

        async def monitor() -> None:
            seen, idle = -1, 0.0
            while not stop.is_set():
                await asyncio.sleep(0.05)
                if total is not None and total.value >= target:
                    stop.set()  # every event is processed, whichever worker took it
                    return
                if counter.count == seen and counter.count > 0:
                    idle += 0.05
                    if idle >= grace:
                        stop.set()
                        return
                else:
                    seen, idle = counter.count, 0.0

        barrier.wait()  # all workers start the drain together
        await asyncio.gather(worker.run(stop), monitor())
        await store.close()
        await transport.close()
        out.put((counter.count, counter.first, counter.last))

    anyio.run(main)


_BACKEND_ENV_KEYS = (
    "HAREL_STORE_BACKEND",
    "HAREL_TRANSPORT_BACKEND",
    "HAREL_REDIS_URL",
    "HAREL_STORE_REDIS_URL",
    "HAREL_POSTGRES_DSN",
    "HAREL_RQLITE_URL",
    "HAREL_MONGO_URL",
    "HAREL_MONGO_DB",
    "HAREL_STORE_DB",
    "HAREL_TRANSPORT_DB",
)


def _run_level(
    workers: int, n: int, concurrency: int, grace: float, producers: int = 0
) -> tuple[float, list[int]]:
    """Spawn `workers` processes (plus `producers` producer processes when > 0, else the
    backlog is pre-loaded), drain, return (agg_eps, per-worker counts)."""
    import anyio

    ids = anyio.run(_setup, n, concurrency, producers == 0)

    env = {k: os.environ[k] for k in _BACKEND_ENV_KEYS if k in os.environ}
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(workers + producers)
    out: Any = ctx.Queue()
    pout: Any = ctx.Queue()
    total = ctx.Value("i", 0)
    target = n * EVENTS_PER_EXECUTION
    procs = [
        ctx.Process(target=_worker_proc, args=(env, concurrency, grace, barrier, out, total, target))
        for _ in range(workers)
    ]
    procs += [
        ctx.Process(target=_producer_proc, args=(env, ids[k::producers], barrier, pout))
        for k in range(producers)
    ]
    for p in procs:
        p.start()
    results = [out.get() for _ in range(workers)]
    releases = [pout.get() for _ in range(producers)]
    for p in procs:
        p.join()

    counts = [c for c, _, _ in results]
    firsts = [f for _, f, _ in results if f > 0]
    lasts = [last for _, _, last in results if last > 0]
    start = min(releases) if releases else (min(firsts) if firsts else 0.0)
    window = (max(lasts) - start) if lasts and start else 0.0
    total = sum(counts)
    if total != n * EVENTS_PER_EXECUTION:
        print(f"  ! processed {total} of {n * EVENTS_PER_EXECUTION} events (raise --grace)")
    agg = total / window if window > 0 else 0.0
    return agg, counts


_HEADER = "{:>8}  {:>12}  {:>12}  {}".format("workers", "agg events/s", "total ev", "per-worker")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--n-executions", type=int, default=3000, metavar="N", help="backlog size (execs)")
    parser.add_argument("--workers", default="1,2,4", metavar="W[,W...]")
    parser.add_argument("--concurrency", type=int, default=64, help="in-flight per worker")
    parser.add_argument("--grace", type=float, default=0.4, help="idle seconds that mark a worker drained")
    parser.add_argument(
        "--producers",
        type=int,
        default=0,
        metavar="P",
        help="time the enqueue too, from P producer processes (0 = pre-loaded backlog)",
    )
    args = parser.parse_args()

    store = os.environ.get("HAREL_STORE_BACKEND", "redis")
    transport = os.environ.get("HAREL_TRANSPORT_BACKEND", store)
    print(
        f"store={store}  transport={transport}  backlog={args.n_executions} execs "
        f"({args.n_executions * EVENTS_PER_EXECUTION} events)  concurrency/worker={args.concurrency}  "
        + (f"end-to-end, {args.producers} producer processes" if args.producers else "drain-only")
    )
    print(_HEADER)
    print("-" * len(_HEADER))
    for w in [int(x) for x in args.workers.split(",")]:
        agg, counts = _run_level(w, args.n_executions, args.concurrency, args.grace, args.producers)
        print("{:>8}  {:>12.0f}  {:>12}  {}".format(w, agg, args.n_executions * EVENTS_PER_EXECUTION, counts))
    print()


if __name__ == "__main__":
    main()
