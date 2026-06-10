"""Worker-scaling benchmark — does aggregate throughput grow with the number of
**worker processes**, or does the backend cap it?

The single-worker bench (`bench_async.py`) sweeps `concurrency` on ONE event loop.
This one launches W independent worker *processes* (separate connections, true CPU
parallelism — what production scale-out looks like) all draining ONE shared backend,
and reports the aggregate events/s. If 2 workers ≈ 2× 1 worker, a single worker (one
loop) was the limit; if it plateaus, the backend is.

Configured from the same env vars as worker.py / bench_async.py.

Usage:
    STM_STORE_BACKEND=redis STM_TRANSPORT_BACKEND=redis \\
    STM_REDIS_URL=redis://localhost:6379/0 \\
        python bench/bench_workers.py --n-executions 3000 --workers 1,2,4 --concurrency 64

Method: the parent pre-loads the whole backlog (create + publish Start/Finish for every
execution — NOT measured). Then it spawns W worker processes that sync on a barrier and
drain the shared queue, each counting its own acks with wall-clock timestamps. Aggregate
throughput = total_events / (last_ack_across_workers − first_ack_across_workers): the
realized rate during the active drain window, excluding startup and the idle tail. No
polling probe — drain completion is detected per worker by watching its own ack counter
go quiet (`--grace` seconds with no progress).
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

from bench_async import _DSL, _build_store, _build_transport, _make_actions  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker  # noqa: E402
from harel.spec.states import Event  # noqa: E402


class _TimedAckCounter:
    """Transport wrapper: counts acks and stamps first/last ack with wall-clock time
    (`time.time()`, comparable across processes on one host). One increment per event."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.count = 0
        self.first = 0.0
        self.last = 0.0

    async def publish(self, group_id: str, event: Event) -> None:
        await self._inner.publish(group_id, event)

    async def claim(self, worker_id: str, visibility: float) -> Any:
        return await self._inner.claim(worker_id, visibility)

    async def ack(self, lease: Any) -> None:
        await self._inner.ack(lease)
        now = time.time()
        if self.count == 0:
            self.first = now
        self.last = now
        self.count += 1

    async def nack(self, lease: Any, delay: float = 0.0) -> None:
        await self._inner.nack(lease, delay)

    async def close(self) -> None:
        await self._inner.close()


def _redis_pool(concurrency: int) -> int:
    return concurrency * 2 + 16


_STORE_TABLES = ("executions", "outbox", "processed_events", "timers", "spawns")
_SURREAL_STORE_TABLES = ("executions", "outbox", "processed", "timers", "spawns", "counter")
_SURREAL_TX_TABLES = ("messages", "locks", "counter")


async def _flush(store: Any, transport: Any) -> None:
    """Empty the backend so each level starts clean — without this, executions and drained
    group rows accumulate across levels/runs and pollute the measurement. Covers every backend
    we worker-bench (redis, postgres, rqlite, mongo, surrealdb)."""
    sb = os.environ.get("STM_STORE_BACKEND", "redis")
    tb = os.environ.get("STM_TRANSPORT_BACKEND", sb)
    if sb == "redis":
        await store._r.flushdb()
    elif sb == "postgres":
        async with store._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE {', '.join(_STORE_TABLES)}")
            await conn.commit()
    elif sb == "rqlite":
        for t in _STORE_TABLES:
            await store._query(f"DELETE FROM {t}", ())
    elif sb == "mongo":
        await store._db.client.drop_database(store._db.name)  # one DB holds store + transport
    elif sb == "surrealdb":
        for t in _SURREAL_STORE_TABLES:
            await store._db.query(f"DELETE {t}")

    if tb == "postgres":
        async with transport._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("TRUNCATE transport_messages, transport_groups")
            await conn.commit()
    elif tb == "redis" and transport is not store:
        await transport._r.flushdb()
    elif tb == "rqlite":
        await transport._query("DELETE FROM messages", ())
    elif tb == "surrealdb":
        for t in _SURREAL_TX_TABLES:
            await transport._db.query(f"DELETE {t}")
    # mongo transport shares the dropped database (handled above)


async def _setup(n: int, concurrency: int) -> None:
    """Create n executions and pre-load the full backlog (Start, then Finish per group).
    Runs in the parent before any worker starts; not part of the measured window."""
    defn = definition_from_dsl(_DSL, "Bench")
    store = await _build_store(concurrency * 2 + 4, _redis_pool(concurrency))
    transport = await _build_transport(concurrency * 2 + 4, _redis_pool(concurrency))
    try:
        await _flush(store, transport)  # clean slate: no leftover execs/groups from a prior level
        runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
        ids = [(await runner.create(defn.id)).id for _ in range(n)]
        for eid in ids:
            await runner.send(eid, Event(kind="Start"))
        for eid in ids:
            await runner.send(eid, Event(kind="Finish"))
    finally:
        await store.close()
        await transport.close()


def _worker_proc(
    env: dict[str, str],
    concurrency: int,
    grace: float,
    barrier: Any,
    out: Any,
) -> None:
    """One worker process: build its own store+transport, sync on the barrier, drain the
    shared queue until its ack counter goes quiet for `grace`s, return (count, first, last)."""
    os.environ.update(env)
    sys.modules["bench_actions"] = _make_actions(False)  # no-op action: backend-bound
    import anyio

    async def main() -> None:
        defn = definition_from_dsl(_DSL, "Bench")
        store = await _build_store(concurrency * 2 + 4, _redis_pool(concurrency))
        transport = await _build_transport(concurrency * 2 + 4, _redis_pool(concurrency))
        counter = _TimedAckCounter(transport)
        worker = AsyncWorker(store, counter, {defn.id: defn}, concurrency=concurrency)
        stop = asyncio.Event()

        async def monitor() -> None:
            seen, idle = -1, 0.0
            while not stop.is_set():
                await asyncio.sleep(0.05)
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
    "STM_STORE_BACKEND",
    "STM_TRANSPORT_BACKEND",
    "STM_REDIS_URL",
    "STM_STORE_REDIS_URL",
    "STM_POSTGRES_DSN",
    "STM_RQLITE_URL",
    "STM_MONGO_URL",
    "STM_MONGO_DB",
    "STM_SURREAL_URL",
    "STM_SURREAL_NS",
    "STM_SURREAL_DB",
    "STM_SURREAL_USER",
    "STM_SURREAL_PASS",
)


def _run_level(workers: int, n: int, concurrency: int, grace: float) -> tuple[float, list[int]]:
    """Pre-load the backlog, spawn `workers` processes, drain, return (agg_eps, per-worker counts)."""
    import anyio

    anyio.run(_setup, n, concurrency)

    env = {k: os.environ[k] for k in _BACKEND_ENV_KEYS if k in os.environ}
    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(workers)
    out: Any = ctx.Queue()
    procs = [
        ctx.Process(target=_worker_proc, args=(env, concurrency, grace, barrier, out)) for _ in range(workers)
    ]
    for p in procs:
        p.start()
    results = [out.get() for _ in range(workers)]
    for p in procs:
        p.join()

    counts = [c for c, _, _ in results]
    firsts = [f for _, f, _ in results if f > 0]
    lasts = [last for _, _, last in results if last > 0]
    window = (max(lasts) - min(firsts)) if firsts and lasts else 0.0
    total = sum(counts)
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
    args = parser.parse_args()

    store = os.environ.get("STM_STORE_BACKEND", "redis")
    transport = os.environ.get("STM_TRANSPORT_BACKEND", store)
    print(
        f"store={store}  transport={transport}  backlog={args.n_executions} execs "
        f"({args.n_executions * 2} events)  concurrency/worker={args.concurrency}"
    )
    print(_HEADER)
    print("-" * len(_HEADER))
    for w in [int(x) for x in args.workers.split(",")]:
        agg, counts = _run_level(w, args.n_executions, args.concurrency, args.grace)
        print("{:>8}  {:>12.0f}  {:>12}  {}".format(w, agg, args.n_executions * 2, counts))
    print()


if __name__ == "__main__":
    main()
