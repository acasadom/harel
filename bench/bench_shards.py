"""Horizontal-scaling benchmark — does aggregate throughput grow with the number of
independent backend **shards**?

`bench_workers.py` showed that piling workers on ONE backend plateaus (the backend, not
the worker, is the limit). The way past that ceiling is the same one Temporal and DBOS use:
**shard** — partition executions across independent backend instances. A shard here is its
own Redis instance with its own worker; because executions are independent (single-consumer
per group, no cross-execution coordination), shards share nothing, so aggregate throughput
should grow ~linearly with shard count — until the *host* runs out of cores (on a real
multi-host deployment you add machines, which is the whole point).

Each `--redis-urls` entry is one shard. A run with `--shards K` uses the first K of them,
pre-loads each with its own backlog, drains them with one worker process per shard, and
reports the aggregate events/s.

Usage (4 independent Redis instances on 4 ports):
    python bench/bench_shards.py \\
        --redis-urls redis://localhost:6379/0,redis://localhost:6380/0,\\
redis://localhost:6381/0,redis://localhost:6382/0 \\
        --shards 1,2,4 --n-executions 3000 --concurrency 64

Method mirrors bench_workers: setup (create + publish the per-shard backlog) is not
measured; only the drain is, detected per worker by its ack counter going quiet
(`--grace`). Aggregate = total_events / (last_ack − first_ack) across all shards.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # import sibling bench modules

from bench_async import _DSL  # noqa: E402
from bench_workers import _redis_pool, _worker_proc  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio.distributed import AsyncDistributedRunner  # noqa: E402
from harel.spec.states import Event  # noqa: E402


async def _setup_shard(redis_url: str, n: int, concurrency: int) -> None:
    """Flush the shard's Redis and pre-load its backlog: n executions, each with Start then
    Finish queued (FIFO delivers Start first). Runs in the parent; not measured."""
    import redis.asyncio as aioredis

    from harel.engine.aio_store import AsyncRedisStore
    from harel.engine.aio_transport import AsyncRedisTransport

    conns = max(_redis_pool(concurrency), n) + 10
    raw = aioredis.Redis.from_url(redis_url, max_connections=conns)
    await raw.flushdb()  # clean state between shard-count levels (instances are reused)
    store = AsyncRedisStore(raw)
    transport = AsyncRedisTransport(aioredis.Redis.from_url(redis_url, max_connections=conns))
    try:
        defn = definition_from_dsl(_DSL, "Bench")
        runner = AsyncDistributedRunner(store, transport, {defn.id: defn})
        ids = [(await runner.create(defn.id)).id for _ in range(n)]
        for eid in ids:
            await runner.send(eid, Event(kind="Start"))
        for eid in ids:
            await runner.send(eid, Event(kind="Finish"))
    finally:
        await store.close()
        await transport.close()


def _shard_env(redis_url: str) -> dict[str, str]:
    return {
        "STM_STORE_BACKEND": "redis",
        "STM_TRANSPORT_BACKEND": "redis",
        "STM_REDIS_URL": redis_url,
    }


def _run_level(urls: list[str], n: int, concurrency: int, grace: float) -> tuple[float, list[int]]:
    """Pre-load `len(urls)` shards, drain each with one worker process, return
    (aggregate events/s, per-shard ack counts)."""
    import anyio

    for url in urls:
        anyio.run(_setup_shard, url, n, concurrency)

    ctx = mp.get_context("spawn")
    barrier = ctx.Barrier(len(urls))
    out: Any = ctx.Queue()
    procs = [
        ctx.Process(target=_worker_proc, args=(_shard_env(url), concurrency, grace, barrier, out))
        for url in urls
    ]
    for p in procs:
        p.start()
    results = [out.get() for _ in urls]
    for p in procs:
        p.join()

    counts = [c for c, _, _ in results]
    firsts = [f for _, f, _ in results if f > 0]
    lasts = [last for _, _, last in results if last > 0]
    window = (max(lasts) - min(firsts)) if firsts and lasts else 0.0
    agg = sum(counts) / window if window > 0 else 0.0
    return agg, counts


_HEADER = "{:>8}  {:>14}  {:>14}  {}".format("shards", "agg events/s", "per-shard ev/s", "per-shard acks")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--redis-urls", required=True, metavar="URL[,URL...]", help="one Redis URL per shard")
    parser.add_argument("--shards", default="1,2,4", metavar="K[,K...]")
    parser.add_argument("--n-executions", type=int, default=3000, metavar="N", help="backlog per shard")
    parser.add_argument("--concurrency", type=int, default=64, help="in-flight per shard worker")
    parser.add_argument("--grace", type=float, default=0.4, help="idle seconds that mark a shard drained")
    args = parser.parse_args()

    all_urls = [u.strip() for u in args.redis_urls.split(",") if u.strip()]
    print(
        f"backend=redis (sharded)  backlog/shard={args.n_executions} execs  concurrency/shard={args.concurrency}"
    )
    print(_HEADER)
    print("-" * len(_HEADER))
    for k in [int(x) for x in args.shards.split(",")]:
        if k > len(all_urls):
            raise SystemExit(f"--shards {k} needs {k} URLs, got {len(all_urls)}")
        agg, counts = _run_level(all_urls[:k], args.n_executions, args.concurrency, args.grace)
        print("{:>8}  {:>14.0f}  {:>14.0f}  {}".format(k, agg, agg / k, counts))
    print()


if __name__ == "__main__":
    main()
