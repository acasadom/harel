"""DBOS comparison bench (throwaway — DBOS is NOT a harel dependency).

Models the SAME toy FSM as the harel benches (Idle --Start--> Working --Finish--> Done)
on DBOS, two ways, and reports durable events/s on the same Postgres + laptop, so the
number sits next to harel-on-Postgres. This is a *paradigm* comparison, not apples-to-
apples: harel is a declarative statechart engine; DBOS is imperative durable execution.

  - Variant A (event-driven, the paradigm match): one durable workflow per execution that
    `recv`s two events. Mirrors harel's "create, then start + send Finish per execution".
  - Variant B (durable transition throughput): one durable workflow per *event* that runs a
    transaction advancing the row's state. The floor: "how fast can it durably transition".

Written against dbos 3.x (transactions through a `SQLAlchemyDatasource`).

Run (needs a Postgres + the `dbos` package, both ad-hoc):
    docker compose -f deploy/docker-compose.yml up -d postgres
    uv pip install dbos
    STM_DBOS_DSN=postgresql://stm:stm@localhost:5432/dbosbench python bench/bench_dbos.py --n 500

`--producers K` sends from K threads at once (default 1, one sequential client) — the
counterpart of `bench_async.py --e2e --producers K`.

`--workers W` runs variant A across processes instead — the counterpart of
`bench_workers.py --producers P`: the workflows are enqueued on a queue (setup, not timed)
and run by W worker processes, then `--producers` *processes* (DBOS clients) send the
events. The window runs from their release to the last workflow's `completed_at`.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
from dbos import DBOS, DBOSClient, DBOSConfig, SetWorkflowID, SQLAlchemyDatasource
from sqlalchemy import text

DSN = os.environ.get("STM_DBOS_DSN", "postgresql://stm:stm@localhost:5432/dbosbench")
_NEXT = {"Start": "Working", "Finish": "Done"}
_PREV = {"Start": "Idle", "Finish": "Working"}
_QUEUE = "fsm"  # the queue the multi-process mode's workflows run off


def _config(executor_id: str = "local") -> DBOSConfig:
    return {
        "name": "harelcmp",
        "system_database_url": DSN,
        "log_level": "WARNING",
        "executor_id": executor_id,
    }


@DBOS.workflow()
def fsm_recv() -> str:
    """Variant A: a long-lived durable workflow that waits for two events."""
    DBOS.recv("ev", timeout_seconds=120)  # Start  -> Working
    DBOS.recv("ev", timeout_seconds=120)  # Finish -> Done
    return "Done"


def _register_variant_b(ds: SQLAlchemyDatasource):
    """Variant B's workflow, bound to the datasource its transaction runs on (which needs the
    database to exist, so it is registered from `main`, before `DBOS.launch`)."""

    @ds.transaction()
    def transition(exec_id: str, ev: str) -> None:
        ds.sql_session().execute(
            text("UPDATE fsm SET state = :n WHERE id = :i AND state = :p"),
            {"n": _NEXT[ev], "i": exec_id, "p": _PREV[ev]},
        )

    @DBOS.workflow(name="advance")
    def advance(exec_id: str, ev: str) -> None:
        """Variant B: one durable workflow per event, doing the transactional transition."""
        transition(exec_id, ev)

    return advance


def _run_id(tag: str, i: int) -> str:
    # workflow ids must be unique per run, else DBOS dedupes/recovers the prior one
    return f"{tag}-{os.getpid()}-{i}"


def _split(items: list, producers: int) -> list[list]:
    return [items[k::producers] for k in range(producers)]


def _in_parallel(fn, chunks: list[list]) -> list:
    """Run `fn(chunk)` for every chunk on its own thread; return their results."""
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        return list(pool.map(fn, chunks))


def variant_a(n: int, producers: int) -> float:
    ids = [_run_id("a", i) for i in range(n)]
    handles = []
    for wid in ids:  # setup (not timed): start the parked workflows
        with SetWorkflowID(wid):
            handles.append(DBOS.start_workflow(fsm_recv))

    def produce(chunk: list[str]) -> None:
        for wid in chunk:
            DBOS.send(wid, "Start", "ev")
        for wid in chunk:
            DBOS.send(wid, "Finish", "ev")

    t0 = time.perf_counter()
    _in_parallel(produce, _split(ids, producers))
    for h in handles:
        h.get_result()
    return (n * 2) / (time.perf_counter() - t0)


def variant_b(advance, n: int, producers: int) -> float:
    with psycopg.connect(DSN, autocommit=True) as c:  # setup (not timed): N rows in Idle
        c.execute("CREATE TABLE IF NOT EXISTS fsm (id TEXT PRIMARY KEY, state TEXT NOT NULL)")
        c.execute("TRUNCATE fsm")
        with c.cursor() as cur:
            cur.executemany(
                "INSERT INTO fsm (id, state) VALUES (%s, 'Idle')", [(f"b-{i}",) for i in range(n)]
            )
    t0 = time.perf_counter()
    for ev in ("Start", "Finish"):  # ordered phases so the guarded transition never races

        def produce(chunk: list[int], ev: str = ev) -> list:
            return [DBOS.start_workflow(advance, f"b-{i}", ev) for i in chunk]

        for handles in _in_parallel(produce, _split(list(range(n)), producers)):
            for h in handles:
                h.get_result()
    return (n * 2) / (time.perf_counter() - t0)


def _worker_proc(executor_id: str, ready: Any, stop: Any) -> None:
    """A DBOS worker process: runs the queue's workflows until `stop` is set. Each has its
    own executor id, so none of them recovers another's in-flight workflows."""
    DBOS(config=_config(executor_id))
    DBOS.launch()  # creates the system database's schema on first use
    DBOS.register_queue(_QUEUE, polling_interval_sec=0.1)
    ready.release()
    stop.wait()
    DBOS.destroy()


def _producer_proc(ids: list[str], barrier: Any, out: Any) -> None:
    """A producer process (a DBOS client): sync on the barrier, then send Start and Finish
    to its workflows, sequentially. Returns the wall-clock time it was released."""
    client = DBOSClient(system_database_url=DSN)
    barrier.wait()
    released = time.time()
    for wid in ids:
        client.send(wid, "Start", "ev")
    for wid in ids:
        client.send(wid, "Finish", "ev")
    client.destroy()
    out.put(released)


def variant_a_distributed(n: int, workers: int, producers: int) -> float:
    """Variant A across processes: W DBOS workers run the workflows, P clients send."""
    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    ready = ctx.Semaphore(0)
    procs = [
        ctx.Process(target=_worker_proc, args=(f"w{k}-{os.getpid()}", ready, stop)) for k in range(workers)
    ]
    for p in procs:
        p.start()
    for _ in procs:  # every worker launched and listening on the queue
        if not ready.acquire(timeout=120):
            raise RuntimeError("a DBOS worker did not start within 120s")
    client = DBOSClient(system_database_url=DSN)
    try:
        prefix = _run_id("d", 0)[:-1]
        ids = [f"{prefix}{i}" for i in range(n)]
        for wid in ids:  # setup (not timed): enqueue, then wait until every one is running
            client.enqueue({"workflow_name": "fsm_recv", "queue_name": _QUEUE, "workflow_id": wid})
        deadline = time.monotonic() + 120
        while client.list_workflows(
            workflow_id_prefix=prefix, status="ENQUEUED", load_input=False, load_output=False
        ):
            if time.monotonic() > deadline:
                raise RuntimeError("the workers did not dequeue every workflow within 120s")
            time.sleep(0.2)

        barrier = ctx.Barrier(producers)
        out: Any = ctx.Queue()
        senders = [
            ctx.Process(target=_producer_proc, args=(ids[k::producers], barrier, out))
            for k in range(producers)
        ]
        for p in senders:
            p.start()
        releases = [out.get() for _ in range(producers)]
        for p in senders:
            p.join()
        while True:
            done = client.list_workflows(
                workflow_id_prefix=prefix, status="SUCCESS", load_input=False, load_output=False
            )
            if len(done) == n:
                break
            if time.monotonic() > deadline + 240:
                raise RuntimeError(f"only {len(done)} of {n} workflows completed")
            time.sleep(0.2)
        end = max(w.completed_at for w in done) / 1000
        return (n * 2) / (end - min(releases))
    finally:
        client.destroy()
        stop.set()
        for p in procs:
            p.join()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=500, help="executions (x2 events each)")
    ap.add_argument("--producers", type=int, default=1, help="threads (processes with --workers) sending")
    ap.add_argument("--workers", type=int, default=0, help="run variant A on W worker processes")
    args = ap.parse_args()
    if args.producers < 1:
        ap.error("--producers must be at least 1")

    with psycopg.connect(DSN.rsplit("/", 1)[0] + "/stm", autocommit=True) as c:
        if c.execute("SELECT 1 FROM pg_database WHERE datname = 'dbosbench'").fetchone() is None:
            c.execute("CREATE DATABASE dbosbench")

    if args.workers:
        eps = variant_a_distributed(args.n, args.workers, args.producers)
        print(
            f"DBOS FSM bench — {args.n} executions x 2 events, {args.workers} worker processes, "
            f"{args.producers} producer processes"
        )
        print(f"{'A: workflow + send/recv (event-driven)':<34} {eps:>9.0f}")
        return

    DBOS(config=_config())
    advance = _register_variant_b(SQLAlchemyDatasource.create(DSN))
    DBOS.launch()
    try:
        print(
            f"DBOS FSM bench — {args.n} executions x 2 events = {args.n * 2} durable events, "
            f"{args.producers} producers"
        )
        print(f"{'variant':<34} {'events/s':>9}")
        print(f"{'A: workflow + send/recv (event-driven)':<34} {variant_a(args.n, args.producers):>9.0f}")
        print(
            f"{'B: workflow-per-event (transition)':<34} {variant_b(advance, args.n, args.producers):>9.0f}"
        )
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
