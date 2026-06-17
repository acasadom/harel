"""DBOS comparison bench (throwaway — DBOS is NOT a harel dependency).

Models the SAME toy FSM as the harel benches (Idle --Start--> Working --Finish--> Done)
on DBOS, two ways, and reports durable events/s on the same Postgres + laptop, so the
number sits next to harel-on-Postgres. This is a *paradigm* comparison, not apples-to-
apples: harel is a declarative statechart engine; DBOS is imperative durable execution.

  - Variant A (event-driven, the paradigm match): one durable workflow per execution that
    `recv`s two events. Mirrors harel's "create, then send Start + Finish per execution".
  - Variant B (durable transition throughput): one durable workflow per *event* that runs a
    transaction advancing the row's state. The floor: "how fast can it durably transition".

Run (needs a Postgres + the `dbos` package, both ad-hoc):
    docker compose -f deploy/docker-compose.yml up -d postgres
    uv pip install dbos
    STM_DBOS_DSN=postgresql://stm:stm@localhost:5432/dbosbench python bench/bench_dbos.py --n 500
"""

from __future__ import annotations

import argparse
import os
import time

import psycopg
from dbos import DBOS, DBOSConfig, SetWorkflowID
from sqlalchemy import text

DSN = os.environ.get("STM_DBOS_DSN", "postgresql://stm:stm@localhost:5432/dbosbench")
_NEXT = {"Start": "Working", "Finish": "Done"}
_PREV = {"Start": "Idle", "Finish": "Working"}


@DBOS.workflow()
def fsm_recv() -> str:
    """Variant A: a long-lived durable workflow that waits for two events."""
    DBOS.recv("ev", timeout_seconds=120)  # Start  -> Working
    DBOS.recv("ev", timeout_seconds=120)  # Finish -> Done
    return "Done"


@DBOS.transaction()
def _transition(exec_id: str, ev: str) -> None:
    DBOS.sql_session.execute(
        text("UPDATE fsm SET state = :n WHERE id = :i AND state = :p"),
        {"n": _NEXT[ev], "i": exec_id, "p": _PREV[ev]},
    )


@DBOS.workflow()
def advance(exec_id: str, ev: str) -> None:
    """Variant B: one durable workflow per event, doing the transactional transition."""
    _transition(exec_id, ev)


def _run_id(tag: str, i: int) -> str:
    # workflow ids must be unique per run, else DBOS dedupes/recovers the prior one
    return f"{tag}-{os.getpid()}-{i}"


def variant_a(n: int) -> float:
    ids = [_run_id("a", i) for i in range(n)]
    handles = []
    for wid in ids:  # setup (not timed): start the parked workflows
        with SetWorkflowID(wid):
            handles.append(DBOS.start_workflow(fsm_recv))
    t0 = time.perf_counter()
    for wid in ids:
        DBOS.send(wid, "Start", "ev")
    for wid in ids:
        DBOS.send(wid, "Finish", "ev")
    for h in handles:
        h.get_result()
    return (n * 2) / (time.perf_counter() - t0)


def variant_b(n: int) -> float:
    with psycopg.connect(DSN, autocommit=True) as c:  # setup (not timed): N rows in Idle
        c.execute("CREATE TABLE IF NOT EXISTS fsm (id TEXT PRIMARY KEY, state TEXT NOT NULL)")
        c.execute("TRUNCATE fsm")
        with c.cursor() as cur:
            cur.executemany(
                "INSERT INTO fsm (id, state) VALUES (%s, 'Idle')", [(f"b-{i}",) for i in range(n)]
            )
    t0 = time.perf_counter()
    for ev in ("Start", "Finish"):  # ordered phases so the guarded transition never races
        handles = [DBOS.start_workflow(advance, f"b-{i}", ev) for i in range(n)]
        for h in handles:
            h.get_result()
    return (n * 2) / (time.perf_counter() - t0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=500, help="executions (x2 events each)")
    args = ap.parse_args()

    with psycopg.connect(DSN.rsplit("/", 1)[0] + "/stm", autocommit=True) as c:
        if c.execute("SELECT 1 FROM pg_database WHERE datname = 'dbosbench'").fetchone() is None:
            c.execute("CREATE DATABASE dbosbench")

    cfg: DBOSConfig = {"name": "harelcmp", "database_url": DSN, "log_level": "WARNING"}
    DBOS(config=cfg)
    DBOS.launch()
    try:
        print(f"DBOS FSM bench — {args.n} executions x 2 events = {args.n * 2} durable events")
        print(f"{'variant':<34} {'events/s':>9}")
        print(f"{'A: workflow + send/recv (event-driven)':<34} {variant_a(args.n):>9.0f}")
        print(f"{'B: workflow-per-event (transition)':<34} {variant_b(args.n):>9.0f}")
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
