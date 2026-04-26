"""First real integration test: many workers, real threads, one sqlite backend.

Unlike the unit wiring tests (one worker draining deterministically), here N
worker threads race for work over a *shared* durable store + transport (each
worker its own connections to the same files). This exercises the genuine
concurrency: per-group exclusivity keeps each Execution single-writer, while
different Executions advance in parallel, and an orthogonal machine fans out to
regions that finish on whatever worker picks them up before the parent joins.

Single process still (threads, not processes) — the stepping stone to the
multi-process variant, which is the same code with workers in separate processes.
"""

import threading
import time

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner, Worker
from harel.engine.execution import Status
from harel.engine.store import SqliteStore
from harel.engine.transport import SqliteTransport
from harel.spec.states import Event


def _h(label: str) -> str:
    """A hook referencing `rec` with its label (DSL helper kept terse)."""
    return f'stm_actions.rec(at: "{label}")'


FLAT = f"""
machine M {{
   initial A
   state A {{ on enter {_h("A.enter")} }}
   state B {{ on enter {_h("B.enter")} }}
   state C {{ on enter {_h("C.enter")} }}
   from A to B
   from B to C on Go
}}
"""

ORTHO = f"""
machine M {{
   initial Fork
   orthogonal Fork {{
      state A {{
         initial A1
         state A1 {{ on enter {_h("A1")} }}
         state A2 {{ on enter {_h("A2")} }}
         from A1 to A2 on Go
      }}
      state B {{
         initial B1
         state B1 {{ on enter {_h("B1")} }}
         state B2 {{ on enter {_h("B2")} }}
         from B1 to B2 on Go
      }}
   }}
   state Done {{ on enter {_h("Done")} }}
   from Fork to Done
}}
"""

# a state with a (short) timeout: if no Success arrives, the worker sweep delivers
# a Timeout and the model's own transition fires (-> Failed).
RETRY = f"""
machine M {{
   initial Trying
   state Trying {{ on enter {_h("trying")}  timeout 1 }}
   state Ok {{ on enter {_h("ok")} }}
   state Failed {{ on enter {_h("failed")} }}
   from Trying to Ok on Success
   from Trying to Failed on Timeout
}}
"""

# a state that owns its cancellation: on Cancel it cleans up via Releasing, which
# waits for a Refunded event before reaching the terminal sink.
CRITICAL = f"""
machine M {{
   initial Working
   state Working {{ on enter {_h("working")} }}
   state Releasing {{ on enter {_h("releasing")} }}
   state Cancelled {{ on enter {_h("cancelled")} }}
   state Done {{ on enter {_h("done")} }}
   from Working to Done on Finish
   from Working to Releasing on Cancel
   from Releasing to Cancelled on Refunded
}}
"""


def _run_workers(n_workers, db_store, db_queue, definitions, suspend_recheck=5.0):
    """Start `n_workers` threads, each with its own connections to the shared
    files. Returns (stop_event, threads); the caller sets stop and joins."""
    stop = threading.Event()

    def run(wid):
        store = SqliteStore(db_store)
        transport = SqliteTransport(db_queue)
        try:
            Worker(
                store, transport, definitions, worker_id=wid, visibility=30.0, suspend_recheck=suspend_recheck
            ).run(stop, idle_sleep=0.002)
        finally:
            store.close()
            transport.close()

    threads = [threading.Thread(target=run, args=(f"w{i}",), daemon=True) for i in range(n_workers)]
    for t in threads:
        t.start()
    return stop, threads


def _await(predicate, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_many_flat_machines_advance_concurrently(tmp_path):
    db_store, db_queue = tmp_path / "stm.db", tmp_path / "q.db"
    defn = definition_from_dsl(FLAT, "M")
    defs = {defn.id: defn}
    store = SqliteStore(db_store)
    transport = SqliteTransport(db_queue)
    runner = DistributedRunner(store, transport, defs)

    ids = [runner.create(defn.id).id for _ in range(25)]
    for eid in ids:
        runner.send(eid, Event(kind="Go"))

    stop, threads = _run_workers(4, db_store, db_queue, defs)
    try:
        done = _await(lambda: all((e := store.load(i)) is not None and e.status is Status.DONE for i in ids))
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert done, "not all machines reached DONE in time"

    finals = [store.load(i) for i in ids]
    assert all(e.active_path == "C" for e in finals)
    # group exclusivity => each machine's trace is exactly-once, in order
    assert all(e.context["trace"] == ["A.enter", "B.enter", "C.enter"] for e in finals)
    store.close()
    transport.close()


def test_orthogonal_machines_fan_out_and_join_across_workers(tmp_path):
    db_store, db_queue = tmp_path / "stm.db", tmp_path / "q.db"
    defn = definition_from_dsl(ORTHO, "M")
    defs = {defn.id: defn}
    store = SqliteStore(db_store)
    transport = SqliteTransport(db_queue)
    runner = DistributedRunner(store, transport, defs)

    parents = [runner.create(defn.id) for _ in range(6)]
    for p in parents:
        assert p.active_path == "Fork" and len(p.children) == 2
        runner.send(p.id, Event(kind="Go"))

    stop, threads = _run_workers(4, db_store, db_queue, defs)
    try:
        done = _await(
            lambda: all((e := store.load(p.id)) is not None and e.status is Status.DONE for p in parents)
        )
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert done, "not all orthogonal machines joined in time"

    for p in parents:
        final = store.load(p.id)
        assert final.active_path == "Done"
        assert final.context["trace"] == ["Done"]
        regions = [store.load(cid) for cid in p.children]
        assert all(r is not None and r.status is Status.DONE for r in regions)
        assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
    store.close()
    transport.close()


def test_cooperative_cancel_discards_backlog_and_cleans_up_across_workers(tmp_path):
    db_store, db_queue = tmp_path / "stm.db", tmp_path / "q.db"
    defn = definition_from_dsl(CRITICAL, "M")
    defs = {defn.id: defn}
    store = SqliteStore(db_store)
    transport = SqliteTransport(db_queue)
    runner = DistributedRunner(store, transport, defs)

    ids = [runner.create(defn.id).id for _ in range(6)]
    for eid in ids:
        runner.send(eid, Event(kind="Finish"))  # backlog that would drive Working -> Done
        runner.cancel(eid)  # cooperative: drains the Finish, runs the Cancel cleanup

    stop, threads = _run_workers(4, db_store, db_queue, defs)
    try:
        # each machine drains its Finish and parks at Releasing (awaiting Refunded)
        at_releasing = _await(
            lambda: all((e := store.load(i)) is not None and e.active_path == "Releasing" for i in ids)
        )
        assert at_releasing, "machines did not reach the Cancel cleanup state"

        for eid in ids:
            runner.send(eid, Event(kind="Refunded"))  # complete the cleanup
        done = _await(lambda: all((e := store.load(i)) is not None and e.status is Status.DONE for i in ids))
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert done, "machines did not finish cleanup in time"

    for eid in ids:
        final = store.load(eid)
        assert final.active_path == "Cancelled"
        # the queued Finish was discarded (no "done"); only the cleanup path ran
        assert final.context["trace"] == ["working", "releasing", "cancelled"]
    store.close()
    transport.close()


def test_timeout_fires_via_the_worker_sweep(tmp_path):
    db_store, db_queue = tmp_path / "stm.db", tmp_path / "q.db"
    defn = definition_from_dsl(RETRY, "M")
    defs = {defn.id: defn}
    store = SqliteStore(db_store)
    transport = SqliteTransport(db_queue)
    runner = DistributedRunner(store, transport, defs)

    eid = runner.create(defn.id).id  # parked at Trying with a 1s timer; no Success sent

    stop, threads = _run_workers(3, db_store, db_queue, defs)
    try:
        # no event is published: the only way out is the worker timer sweep
        done = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.DONE)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert done, "the timeout did not fire via the worker sweep"

    final = store.load(eid)
    assert final.active_path == "Failed"
    assert final.context["trace"] == ["trying", "failed"]


def test_suspend_then_resume_across_workers(tmp_path):
    db_store, db_queue = tmp_path / "stm.db", tmp_path / "q.db"
    defn = definition_from_dsl(FLAT, "M")
    defs = {defn.id: defn}
    store = SqliteStore(db_store)
    transport = SqliteTransport(db_queue)
    runner = DistributedRunner(store, transport, defs)

    eid = runner.create(defn.id).id  # parked at B
    runner.suspend(eid)
    runner.send(eid, Event(kind="Go"))  # parked while suspended, not processed

    stop, threads = _run_workers(3, db_store, db_queue, defs, suspend_recheck=0.05)
    try:
        # give the workers a chance to (not) process it: it must stay at B
        time.sleep(0.3)
        assert store.load(eid).status is Status.SUSPENDED
        assert store.load(eid).active_path == "B"

        runner.resume(eid)  # the parked Go becomes claimable within suspend_recheck
        done = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.DONE)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
    assert done, "suspended machine did not resume and finish"

    final = store.load(eid)
    assert final.active_path == "C"
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]
    store.close()
    transport.close()
