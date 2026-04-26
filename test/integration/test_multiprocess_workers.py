"""The multi-process variant: workers in separate OS processes.

Unlike the multi-thread test (one interpreter, shared objects), here the workers
are real subprocesses that share *nothing* with the parent but the two sqlite
files (the store and the queue). Each subprocess rebuilds the Definition from
the DSL file, opens its own connections, and drives Executions off the transport. This is
the actual distributed model — the same code as the threaded variant, only the
workers live in different processes. WAL lets the processes share the files.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.store import SqliteStore
from harel.engine.transport import SqliteTransport
from harel.spec.states import Event

# The parent builds the Definition from the DSL; the subprocess workers rebuild
# it from the same DSL file on disk (via _worker_main.py). Both produce the same
# machine "M" (id = machine name), so the store and transport records match
# across processes.
FLAT_DSL = """
machine M {
   initial A
   state A { on enter stm_actions.rec(at: "A.enter") }
   state B { on enter stm_actions.rec(at: "B.enter") }
   state C { on enter stm_actions.rec(at: "C.enter") }
   from A to B
   from B to C on Go
}
"""

ORTHO_DSL = """
machine M {
   initial Fork
   orthogonal Fork {
      state A {
         initial A1
         state A1 { on enter stm_actions.rec(at: "A1") }
         state A2 { on enter stm_actions.rec(at: "A2") }
         from A1 to A2 on Go
      }
      state B {
         initial B1
         state B1 { on enter stm_actions.rec(at: "B1") }
         state B2 { on enter stm_actions.rec(at: "B2") }
         from B1 to B2 on Go
      }
   }
   state Done { on enter stm_actions.rec(at: "Done") }
   from Fork to Done
}
"""

_WORKER_MAIN = Path(__file__).parent / "_worker_main.py"
_REPO = Path(__file__).resolve().parents[2]


def _worker_env() -> dict:
    # the subprocess needs src (harel) + test (stm_actions) on the path
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(_REPO / "src"), str(_REPO / "test")])
    return env


def _spawn(n: int, store_db: Path, transport_kind: str, transport_dsn: str, stm_file: Path, machine: str):
    procs = []
    for i in range(n):
        procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    str(_WORKER_MAIN),
                    str(store_db),
                    transport_kind,
                    transport_dsn,
                    str(stm_file),
                    machine,
                    f"w{i}",
                ],
                env=_worker_env(),
                stderr=subprocess.PIPE,
            )
        )
    return procs


def _stop(procs):
    for p in procs:
        p.terminate()
    errs = []
    for p in procs:
        try:
            _, err = p.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
            _, err = p.communicate()
        if err:
            errs.append(err.decode(errors="replace"))
    return errs


def _await(predicate, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_flat_machines_driven_by_separate_processes(tmp_path):
    store_db, queue_db = tmp_path / "stm.db", tmp_path / "q.db"
    stm_file = tmp_path / "flat.stm"
    stm_file.write_text(FLAT_DSL)
    defn = definition_from_dsl(FLAT_DSL, "M")
    store = SqliteStore(store_db)
    transport = SqliteTransport(queue_db)
    runner = DistributedRunner(store, transport, {defn.id: defn})

    ids = [runner.create(defn.id).id for _ in range(8)]
    for eid in ids:
        runner.send(eid, Event(kind="Go"))

    procs = _spawn(3, store_db, "sqlite", str(queue_db), stm_file, "M")
    try:
        done = _await(lambda: all((e := store.load(i)) is not None and e.status is Status.DONE for i in ids))
    finally:
        errs = _stop(procs)
    assert done, f"machines did not finish; worker stderr:\n{''.join(errs)}"

    finals = [store.load(i) for i in ids]
    assert all(e.active_path == "C" for e in finals)
    assert all(e.context["trace"] == ["A.enter", "B.enter", "C.enter"] for e in finals)
    store.close()
    transport.close()


def test_orthogonal_fan_out_and_join_across_processes(tmp_path):
    store_db, queue_db = tmp_path / "stm.db", tmp_path / "q.db"
    stm_file = tmp_path / "ortho.stm"
    stm_file.write_text(ORTHO_DSL)
    defn = definition_from_dsl(ORTHO_DSL, "M")
    store = SqliteStore(store_db)
    transport = SqliteTransport(queue_db)
    runner = DistributedRunner(store, transport, {defn.id: defn})

    parents = [runner.create(defn.id) for _ in range(4)]
    for p in parents:
        assert p.active_path == "Fork" and len(p.children) == 2
        runner.send(p.id, Event(kind="Go"))

    procs = _spawn(3, store_db, "sqlite", str(queue_db), stm_file, "M")
    try:
        done = _await(
            lambda: all((e := store.load(p.id)) is not None and e.status is Status.DONE for p in parents)
        )
    finally:
        errs = _stop(procs)
    assert done, f"machines did not join; worker stderr:\n{''.join(errs)}"

    for p in parents:
        final = store.load(p.id)
        assert final.active_path == "Done"
        assert final.context["trace"] == ["Done"]
        regions = [store.load(cid) for cid in p.children]
        assert all(r is not None and r.status is Status.DONE for r in regions)
        assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
    store.close()
    transport.close()


# Real-Redis multi-process runs against the docker-compose stack instead of
# spawning workers here — see test_compose_stack.py.
