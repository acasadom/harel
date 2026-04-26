"""Control-plane lifecycle: cancel (forceful + cooperative), terminate, suspend,
resume — over both the distributed (transport) and durable (synchronous) runners.

The key behaviours:
- A `cancel` of a state with no `Cancel` transition is a forceful terminate.
- A `cancel` of a state that models `on: Cancel` goes through CANCELLING: the
  worker drains the queued backlog as no-ops until the injected Cancel reaches the
  machine, which then runs its own cleanup transition (which may itself wait for a
  later event). This gives the queue-jump semantics without touching the transport.
- `suspend` parks the backlog (FIFO preserved, no worker spin); `resume` continues.
- All commands propagate to an orthogonal parent's regions.
"""

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore, SqliteStore, StoreConflict
from harel.engine.transport import InMemoryTransport, SqliteTransport
from harel.spec.states import Event

# A flat machine with NO Cancel transition -> cancel is a forceful terminate.
FLAT = """
machine M {
   initial A
   state A { on enter stm_actions.rec(at: "A.enter") }
   state B { on enter stm_actions.rec(at: "B.enter") }
   state C { on enter stm_actions.rec(at: "C.enter") }
   from A to B
   from B to C on Go
}
"""

# A machine whose Working state OWNS its cancellation: on Cancel it cleans up via
# Releasing, which waits for a Refunded event before reaching the terminal sink.
CRITICAL = """
machine M {
   initial Working
   state Working { on enter stm_actions.rec(at: "working") }
   state Releasing { on enter stm_actions.rec(at: "releasing") }
   state Cancelled { on enter stm_actions.rec(at: "cancelled") }
   state Done { on enter stm_actions.rec(at: "done") }
   from Working to Done on Finish
   from Working to Releasing on Cancel
   from Releasing to Cancelled on Refunded
}
"""

ORTHO = """
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


@pytest.fixture(params=["memory", "sqlite"])
def backend(request, tmp_path):
    if request.param == "memory":
        yield DictStore(), InMemoryTransport()
    else:
        store = SqliteStore(tmp_path / "stm.db")
        transport = SqliteTransport(tmp_path / "q.db")
        yield store, transport
        store.close()
        transport.close()


def _drain(worker):
    while worker.step():
        pass


# --- forceful cancel / terminate ------------------------------------------------
def test_cancel_without_handler_is_a_forceful_terminate(backend):
    store, transport = backend
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at B

    runner.cancel(exe.id)

    assert store.load(exe.id).status is Status.CANCELLED


def test_terminate_drains_a_queued_backlog_as_noops(backend):
    store, transport = backend
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at B

    runner.send(exe.id, Event(kind="Go"))  # would advance B -> C if processed
    runner.terminate(exe.id)
    _drain(runner.worker())

    final = store.load(exe.id)
    assert final.status is Status.CANCELLED
    assert final.active_path == "B"  # the queued Go was discarded, not processed


# --- cooperative cancel ---------------------------------------------------------
def test_cooperative_cancel_discards_backlog_and_runs_cleanup(backend):
    store, transport = backend
    defn = definition_from_dsl(CRITICAL, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at Working
    assert exe.active_path == "Working"

    # a domain event is already queued; it would drive Working -> Done if processed
    runner.send(exe.id, Event(kind="Finish"))
    runner.cancel(exe.id)  # cooperative: -> CANCELLING + injected Cancel
    _drain(runner.worker())

    # the queued Finish was drained (no Done); the machine took its Cancel cleanup
    after_cancel = store.load(exe.id)
    assert after_cancel.active_path == "Releasing"
    assert after_cancel.status is Status.RUNNING  # cleanup is in progress, awaiting Refunded
    assert "done" not in after_cancel.context["trace"]

    # cleanup waits for an event: Refunded completes it and reaches the sink
    runner.send(exe.id, Event(kind="Refunded"))
    _drain(runner.worker())

    final = store.load(exe.id)
    assert final.active_path == "Cancelled"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["working", "releasing", "cancelled"]


# --- suspend / resume -----------------------------------------------------------
def test_suspend_preserves_the_backlog_and_resume_continues():
    clock = [1000.0]
    store = DictStore()
    transport = InMemoryTransport(clock=lambda: clock[0])
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    worker = runner.worker(visibility=30.0, suspend_recheck=5.0)
    exe = runner.create(defn.id)  # parked at B

    runner.suspend(exe.id)
    runner.send(exe.id, Event(kind="Go"))
    _drain(worker)  # the Go is parked, not processed; no spin (claim returns None)

    paused = store.load(exe.id)
    assert paused.status is Status.SUSPENDED
    assert paused.active_path == "B"  # untouched

    runner.resume(exe.id)
    clock[0] += 6.0  # past the suspend-recheck park window
    _drain(worker)

    final = store.load(exe.id)
    assert final.status is Status.DONE
    assert final.active_path == "C"
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


# --- orthogonal propagation -----------------------------------------------------
def test_suspend_and_terminate_propagate_to_regions(backend):
    store, transport = backend
    defn = definition_from_dsl(ORTHO, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)  # Fork: two region children parked
    child_ids = list(exe.children)
    assert len(child_ids) == 2

    runner.suspend(exe.id)
    assert store.load(exe.id).status is Status.SUSPENDED
    assert all(store.load(cid).status is Status.SUSPENDED for cid in child_ids)

    runner.resume(exe.id)
    assert all(store.load(cid).status is Status.RUNNING for cid in child_ids)

    runner.terminate(exe.id)
    assert store.load(exe.id).status is Status.CANCELLED
    assert all(store.load(cid).status is Status.CANCELLED for cid in child_ids)


# --- hardening: a worker survives a concurrent-writer conflict ------------------
class _ConflictOnce:
    """Wraps a store and raises StoreConflict on the next commit once armed — a
    stand-in for a control-plane command (or a raced worker) advancing the
    Execution between a worker's load and its commit."""

    def __init__(self, inner):
        self._inner = inner
        self._armed = False

    def arm(self):
        self._armed = True

    def commit(self, exe, emits, processed_event_id=None, timers=(), spawns=()):
        if self._armed:
            self._armed = False
            raise StoreConflict(exe.id, expected=exe.version, found=exe.version + 1)
        return self._inner.commit(
            exe, emits, processed_event_id=processed_event_id, timers=timers, spawns=spawns
        )

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_worker_survives_a_store_conflict_and_reprocesses(tmp_path):
    # a real (serializing) store, so a failed commit leaves the stored state intact
    # (DictStore returns the same mutated object, which would not model the rollback)
    store = _ConflictOnce(SqliteStore(tmp_path / "stm.db"))
    transport = InMemoryTransport()
    defn = definition_from_dsl(FLAT, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at B
    worker = runner.worker()

    store.arm()  # the next commit (the Go route) loses the CAS, once
    runner.send(exe.id, Event(kind="Go"))

    assert worker.step() is True  # route -> conflict -> caught -> nack (worker survives)
    assert store.load(exe.id).active_path == "B"  # not advanced; the work was stale

    assert worker.step() is True  # redelivered Go -> commit succeeds against fresh state
    assert store.load(exe.id).active_path == "C"
    assert store.load(exe.id).status is Status.DONE
    store.close()
