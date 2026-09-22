"""Control-plane lifecycle: cancel (forceful + cooperative), terminate, suspend,
resume, redrive — over both the distributed (transport) and durable (synchronous)
runners.

The key behaviours:
- A `cancel` of a state with no `Cancel` transition is a forceful terminate.
- A `cancel` of a state that models `on: Cancel` goes through CANCELLING: the
  worker drains the queued backlog as no-ops until the injected Cancel reaches the
  machine, which then runs its own cleanup transition (which may itself wait for a
  later event). This gives the queue-jump semantics without touching the transport.
- `suspend` parks the backlog (FIFO preserved, no worker spin); `resume` continues.
- `redrive` forces a dead-lettered (FAILED) execution back to RUNNING at a caller-
  chosen leaf; it's a plain CAS write, like `resume` — it does not drain, the next
  event/timer does. Unlike the others, it does NOT propagate to regions (a region
  is its own Execution, redriven independently) and refuses a target outside the
  Execution's own branch or one left with unfinished children.
- All other commands propagate to an orthogonal parent's regions.
"""

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
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

# B's `on enter` always raises -> dead-letters (status=FAILED) with no `on error`.
DEAD_LETTERS = """
machine M {
   initial A
   state A { on enter stm_actions.rec(at: "A.enter") }
   state B { on enter stm_actions.boom }
   state C { on enter stm_actions.rec(at: "C.enter") }
   from A to B on Go
   from B to C on Advance
}
"""

# An orthogonal fork whose OWN `on exit` always raises. Its regions never finish on
# their own (they wait on `Go`, which never arrives), so when the fork's `timeout`
# fires and its `on exit` blows up (before `_leave_regions` can cancel them — see
# `_take`), the parent dead-letters with both regions still live/unfinished.
ORPHANS_LIVE_REGIONS = """
event Go {}
machine M {
   initial Fork
   orthogonal Fork {
      timeout 1
      on exit stm_actions.boom
      state A {
         initial A1
         state A1 {}
         from A1 to A2 on Go
         state A2 { outcome success }
      }
      state B {
         initial B1
         state B1 {}
         from B1 to B2 on Go
         state B2 { outcome success }
      }
   }
   final Elsewhere success {}
   from Fork to Elsewhere on Timeout
}
"""

# Dies before ever reaching the fork -> no region Executions exist at all yet.
DIES_BEFORE_FORK = """
event Go {}
machine M {
   initial Setup
   state Setup { on enter stm_actions.boom }
   orthogonal Fork {
      state A {
         initial A1
         state A1 {}
         from A1 to A2 on Go
         state A2 { outcome success }
      }
      state B {
         initial B1
         state B1 {}
         from B1 to B2 on Go
         state B2 { outcome success }
      }
   }
   final Done success {}
   from Setup to Fork on Go
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


# --- redrive ----------------------------------------------------------------------
def test_redrive_revives_a_dead_letter_at_the_chosen_leaf():
    store = DictStore()
    defn = definition_from_dsl(DEAD_LETTERS, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))  # A -> B, B.enter raises -> dead-lettered

    dead = store.load(exe.id)
    assert dead.status is Status.FAILED and dead.active_path == "B" and dead.error

    revived = runner.redrive(exe.id, "A")  # back to the last known-good leaf
    assert revived.status is Status.RUNNING
    assert revived.active_path == "A"
    assert revived.error is None

    # the execution is genuinely alive again: it keeps processing normally
    runner.process(exe.id, Event(kind="Go"))
    still_dead_at_b = store.load(exe.id)
    assert still_dead_at_b.status is Status.FAILED  # same bug, same result — expected


def test_redrive_rejects_an_unknown_or_composite_target():
    store = DictStore()
    defn = definition_from_dsl(DEAD_LETTERS, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))  # dead-lettered at B

    with pytest.raises(ValueError):
        runner.redrive(exe.id, "NoSuchState")

    composite_defn = definition_from_dsl(
        """
        machine M {
           initial A
           state A { on enter stm_actions.rec(at: "A.enter") }
           state P { initial L  state L {}  on enter stm_actions.boom }
           from A to P on Go
        }
        """,
        "M",
    )
    runner2 = DurableRunner(store, {composite_defn.id: composite_defn})
    exe2 = runner2.create(composite_defn.id)
    runner2.process(exe2.id, Event(kind="Go"))  # dead-lettered on entering P (composite)
    assert store.load(exe2.id).status is Status.FAILED

    with pytest.raises(ValueError):
        runner2.redrive(exe2.id, "P")  # a composite is never a valid resting leaf


def test_redrive_is_a_noop_unless_the_execution_is_failed():
    store = DictStore()
    defn = definition_from_dsl(DEAD_LETTERS, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at A, RUNNING — never failed

    result = runner.redrive(exe.id, "B")  # a state whose `on enter` would raise, even
    assert result.status is Status.RUNNING  # but it's a no-op: not FAILED, so untouched
    assert result.active_path == "A"


def test_redrive_over_the_distributed_runner(backend):
    store, transport = backend
    defn = definition_from_dsl(DEAD_LETTERS, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id)

    runner.send(exe.id, Event(kind="Go"))
    _drain(runner.worker())
    assert store.load(exe.id).status is Status.FAILED

    runner.redrive(exe.id, "A")
    revived = store.load(exe.id)
    assert revived.status is Status.RUNNING and revived.active_path == "A" and revived.error is None


def test_redrive_rejects_a_target_outside_the_executions_own_branch():
    """A region spawned by an orthogonal fork is its own Execution, rooted at the
    branch's path (e.g. `root_path='Fork.A'`). A target outside that branch would
    leave `active_path` with no ancestor relationship to `root_path` — `chain()`
    asserts that relationship and raises on the very next event, so this must be
    rejected up front instead of silently corrupting the record."""
    store = DictStore()
    defn = definition_from_dsl(ORPHANS_LIVE_REGIONS, "M", validate=True)
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    region_a_id = next(cid for cid in exe.children if ".A:" in cid)

    # force region A to dead-letter on its own (independent of the fork's timeout)
    region_a = store.load(region_a_id)
    region_a.status = Status.FAILED
    region_a.error = "boom"
    store.save(region_a)

    with pytest.raises(ValueError, match="outside this execution's own branch"):
        runner.redrive(region_a_id, "Fork.B.B1")  # a leaf, but in the OTHER region


def test_redrive_refuses_a_dead_letter_with_live_children():
    """The fork's own `on exit` raises before `_leave_regions` can cancel its live
    regions (see `_take`'s ordering) — the parent dead-letters with both regions
    still unfinished. Redrive must refuse rather than orphan them."""
    store = DictStore()
    defn = definition_from_dsl(ORPHANS_LIVE_REGIONS, "M", validate=True)
    clock = [1000.0]
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)

    clock[0] += 2.0  # past the fork's 1-second timeout, no real sleep needed
    runner.fire_due_timers()

    dead = store.load(exe.id)
    assert dead.status is Status.FAILED
    assert all(not cs.finished for cs in dead.children.values())

    # a target outside the fork entirely, so only the unfinished-children check
    # (not the separate orthogonal-ancestor check) is exercised here
    with pytest.raises(ValueError, match="unfinished children"):
        runner.redrive(exe.id, "Elsewhere")


def test_redrive_rejects_a_target_inside_an_unforked_orthogonal_branch():
    """The execution dead-letters before ever reaching Fork, so `exe.children` is
    empty — the "unfinished children" check alone would vacuously pass. Redriving
    straight into `Fork.A.A1` would park `active_path` inside one branch with no
    region Execution for A *or* B ever spawned, silently breaking the fork's
    parallel-regions semantics rather than orphaning anything visible."""
    store = DictStore()
    defn = definition_from_dsl(DIES_BEFORE_FORK, "M", validate=True)
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)

    dead = store.load(exe.id)
    assert dead.status is Status.FAILED and dead.children == {}

    with pytest.raises(ValueError, match="inside orthogonal state 'Fork'"):
        runner.redrive(exe.id, "Fork.A.A1")

    # the correct fix: redrive to Setup and let the normal transition re-fork it
    revived = runner.redrive(exe.id, "Setup")
    assert revived.status is Status.RUNNING and revived.active_path == "Setup"


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

    def commit(self, exe, emits, processed_event_id=None, timers=(), spawns=(), trace=None):
        if self._armed:
            self._armed = False
            raise StoreConflict(exe.id, expected=exe.version, found=exe.version + 1)
        return self._inner.commit(
            exe, emits, processed_event_id=processed_event_id, timers=timers, spawns=spawns, trace=trace
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
