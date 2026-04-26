"""Durability: an Execution persists through an ExecutionStore and resumes.

Each test creates a run with one `DurableRunner`+`SqliteStore` (on a temp file),
drops it, then builds a *fresh* runner+store on the same file (a stand-in for a
process restart) and feeds the next event — proving the position, context and
status were checkpointed and the run continues from the store. Single process,
so a unit test; the genuinely multi-process variant belongs in test/integration.
"""

from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.runtime import Driver
from harel.engine.store import SqliteStore
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

# a state that owns its cancellation: on Cancel it cleans up via Releasing, which
# waits for a Refunded event before reaching the terminal sink.
CRITICAL = f"""
machine M {{
   initial Working
   state Working {{ on enter {_h("working")} }}
   state Releasing {{ on enter {_h("releasing")} }}
   state Cancelled {{ on enter {_h("cancelled")} }}
   from Working to Releasing on Cancel
   from Releasing to Cancelled on Refunded
}}
"""


def test_flat_execution_persists_and_resumes_across_restart(tmp_path):
    defn = definition_from_dsl(FLAT, "M")
    db = tmp_path / "stm.db"

    # process 1: create + start (A -> B automatically, then waits at B for Go)
    store1 = SqliteStore(db)
    exe = DurableRunner(store1, {defn.id: defn}).create(defn.id)
    assert exe.active_path == "B"
    assert exe.status is Status.RUNNING
    assert exe.context["trace"] == ["A.enter", "B.enter"]
    eid = exe.id
    store1.close()

    # process 2 ("restart"): a fresh store + runner on the same file resume from B
    store2 = SqliteStore(db)
    resumed = DurableRunner(store2, {defn.id: defn}).process(eid, Event(kind="Go"))
    assert resumed.active_path == "C"
    assert resumed.status is Status.DONE
    assert resumed.context["trace"] == ["A.enter", "B.enter", "C.enter"]
    store2.close()


def test_orthogonal_children_persist_and_join_after_restart(tmp_path):
    defn = definition_from_dsl(ORTHO, "M")
    db = tmp_path / "stm.db"

    # process 1: the fork is persisted with its two region Executions parked
    store1 = SqliteStore(db)
    exe = DurableRunner(store1, {defn.id: defn}).create(defn.id)
    assert exe.active_path == "Fork"
    assert exe.status is Status.RUNNING
    child_ids = list(exe.children)
    assert len(child_ids) == 2
    eid = exe.id
    store1.close()

    # process 2 ("restart"): Go advances both regions to their sinks; the join
    # fires and the parent transitions Fork -> Done
    store2 = SqliteStore(db)
    runner = DurableRunner(store2, {defn.id: defn})
    resumed = runner.process(eid, Event(kind="Go"))
    assert resumed.active_path == "Done"
    assert resumed.status is Status.DONE
    assert resumed.context["trace"] == ["Done"]

    # the region Executions were persisted and finished, each with its own context
    children = [store2.load(cid) for cid in child_ids]
    assert all(c is not None and c.status is Status.DONE for c in children)
    assert sorted(c.context["trace"] for c in children) == [["A1", "A2"], ["B1", "B2"]]
    store2.close()


class _NoFlushDriver(Driver):
    """A Driver that never runs the relay — stands in for a crash AFTER the
    Executions committed but BEFORE their emitted events were delivered."""

    def _flush(self) -> None:
        pass


def test_finished_emit_survives_a_crash_before_the_relay(tmp_path):
    # The dual-write hazard: a region commits its transition (and a `Finished` to
    # the outbox) but the process dies before the parent's join is notified. With
    # a durable outbox the emit is not lost: a restart's relay delivers it and the
    # join completes.
    defn = definition_from_dsl(ORTHO, "M")
    db = tmp_path / "stm.db"

    # process 1: create + start the fork (two regions parked, parent at Fork)
    store1 = SqliteStore(db)
    parent = Execution(definition_id=defn.id)
    Driver(defn, store1).start(parent)
    parent = store1.load(parent.id)
    assert parent.active_path == "Fork"
    store1.close()

    # "crash": Go drives both regions to their sinks (each commits a Finished to
    # the durable outbox) but the relay never runs, so the parent never joins
    store_crash = SqliteStore(db)
    _NoFlushDriver(defn, store_crash).inject(parent, Event(kind="Go"))
    assert store_crash.load(parent.id).active_path == "Fork"  # still parked, not joined
    assert len(store_crash.pending_outbox()) == 2  # two Finished waiting in the outbox
    store_crash.close()

    # restart: a fresh store + driver runs the relay, which delivers the two
    # Finished from the outbox -> the join fires -> parent transitions Fork -> Done
    store2 = SqliteStore(db)
    DurableRunner(store2, {defn.id: defn}).recover(defn.id)
    final = store2.load(parent.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["Done"]
    assert store2.pending_outbox() == []  # outbox fully drained
    store2.close()


def test_duplicate_event_is_processed_once(tmp_path):
    # At-least-once delivery: the same event (same id) delivered twice must take
    # effect once. The trace must not gain a second B.enter.
    defn = definition_from_dsl(FLAT, "M")
    db = tmp_path / "stm.db"
    store = SqliteStore(db)
    runner = DurableRunner(store, {defn.id: defn})

    exe = runner.create(defn.id)  # A -> B, parked at B
    go = Event(kind="Go")  # one event, one id

    first = runner.process(exe.id, go)
    assert first.active_path == "C"
    assert first.context["trace"] == ["A.enter", "B.enter", "C.enter"]

    # re-deliver the very same event: dedupe drops it, state and trace unchanged
    again = runner.process(exe.id, go)
    assert again.active_path == "C"
    assert again.context["trace"] == ["A.enter", "B.enter", "C.enter"]
    assert again.version == first.version  # no second commit happened
    store.close()


def test_durable_forceful_cancel_and_suspend_resume(tmp_path):
    defn = definition_from_dsl(FLAT, "M")
    store = SqliteStore(tmp_path / "stm.db")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at B (no Cancel transition)

    runner.suspend(exe.id)
    assert store.load(exe.id).status is Status.SUSPENDED
    # while suspended, a domain event is ignored (state preserved)
    assert runner.process(exe.id, Event(kind="Go")).active_path == "B"

    runner.resume(exe.id)
    assert runner.process(exe.id, Event(kind="Go")).active_path == "C"

    # cancel of a state with no Cancel handler is a forceful terminate
    runner.create(defn.id)
    other = runner.create(defn.id)
    assert runner.cancel(other.id).status is Status.CANCELLED
    store.close()


def test_durable_cooperative_cancel_runs_cleanup_inline(tmp_path):
    defn = definition_from_dsl(CRITICAL, "M")
    store = SqliteStore(tmp_path / "stm.db")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at Working
    assert exe.active_path == "Working"

    # cooperative cancel: the injected Cancel is delivered inline and runs the
    # machine's own cleanup transition (Working -> Releasing), then awaits Refunded
    after = runner.cancel(exe.id)
    assert after.active_path == "Releasing"
    assert after.status is Status.RUNNING

    final = runner.process(exe.id, Event(kind="Refunded"))
    assert final.active_path == "Cancelled"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["working", "releasing", "cancelled"]
    store.close()


# region B finishes on start (B1 is a sink) — exercises the lost-Finished hazard.
ORTHO_INSTANT = f"""
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
      }}
   }}
   state Done {{ on enter {_h("Done")} }}
   from Fork to Done
}}
"""


def test_fork_children_are_created_via_the_relay_and_survive_a_crash(tmp_path):
    # crash AFTER the parent committed its fork (advance + join expectations + spawn
    # outbox) but BEFORE the relay created the children. Restart's recover() drains
    # the spawn outbox -> children created idempotently, no CAS conflict, join works.
    defn = definition_from_dsl(ORTHO, "M")
    db = tmp_path / "stm.db"

    store1 = SqliteStore(db)
    parent = Execution(definition_id=defn.id)
    _NoFlushDriver(defn, store1).start(parent)  # commits the fork, relay never runs
    pid = parent.id
    p = store1.load(pid)
    assert p.active_path == "Fork"
    assert len(p.children) == 2  # join expectations are durable
    assert len(store1.pending_spawns()) == 2  # children queued, not yet created
    assert all(store1.load(cid) is None for cid in p.children)  # no child executions yet
    store1.close()

    store2 = SqliteStore(db)
    runner = DurableRunner(store2, {defn.id: defn})
    runner.recover(defn.id)  # drains the spawn outbox -> creates the children
    assert store2.pending_spawns() == []
    assert all(store2.load(cid) is not None for cid in p.children)

    # a second recover is idempotent (children already exist -> no re-create, no conflict)
    runner.recover(defn.id)

    runner.process(pid, Event(kind="Go"))  # drive both regions to finish -> join -> Done
    final = store2.load(pid)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    store2.close()


def test_an_immediately_finishing_region_is_not_lost_on_a_fork_crash(tmp_path):
    # the subtle hazard: a region that finishes on start emits Finished. Because the
    # parent's join expectations are committed (process 1) BEFORE any child runs, the
    # Finished is recognised on recovery — not consumed against an empty children dict.
    defn = definition_from_dsl(ORTHO_INSTANT, "M")
    db = tmp_path / "stm.db"

    store1 = SqliteStore(db)
    parent = Execution(definition_id=defn.id)
    _NoFlushDriver(defn, store1).start(parent)  # fork committed; children not created
    pid = parent.id
    assert len(store1.pending_spawns()) == 2
    store1.close()

    # restart: recover() creates the children; region B finishes on start and its
    # Finished is delivered to the parent (which already knows B) -> B marked finished
    store2 = SqliteStore(db)
    runner = DurableRunner(store2, {defn.id: defn})
    runner.recover(defn.id)

    # region A still waits for Go; sending it completes the join -> Done (B not lost)
    runner.process(pid, Event(kind="Go"))
    final = store2.load(pid)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    store2.close()
