"""Reset restarts an orthogonal / fan-out machine with FRESH regions.

`Reset` bypasses the normal transition path (it clears state and re-`start`s), so
the per-entry seq bump that a normal exit does (`_leave_regions`) never fired. If a
region had already completed when Reset arrived, re-forking reused the region's
(DONE) child-execution id: the relay's idempotent create skipped it, it never
re-emitted `Finished`, and the join deadlocked. Reset now bumps the active spawn
site's entry seq (and clears `children`) so the restart spawns fresh children.
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution, Status
from harel.spec.states import Event

# regions advance independently, so one can finish while the other is still live.
ORTHO = """
event GoA {}
event GoB {}
machine M {
  initial Fork
  orthogonal Fork {
    state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on GoA }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on GoB }
  }
  final Done success
  from Fork to Done
}
"""


def _fresh():
    defn = definition_from_dsl(ORTHO, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    return runner, exe


def test_reset_with_a_finished_region_respawns_and_does_not_deadlock():
    runner, exe = _fresh()
    runner.inject(exe, Event(kind="GoA"))  # region A finished; B still live; Fork not joined
    assert exe.active_path == "Fork"
    runner.inject(exe, Event(kind="Reset"))  # restart while a region is already DONE
    assert exe.active_path == "Fork"  # back in the AND-state, fresh
    runner.inject(exe, Event(kind="GoA"))
    runner.inject(exe, Event(kind="GoB"))
    assert exe.active_path == "Done"  # pre-fix: deadlocked at Fork forever
    assert exe.status is Status.DONE


def test_reset_after_completion_restarts_the_orthogonal_cleanly():
    runner, exe = _fresh()
    runner.inject(exe, Event(kind="GoA"))
    runner.inject(exe, Event(kind="GoB"))
    assert exe.active_path == "Done"
    runner.inject(exe, Event(kind="Reset"))
    assert exe.active_path == "Fork"  # re-entered the AND-state
    assert not exe.context  # Reset cleared context
    runner.inject(exe, Event(kind="GoA"))
    runner.inject(exe, Event(kind="GoB"))
    assert exe.active_path == "Done"
    assert exe.status is Status.DONE


def test_reset_cancels_a_still_live_region_instead_of_abandoning_it():
    # region A finished (GoA); region B is still live (no GoB yet) when Reset fires.
    # Reset discards exe.children wholesale — without an explicit Cancel, B would
    # just keep running forever, orphaned, never told to stop.
    runner, exe = _fresh()
    runner.inject(exe, Event(kind="GoA"))
    assert exe.active_path == "Fork"
    live_child_ids = [cid for cid, cs in exe.children.items() if not cs.finished]
    assert len(live_child_ids) == 1  # region B, still at B1

    runner.inject(exe, Event(kind="Reset"))

    region_b = runner.get(live_child_ids[0])
    assert region_b.status is Status.CANCELLED  # B has no `on Cancel` -> forceful terminate


def test_reset_is_a_noop_while_cancelling():
    # a Reset arriving mid cooperative-cancel cleanup must not abandon it half-done
    # -- the same hazard a duplicate/racing Cancel already has (see control.py).
    runner, exe = _fresh()
    exe.status = Status.CANCELLING
    before = (exe.status, exe.active_path, dict(exe.context))

    runner.inject(exe, Event(kind="Reset"))

    assert (exe.status, exe.active_path, dict(exe.context)) == before
