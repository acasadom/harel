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
