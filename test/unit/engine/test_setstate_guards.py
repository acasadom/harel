"""`SetState` is a low-level repositioning primitive (`redrive` is the safe,
validated, control-plane way to repair a dead-lettered execution) — but it still
refuses the ways it could corrupt or orphan the record, regardless of who calls
it: an unresolvable/unsafe target, live unfinished children, a cooperative-cancel
cleanup in progress, or an execution that isn't currently RUNNING (unlike `Reset`,
which deliberately allows restarting from DONE/FAILED — SetState repositions to
an arbitrary mid-flow state while keeping history/context intact, so resurrecting
an already-finished or dead-lettered execution into a live position is exactly
the unguarded power `redrive` exists to gate).
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.store import DictStore
from harel.spec.states import Event

LINEAR = """
event Bump {}
machine M {
  initial A
  state A { on enter stm_actions.rec(at: "A.enter") }
  state B { on enter stm_actions.rec(at: "B.enter") }
  final C success { on enter stm_actions.rec(at: "C.enter") }
  from A to B
  from B to C on Bump
}
"""

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

# Working's on-enter always raises -> dead-letters (status=FAILED) immediately on
# start, with no `on error` to catch it.
BOOM = """
machine M {
  initial Working
  state Working { on enter stm_actions.boom }
  state Idle {}
}
"""

# a gate before Fork, so a machine can sit at Idle with children={} — the fork
# never having run yet — to exercise SetState teleporting straight into a branch
# without ever forking it.
GATED_ORTHO = """
event Go {}
event GoA {}
event GoB {}
machine M {
  initial Idle
  state Idle {}
  orthogonal Fork {
    state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on GoA }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on GoB }
  }
  final Done success
  from Idle to Fork on Go
  from Fork to Done
}
"""


def _linear():
    defn = definition_from_dsl(LINEAR, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    return runner, exe


def test_set_state_repositions_to_a_valid_target():
    runner, exe = _linear()
    assert exe.active_path == "B"
    runner.inject(exe, Event(kind="SetState", data={"current_state": "C"}))
    assert exe.active_path == "C"
    assert exe.status is Status.DONE  # C is a sink with an outcome -> drains to DONE


def test_set_state_does_not_resurrect_a_finished_execution():
    # unlike Reset (deliberately allowed from DONE/FAILED -- restarting from
    # scratch is the whole point), SetState repositions to an arbitrary *mid-flow*
    # state while keeping history/context intact: resurrecting an already-DONE
    # execution into a live position with none of redrive's safeguards is exactly
    # the unguarded power redrive exists to gate.
    runner, exe = _linear()
    runner.inject(exe, Event(kind="Bump"))  # B -> C, a sink -> DONE
    assert exe.status is Status.DONE
    assert exe.active_path == "C"

    runner.inject(exe, Event(kind="SetState", data={"current_state": "B"}))

    assert exe.status is Status.DONE  # refused: not resurrected back to RUNNING
    assert exe.active_path == "C"


def test_set_state_does_not_resurrect_a_dead_letter():
    # DurableRunner, not the bare _Runner: a dead letter must fail gracefully
    # (status=FAILED), not propagate the action error, to observe it here.
    defn = definition_from_dsl(BOOM, "M")
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)  # Working's on-enter always raises
    assert exe.status is Status.FAILED

    after = runner.process(exe.id, Event(kind="SetState", data={"current_state": "Idle"}))

    assert after.status is Status.FAILED  # refused -- redrive() is the sanctioned way


def test_set_state_is_a_noop_for_an_unresolvable_target():
    runner, exe = _linear()
    before = (exe.active_path, exe.status, exe.version)
    runner.inject(exe, Event(kind="SetState", data={"current_state": "NoSuchState"}))
    assert (exe.active_path, exe.status) == before[:2]


def test_set_state_is_a_noop_while_cancelling():
    runner, exe = _linear()
    exe.status = Status.CANCELLING
    before = (exe.status, exe.active_path)
    runner.inject(exe, Event(kind="SetState", data={"current_state": "C"}))
    assert (exe.status, exe.active_path) == before


def test_set_state_is_a_noop_with_a_still_live_child():
    defn = definition_from_dsl(ORTHO, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind="GoA"))  # region A finished; B still live
    assert exe.active_path == "Fork"
    before_children = dict(exe.children)

    runner.inject(exe, Event(kind="SetState", data={"current_state": "Done"}))

    assert exe.active_path == "Fork"  # unchanged: refused, did not teleport away
    assert exe.children == before_children  # region B untouched, not orphaned


def test_set_state_refuses_a_leaf_inside_an_unforked_orthogonal_branch():
    # a leaf nested under an orthogonal node resolves fine in the shared
    # Definition, but teleporting straight into it bypasses the fork entirely:
    # no sibling region is ever spawned, exe.children stays {}, and the AND-
    # state's join would fire the moment this one branch alone reaches a sink —
    # vacuously "joined" with region B having never run at all.
    defn = definition_from_dsl(GATED_ORTHO, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    assert exe.active_path == "Idle"

    runner.inject(exe, Event(kind="SetState", data={"current_state": "Fork.A.A1"}))
    assert exe.active_path == "Idle"  # refused, not teleported into the branch
    assert exe.children == {}

    # sanity: still refused, so a follow-up event has nothing to act on either —
    # region B is never silently skipped
    runner.inject(exe, Event(kind="GoA"))
    assert exe.active_path == "Idle"  # still refused; GoA has no effect from Idle


def test_set_state_refuses_a_target_outside_this_executions_own_branch():
    # a region's own Execution (root_path="Fork.A") targeting a path under its
    # SIBLING region (Fork.B) resolves in the shared Definition (defn.index isn't
    # scoped per-Execution) but isn't a descendant of this Execution's root_path
    # -- _drain's chain(root, active) would assert on that, uncaught, if this
    # weren't refused first.
    defn = definition_from_dsl(ORTHO, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    region_a_id = next(cid for cid, cs in exe.children.items() if cs.root_path == "Fork.A")
    region_a = runner.get(region_a_id)
    assert region_a.active_path == "Fork.A.A1"

    runner.inject(region_a, Event(kind="SetState", data={"current_state": "Fork.B.B1"}))

    assert region_a.active_path == "Fork.A.A1"  # refused, not crashed
