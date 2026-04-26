"""Engine hook semantics (UML, no override-by-depth inheritance).

Deterministic, thread-free tests that pin exactly which enter/exit/activity
actions fire (and in which order) for the new engine. Each scenario is a tiny
declarative STM whose actions record their label in `execution_ctx["trace"]`;
we drive it through the in-memory multi-Execution `_Runner` and assert the
trace, the active position and the status.

Semantics asserted:
- each entered/exited level runs its OWN hook (a state without one fires
  nothing — no borrowing from an ancestor),
- enter is outermost-first, exit is innermost-first (LCA-based),
- a self/local transition (target == active leaf) fires nothing,
- activity runs only the active leaf's own on_activity.
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution
from harel.spec.states import Event

# Actions (the labelled recorders and `pick`) live in the shared `stm_actions`
# module so the configs resolve them by name regardless of this file's location.


def run(dsl: str, events, context=None):
    defn = definition_from_dsl(dsl, "M")
    exe = Execution(definition_id=defn.id, context=dict(context or {}))
    runner = _Runner(defn)
    runner.start(exe)
    for kind in events:
        runner.inject(exe, Event(kind=kind, data={}))
    return exe.context.get("trace", []), exe.active_path, exe.status.value


NO_INHERITANCE = """
machine M {
  initial Outer
  state Outer {
    on enter stm_actions.oe
    on exit stm_actions.ox
    initial In1
    state In1 { on enter stm_actions.in1e }
    state In2 {}
    from In1 to In2 on Go
    from In2 to In1 on Back
  }
}
"""


def test_entering_a_hookless_child_fires_nothing_and_does_not_reenter_parent():
    trace, active, status = run(NO_INHERITANCE, ["Go"])
    # Go: In1 -> In2 (lca = Outer). In2 has no on_enter and Outer is NOT re-entered.
    assert trace == ["oe", "in1e"]
    assert active == "Outer.In2"
    assert status == "RUNNING"


CROSS_LEVEL = """
machine M {
  initial A
  state A { on enter stm_actions.ae  on exit stm_actions.ax }
  state Outer {
    on enter stm_actions.oe
    on exit stm_actions.ox
    initial In1
    state In1 { on enter stm_actions.in1e  on exit stm_actions.in1x }
    state In2 { on enter stm_actions.in2e }
    from In1 to In2 on Step
  }
  from A to "Outer.In1" on Go
  from Outer to A on Back
}
"""


def test_enter_outermost_first_exit_innermost_first():
    trace, active, status = run(CROSS_LEVEL, ["Go", "Back"])
    assert trace == [
        "ae",  # start
        "ax",
        "oe",
        "in1e",  # Go: exit A, enter Outer then In1 (outermost-first)
        "in1x",
        "ox",
        "ae",  # Back: exit In1 then Outer (innermost-first), enter A
    ]
    assert active == "A"
    assert status == "RUNNING"


SELF = """
machine M {
  initial Decide
  state Decide { on enter stm_actions.de  on exit stm_actions.dx }
  state Other { on enter stm_actions.other_e }
  from Decide select stm_actions.pick on Go { true to Decide  false to Other }
}
"""


def test_self_transition_is_local_fires_nothing():
    trace, active, status = run(SELF, ["Go"], context={"pick": True})
    # selector picks Decide (self): no exit, no re-enter
    assert trace == ["de", "pick"]
    assert active == "Decide"
    assert status == "RUNNING"


def test_selector_to_a_different_state_does_enter_it():
    trace, active, status = run(SELF, ["Go"], context={"pick": False})
    # real transition Decide -> Other: exit Decide (dx), enter Other (other_e)
    assert trace == ["de", "pick", "dx", "other_e"]
    assert active == "Other"


ACTIVITY = """
machine M {
  initial W
  state W { on enter stm_actions.we  on activity stm_actions.wa }
  state N { on enter stm_actions.ne }
  from W to N on Go
  from N to W on Reset2
}
"""


def test_activity_runs_only_the_active_leafs_own_hook():
    # Ping has no transition from W -> activity (wa). Then Go -> N (no on_activity):
    # a second Ping at N runs nothing.
    trace, active, status = run(ACTIVITY, ["Ping", "Go", "Ping"])
    assert trace == ["we", "wa", "ne"]
    assert active == "N"


DESCEND = """
machine M {
  initial L1
  state L1 {
    on enter stm_actions.l1e
    initial L2
    state L2 {
      on enter stm_actions.l2e
      initial Leaf
      state Leaf { on enter stm_actions.leafe }
    }
  }
}
"""


def test_composite_descend_runs_each_level_own_hook_outermost_first():
    trace, _, _ = run(DESCEND, [])
    assert trace == ["l1e", "l2e", "leafe"]


# --- Cancel as a modelable event ------------------------------------------------
CANCEL_HANDLED = """
machine M {
  initial Working
  state Working { on enter stm_actions.we }
  state Cleanup { on enter stm_actions.ae }
  from Working to Cleanup on Cancel
}
"""

CANCEL_UNHANDLED = """
machine M {
  initial Working
  state Working { on enter stm_actions.we }
  state Done { on enter stm_actions.ae }
  from Working to Done on Go
}
"""


def test_cancel_with_a_transition_runs_the_modelled_cleanup():
    # a state that models on:Cancel takes that transition (not a forceful terminate);
    # Cleanup is a sink (no outgoing) so the machine then finishes normally
    trace, active, status = run(CANCEL_HANDLED, ["Cancel"])
    assert trace == ["we", "ae"]
    assert active == "Cleanup"
    assert status == "DONE"


def test_cancel_without_a_transition_forcefully_terminates():
    trace, active, status = run(CANCEL_UNHANDLED, ["Cancel"])
    assert trace == ["we"]  # no hooks ran on the forceful terminate
    assert status == "CANCELLED"


def test_has_cancel_handler_reflects_the_active_configuration():
    from harel import engine
    from harel.engine.execution import Execution

    defn = definition_from_dsl(CANCEL_HANDLED, "M")
    exe = Execution(definition_id=defn.id)
    _Runner(defn).start(exe)  # parked at Working, which models on:Cancel
    assert engine.has_cancel_handler(defn, exe) is True

    defn2 = definition_from_dsl(CANCEL_UNHANDLED, "M")
    exe2 = Execution(definition_id=defn2.id)
    _Runner(defn2).start(exe2)
    assert engine.has_cancel_handler(defn2, exe2) is False
