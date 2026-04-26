"""The STM battery: the primary execution spec for the engine.

A declarative set of state-machine configs (DSL), each driven through the engine
with a sequence of events; we assert the active position after every step, the
exact ordered action trace, and the final status. Expectations are derived from
the intended (UML) semantics — own hook per entered/exited level, no ancestor
inheritance, LCA-based enter/exit, local self-transitions.

Recording mechanism: every hook is `rec` with its label in `inputs.at`; a
selector is `sel`, which records `inputs.at` and returns the next branch key from
`context["picks"]` (so a machine can loop through different selector outcomes).
"""

import pytest
from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution
from harel.spec.states import Event

# hooks (`rec`) and the selector (`sel`) are resolved by name from `stm_actions`.


def _run(case):
    defn = definition_from_dsl(case["dsl"], "M")
    ctx = dict(case.get("context", {}))
    ctx["picks"] = list(case.get("picks", []))
    exe = Execution(definition_id=defn.id, context=ctx)
    runner = _Runner(defn)
    runner.start(exe)
    states = [exe.active_path]
    for ev in case["events"]:
        kind = ev if isinstance(ev, str) else ev["kind"]
        data = {} if isinstance(ev, str) else ev.get("data", {})
        runner.inject(exe, Event(kind=kind, data=data))
        states.append(exe.active_path)
    return states, exe.context.get("trace", []), exe.status.value


def _h(label):
    """A hook referencing `rec` with its label (DSL helper kept terse)."""
    return f'stm_actions.rec(at: "{label}")'


# Each case: dsl, events, and the expected (states-after-Start-and-each-event,
# ordered action trace, final status). `picks` feeds selectors.
BATTERY = [
    # --- composable filter: `any` over event data + missing-field fails ------
    {
        "name": "composite_filter_any",
        "dsl": f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")} }}
  from A to B on Go where n == 1 or n == 2
}}
""",
        "events": [{"kind": "Go", "data": {"n": 9}}, {"kind": "Go", "data": {"n": 2}}],
        "states": ["A", "A", "B"],  # n=9 no match (stays A), n=2 matches -> B
        "trace": ["A.enter", "B.enter"],
        "status": "DONE",
    },
    # --- flat: automatic + event transition, sink finishes -------------------
    {
        "name": "flat_linear",
        "dsl": f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")}  on activity {_h("B.act")} }}
  state C {{ on enter {_h("C.enter")}  on exit {_h("C.exit")} }}
  from A to B
  from B to C on Go
}}
""",
        "events": ["Ping", "Go"],
        "states": ["B", "B", "C"],
        "trace": ["A.enter", "B.enter", "B.act", "C.enter", "C.exit"],
        "status": "DONE",
    },
    # --- composite: enter/descend, sink bubbles up to the composite ----------
    {
        "name": "composite_bubble",
        "dsl": f"""
machine M {{
  initial Outer
  state Outer {{
    on enter {_h("Outer.enter")}
    initial In1
    state In1 {{ on enter {_h("In1.enter")} }}
    state Inend {{ on enter {_h("Inend.enter")} }}
    from In1 to Inend
  }}
  state Done {{ on enter {_h("Done.enter")} }}
  from Outer to Done
}}
""",
        "events": [],
        "states": ["Done"],
        "trace": ["Outer.enter", "In1.enter", "Inend.enter", "Done.enter"],
        "status": "DONE",
    },
    # --- transition scope: inner shadows outer on the same event -------------
    {
        "name": "scope_override",
        "dsl": f"""
machine M {{
  initial Outer
  state Outer {{
    initial In1
    state In1 {{ on enter {_h("In1.enter")} }}
    state In2 {{ on enter {_h("In2.enter")} }}
    from In1 to In2 on Tick
    from In2 to In1 on Back
  }}
  state Err {{ on enter {_h("Err.enter")} }}
  from Outer to Err on Tick
}}
""",
        "events": ["Tick"],
        "states": ["Outer.In1", "Outer.In2"],  # inner In1->In2 wins, not Outer->Err
        "trace": ["In1.enter", "In2.enter"],
        "status": "RUNNING",
    },
    # --- history: re-entering a composite resumes the last child -------------
    {
        "name": "history_resume",
        "dsl": f"""
machine M {{
  initial Outer
  state Outer {{
    initial In1
    state In1 {{ on enter {_h("In1.enter")} }}
    state In2 {{ on enter {_h("In2.enter")} }}
    from In1 to In2 on Step
    from In2 to In1 on Noop
  }}
  state Wait {{ on enter {_h("Wait.enter")} }}
  from Outer to Wait on Pause
  from Wait to Outer on Resume
}}
""",
        "events": ["Step", "Pause", "Resume"],
        "states": ["Outer.In1", "Outer.In2", "Wait", "Outer.In2"],  # resumes In2
        "trace": ["In1.enter", "In2.enter", "Wait.enter", "In2.enter"],
        "status": "RUNNING",
    },
    # --- history disabled: re-entering restarts at the start child -----------
    {
        "name": "history_restart",
        "dsl": f"""
machine M {{
  initial Outer
  state Outer {{
    no history
    initial In1
    state In1 {{ on enter {_h("In1.enter")} }}
    state In2 {{ on enter {_h("In2.enter")} }}
    from In1 to In2 on Step
    from In2 to In1 on Noop
  }}
  state Wait {{ on enter {_h("Wait.enter")} }}
  from Outer to Wait on Pause
  from Wait to Outer on Resume
}}
""",
        "events": ["Step", "Pause", "Resume"],
        "states": ["Outer.In1", "Outer.In2", "Wait", "Outer.In1"],  # restarts In1
        "trace": ["In1.enter", "In2.enter", "Wait.enter", "In1.enter"],
        "status": "RUNNING",
    },
    # --- selector retry loop: Decide -> Retry -> Decide -> Done --------------
    {
        "name": "selector_retry_then_done",
        "dsl": f"""
machine M {{
  initial Decide
  state Decide {{ on enter {_h("Decide.enter")} }}
  state Retry {{ on enter {_h("Retry.enter")} }}
  state Done {{ on enter {_h("Done.enter")} }}
  from Decide select stm_actions.sel on Go {{ "retry" to Retry  "done" to Done }}
  from Retry to Decide
}}
""",
        "picks": ["retry", "done"],
        "events": ["Go", "Go"],
        "states": ["Decide", "Decide", "Done"],
        "trace": [
            "Decide.enter",
            "pick",
            "Retry.enter",
            "Decide.enter",  # Go #1: retry -> Retry -> (auto) Decide
            "pick",
            "Done.enter",  # Go #2: done -> Done
        ],
        "status": "DONE",
    },
    # --- selector `else`: an unmatched result falls back to the default target -
    {
        "name": "selector_else_default",
        "dsl": f"""
machine M {{
  initial Decide
  state Decide {{ on enter {_h("Decide.enter")} }}
  state Known {{ on enter {_h("Known.enter")} }}
  state Fallback {{ on enter {_h("Fallback.enter")} }}
  from Decide select stm_actions.sel on Go {{ "known" to Known  else to Fallback }}
}}
""",
        "picks": ["surprise"],  # not in the mapper -> routes to the `else` (Fallback)
        "events": ["Go"],
        "states": ["Decide", "Fallback"],
        "trace": ["Decide.enter", "pick", "Fallback.enter"],
        "status": "DONE",
    },
    # --- cross-level: enter into a nested leaf, then exit the composite -------
    {
        "name": "cross_level",
        "dsl": f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")}  on exit {_h("A.exit")} }}
  state Outer {{
    on enter {_h("Outer.enter")}
    on exit {_h("Outer.exit")}
    initial In1
    state In1 {{ on enter {_h("In1.enter")}  on exit {_h("In1.exit")} }}
    state In2 {{}}
    from In1 to In2 on Step
  }}
  from A to "Outer.In1" on Go
  from Outer to A on Back
}}
""",
        "events": ["Go", "Back"],
        "states": ["A", "Outer.In1", "A"],
        "trace": [
            "A.enter",
            "A.exit",
            "Outer.enter",
            "In1.enter",  # Go: exit A; enter Outer then In1
            "In1.exit",
            "Outer.exit",
            "A.enter",  # Back: exit In1 then Outer; enter A
        ],
        "status": "RUNNING",
    },
    # --- no inheritance: a hookless child fires nothing on entry -------------
    {
        "name": "no_inheritance",
        "dsl": f"""
machine M {{
  initial Outer
  state Outer {{
    on enter {_h("Outer.enter")}
    initial In1
    state In1 {{ on enter {_h("In1.enter")} }}
    state In2 {{}}
    from In1 to In2 on Go
    from In2 to In1 on Back
  }}
}}
""",
        "events": ["Go"],
        "states": ["Outer.In1", "Outer.In2"],
        "trace": ["Outer.enter", "In1.enter"],  # In2 has no hook, Outer not re-entered
        "status": "RUNNING",
    },
    # --- self-transition is local (fires nothing) ----------------------------
    {
        "name": "self_transition_local",
        "dsl": f"""
machine M {{
  initial Decide
  state Decide {{ on enter {_h("Decide.enter")}  on exit {_h("Decide.exit")} }}
  state Other {{ on enter {_h("Other.enter")} }}
  from Decide select stm_actions.sel on Go {{ "self" to Decide  "other" to Other }}
}}
""",
        "picks": ["self"],
        "events": ["Go"],
        "states": ["Decide", "Decide"],
        "trace": ["Decide.enter", "pick"],  # local: no exit, no re-enter
        "status": "RUNNING",
    },
    # --- control: Cancel / Reset / SetState ----------------------------------
    {
        "name": "cancel",
        "dsl": f"""
machine M {{
  initial Wait
  state Wait {{ on enter {_h("Wait.enter")} }}
  state Next {{ on enter {_h("Next.enter")} }}
  from Wait to Next on Go
}}
""",
        "events": ["Cancel"],
        "states": ["Wait", "Wait"],
        "trace": ["Wait.enter"],
        "status": "CANCELLED",
    },
    {
        "name": "reset",
        "dsl": f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")} }}
  from A to B
  from B to A on Loop
}}
""",
        "events": ["Reset"],
        "states": ["B", "B"],
        # Reset clears the context (so the pre-reset trace is gone) and re-runs start
        "trace": ["A.enter", "B.enter"],
        "status": "RUNNING",
    },
    {
        "name": "set_state",
        "dsl": f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")}  on exit {_h("B.exit")} }}
  state C {{ on enter {_h("C.enter")} }}
  from A to B
  from B to C on Go
}}
""",
        "events": [{"kind": "SetState", "data": {"current_state": "C"}}],
        "states": ["B", "C"],  # SetState repositions; C is a sink
        "trace": ["A.enter", "B.enter"],  # SetState does not run enters
        "status": "DONE",
    },
    # --- orthogonal: join only after both regions finish ---------------------
    {
        "name": "orthogonal_join",
        "dsl": f"""
machine M {{
  initial Fork
  orthogonal Fork {{
    state A {{
      initial A1
      state A1 {{ on enter {_h("A1.enter")} }}
      state A2 {{ on enter {_h("A2.enter")} }}
      from A1 to A2 on Go
    }}
    state B {{
      initial B1
      state B1 {{ on enter {_h("B1.enter")} }}
      state B2 {{ on enter {_h("B2.enter")} }}
      from B1 to B2 on Go
    }}
  }}
  state Done {{ on enter {_h("Done.enter")} }}
  from Fork to Done
}}
""",
        "events": ["Go"],
        "states": ["Fork", "Done"],
        "trace": ["Done.enter"],  # region actions run in each region's own context
        "status": "DONE",
    },
]


@pytest.mark.parametrize("case", BATTERY, ids=lambda c: c["name"])
def test_battery(case):
    states, trace, status = _run(case)
    assert states == case["states"], f"states for {case['name']}"
    assert trace == case["trace"], f"trace for {case['name']}"
    assert status == case["status"], f"status for {case['name']}"
