"""Execution scenarios + new-engine harness for the engine rewrite.

Self-contained scenarios (inline DSL + deterministic actions defined here) that
exercise the simple hierarchical chain: automatic transitions, on_activity,
selectors, sinks, transition scope/override, and action override-by-depth.

`run_new(scenario)` drives a scenario through the new Definition/Execution/Engine
(via the multi-Execution `_Runner`) and returns a trace (per event: end_state),
the final context and status. `test_engine_parity` asserts these against the
frozen legacy outputs. Actions record their label in `execution_ctx["trace"]` so
we capture *which* actions ran; selectors read the context so branches are
deterministic.
"""

from harel.spec.states import Event


def _action(label):
    def run(stm, event, **kw):
        stm.execution_ctx.setdefault("trace", []).append(label)

    run.__name__ = label
    return run


# Action functions referenced by the scenarios as "scenarios.<name>".
enter_a = _action("enter_a")
enter_b = _action("enter_b")
activity_b = _action("activity_b")
exit_c = _action("exit_c")

enter_decide = _action("enter_decide")
enter_done = _action("enter_done")
enter_retry = _action("enter_retry")


def pick(stm, event, **kw):
    """Selector: returns the configured boolean branch from the context.

    The mapper keys in YAML are `true`/`false` (YAML booleans, coerced to the
    strings "True"/"False"), so the selector must return a bool.
    """
    stm.execution_ctx.setdefault("trace", []).append("pick")
    return stm.execution_ctx.get("pick", True)


def set_note(stm, event, **kw):
    """A region action that writes a context key (so `carry` has something to
    propagate on the region's `Finished`)."""
    stm.execution_ctx["note"] = "from-A"


def join_route(stm, event, **kw):
    """Selector over the orthogonal join: route by whether any region failed,
    reading the engine-exposed `region_results`."""
    results = stm.execution_ctx.get("region_results", {})
    return "failed" if any(r.get("outcome") == "failed" for r in results.values()) else "ok"


def capture_event(stm, event, **kw):
    """Records the triggering event's data into context — used to observe the
    opaque payload a system event carries (e.g. a Cancel `reason`)."""
    stm.execution_ctx["seen"] = dict(event.data)


def decide(stm, event, **kw):
    """A submachine selector: branch on the input context seeded by the parent's
    `invoke ... with`."""
    return "won" if stm.execution_ctx.get("ok") else "lost"


def bump(stm, event, **kw):
    """Increment a `count` in context (a parent looping over an `invoke`)."""
    stm.execution_ctx["count"] = stm.execution_ctx.get("count", 0) + 1


def loop_or_done(stm, event, **kw):
    """Selector: keep looping the invoke while `count` < 2, else finish."""
    return "loop" if stm.execution_ctx.get("count", 0) < 2 else "done"


LINEAR = """
machine M {
  initial A
  state A { on enter scenarios.enter_a }
  state B { on enter scenarios.enter_b  on activity scenarios.activity_b }
  state C { on exit scenarios.exit_c }
  from A to B
  from B to C on Exit
}
"""

SELECTOR = """
machine M {
  initial Decide
  state Decide { on enter scenarios.enter_decide }
  state Done   { on enter scenarios.enter_done }
  state Retry  { on enter scenarios.enter_retry }
  from Decide select scenarios.pick on Go { true to Done  false to Retry }
  from Retry to Decide
}
"""


n_in1 = _action("n_in1")
n_in2 = _action("n_in2")
n_done = _action("n_done")
o_in1 = _action("o_in1")
o_in2 = _action("o_in2")
o_err = _action("o_err")
h_in1 = _action("h_in1")
h_in2 = _action("h_in2")
h_wait = _action("h_wait")
b_in1 = _action("b_in1")
b_inend = _action("b_inend")
b_done = _action("b_done")


NESTED = """
machine M {
  initial Outer
  state Outer {
    initial In1
    state In1 { on enter scenarios.n_in1 }
    state In2 { on enter scenarios.n_in2 }
    from In1 to In2 on Step
  }
  state Done { on enter scenarios.n_done }
  from Outer to Done on Finish
}
"""

# inner In1->In2 on Tick must shadow the outer Outer->Err on Tick
SCOPE_OVERRIDE = """
machine M {
  initial Outer
  state Outer {
    initial In1
    state In1 { on enter scenarios.o_in1 }
    state In2 { on enter scenarios.o_in2 }
    from In1 to In2 on Tick
  }
  state Err { on enter scenarios.o_err }
  from Outer to Err on Tick
}
"""

# `{nohistory}` is either "" (history on) or "no history" (history off)
HISTORY = """
machine M {{
  initial Outer
  state Outer {{
    {nohistory}
    initial In1
    state In1 {{ on enter scenarios.h_in1 }}
    state In2 {{ on enter scenarios.h_in2 }}
    from In1 to In2 on Step
  }}
  state Wait {{ on enter scenarios.h_wait }}
  from Outer to Wait on Pause
  from Wait to Outer on Resume
}}
"""

# Inend is a sink within Outer -> Outer "finishes" and the STM auto-transitions
BUBBLE = """
machine M {
  initial Outer
  state Outer {
    initial In1
    state In1 { on enter scenarios.b_in1 }
    state Inend { on enter scenarios.b_inend }
    from In1 to Inend
  }
  state Done { on enter scenarios.b_done }
  from Outer to Done
}
"""

f_done = _action("f_done")

# Orthogonal (AND-state): Fork runs regions A and B concurrently; each is a
# ParallelState whose leaf is a sink after `Go`. Fork->Done is automatic and
# only fires once BOTH regions finish (the join). Region actions run in each
# region's *own* context (isolated), so the parent context only ever sees the
# parent-level `f_done`.
ORTHOGONAL_JOIN = """
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      initial A1
      state A1 { on enter scenarios.o_in1 }
      state A2 { on enter scenarios.o_in2 }
      from A1 to A2 on Go
    }
    state B {
      initial B1
      state B1 { on enter scenarios.h_in1 }
      state B2 { on enter scenarios.h_in2 }
      from B1 to B2 on Go
    }
  }
  state Done { on enter scenarios.f_done }
  from Fork to Done
}
"""

# Only region A reacts to GoA; B keeps running, so the join does NOT complete on
# GoA (parent stays on Fork). GoB then finishes B and the join fires.
ORTHOGONAL_PENDING = """
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      initial A1
      state A1 { on enter scenarios.o_in1 }
      state A2 { on enter scenarios.o_in2 }
      from A1 to A2 on GoA
    }
    state B {
      initial B1
      state B1 { on enter scenarios.h_in1 }
      state B2 { on enter scenarios.h_in2 }
      from B1 to B2 on GoB
    }
  }
  state Done { on enter scenarios.f_done }
  from Fork to Done
}
"""

# Wait has no on_activity, so the "Cancel falls through to activity" legacy quirk
# is unobservable.
CANCEL_YAML = """
machine M {
  initial Wait
  state Wait { on enter scenarios.enter_b }
  state Next { on enter scenarios.enter_a }
  from Wait to Next on Go
}
"""


SCENARIOS = [
    {"name": "linear", "stm": "M", "dsl": LINEAR, "events": [{"kind": "Work"}, {"kind": "Exit"}]},
    {"name": "nested", "stm": "M", "dsl": NESTED, "events": [{"kind": "Step"}, {"kind": "Finish"}]},
    {"name": "scope_override", "stm": "M", "dsl": SCOPE_OVERRIDE, "events": [{"kind": "Tick"}]},
    {
        "name": "history_resume",
        "stm": "M",
        "dsl": HISTORY.format(nohistory=""),
        "events": [{"kind": "Step"}, {"kind": "Pause"}, {"kind": "Resume"}],
    },
    {
        "name": "history_restart",
        "stm": "M",
        "dsl": HISTORY.format(nohistory="no history"),
        "events": [{"kind": "Step"}, {"kind": "Pause"}, {"kind": "Resume"}],
    },
    {
        "name": "selector_done",
        "stm": "M",
        "dsl": SELECTOR,
        "context": {"pick": True},
        "events": [{"kind": "Go"}],
    },
    {
        "name": "selector_retry",
        "stm": "M",
        "dsl": SELECTOR,
        "context": {"pick": False},
        "events": [{"kind": "Go"}],
    },
    {"name": "bubble", "stm": "M", "dsl": BUBBLE, "events": []},
    {"name": "cancel", "stm": "M", "dsl": CANCEL_YAML, "events": [{"kind": "Cancel"}]},
    {"name": "reset", "stm": "M", "dsl": LINEAR, "events": [{"kind": "Work"}, {"kind": "Reset"}]},
    {
        "name": "set_state",
        "stm": "M",
        "dsl": LINEAR,
        "events": [{"kind": "SetState", "data": {"current_state": "C"}}],
    },
    {"name": "orthogonal_join", "stm": "M", "dsl": ORTHOGONAL_JOIN, "events": [{"kind": "Go"}]},
    {
        "name": "orthogonal_pending",
        "stm": "M",
        "dsl": ORTHOGONAL_PENDING,
        "events": [{"kind": "GoA"}, {"kind": "GoB"}],
    },
]


# --- new-engine harness (a tiny in-memory multi-Execution driver) -------------


# The in-memory runtime is the production `Driver` (Execution-centric core); the
# tests drive raw Executions through it directly.
from harel.engine.runtime import Driver as _Runner  # noqa: E402


def run_new(scenario) -> dict:
    from harel.dsl import definition_from_dsl
    from harel.engine.execution import Execution

    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))

    runner = _Runner(defn)
    runner.start(exe)
    trace = [{"event": "Start", "end_state": exe.active_path}]
    for ev in scenario["events"]:
        event = Event(kind=ev["kind"], data=dict(ev.get("data", {})))
        runner.inject(exe, event)
        trace.append({"event": ev["kind"], "end_state": exe.active_path})

    return {"trace": trace, "context": dict(exe.context), "status": exe.status.value}
