"""`on error` transitions: when a state's action raises, the model can route to a
handler state instead of failing the execution. The synthetic `error` event carries
the exception `type`/`message` (guardable via `where type == ...`), and the exception
is also placed in `context["_error"]`. With no `on error` in scope, the runner's policy
is unchanged (the base Driver re-raises; the production runner fails the execution)."""

import pytest
from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.store import DictStore
from harel.spec.states import Event

# `boom` (stm_actions) raises RuntimeError("boom") on enter.
ROUTE = """
machine api {
  initial Calling
  state Calling { on enter stm_actions.boom }
  state ApiError { on enter stm_actions.rec(at: "recovered") }
  final Failed failed {}
  from Calling to ApiError on error
  from ApiError to Failed
}
"""

TYPED = """
machine m {
  initial Calling
  state Calling { on enter stm_actions.boom }
  final OnRuntime failed {}
  final OnOther   failed {}
  from Calling to OnRuntime on error where type == "RuntimeError"
  from Calling to OnOther   on error
}
"""

NO_HANDLER = """
machine m {
  initial Calling
  state Calling { on enter stm_actions.boom }
  final Done success {}
  from Calling to Done
}
"""

# `boom` raises RuntimeError — this handler only catches ValueError, so no match.
GUARD_MISMATCH = """
machine m {
  initial Calling
  state Calling { on enter stm_actions.boom }
  state Recovered {}
  final Done success {}
  from Calling to Recovered on error where type == "ValueError"
  from Recovered to Done
}
"""

# `boom` raises on entering Calling as the TARGET of an ordinary transition (not the
# initial state, unlike ROUTE). Regression for a bug where `exe.active_path` was
# advanced to the transition's pivot (an ancestor, sometimes the root) before the
# entered state's `on enter` ran, so `on error` scoped to Calling itself could never
# be resolved and the exception always fell through to the runner policy.
TARGET_ROUTE = """
event Go {}
machine api {
  initial Idle
  state Idle {}
  state Calling { on enter stm_actions.boom }
  state ApiError { on enter stm_actions.rec(at: "recovered") }
  final Failed failed {}
  from Idle to Calling on Go
  from Calling to ApiError on error
  from ApiError to Failed
}
"""

# Same bug, but the failing `on enter` fires while descending into a composite's
# initial child (`_descend`) rather than via the plain entry loop (`_take`).
NESTED_ROUTE = """
event Go {}
machine api {
  initial Idle
  state Idle {}
  state Calling {
    initial Dialing
    state Dialing { on enter stm_actions.boom }
    from Dialing to ApiError on error
  }
  state ApiError { on enter stm_actions.rec(at: "recovered") }
  final Failed failed {}
  from Idle to Calling on Go
  from ApiError to Failed
}
"""


# `boom` raises on both Calling's and the handler's own `on enter` — regression for the
# second failure silently replacing the first. `_on_action_error` must see the original
# exception chained via `__cause__` (Python's *implicit* exception context isn't reliable
# here since actions run in a thread pool — see `AsyncDriver._drive`), and the durable
# runner's `error` field must mention both, not just the retry's.
DOUBLE_FAILURE = """
machine m {
  initial Calling
  state Calling { on enter stm_actions.boom }
  state AlsoFails { on enter stm_actions.boom }
  final Failed failed {}
  from Calling to AlsoFails on error
  from AlsoFails to Failed
}
"""


# Regression: Outer's own `on_exit` raises; the `on error` handler is scoped to Leaf
# (a child that already exited cleanly before Outer's `on_exit` ran). Without tracking
# `exe.active_path` through the exit cascade, `has_error_handler` misattributes the
# failure to Leaf's scope, and the recovery transition — reading that same stale path
# — re-exits (and re-runs the side effect of) Leaf a second time.
EXIT_MISATTRIBUTION = """
event Go {}
machine m {
  initial Outer
  state Outer {
    initial Leaf
    state Leaf { on exit stm_actions.rec(at: "exit_leaf") }
    state Recovered { on enter stm_actions.rec(at: "enter_recovered") }
    on exit stm_actions.boom
    from Leaf to Elsewhere on Go
    from Leaf to Recovered on error
    from Recovered to Failed
  }
  final Elsewhere success {}
  final Failed failed {}
}
"""

# Regression: even a *structurally sound* recovery — a self-loop, `from M to M on
# error`, which re-enters M via history without re-running its own `on_exit` — is
# deliberately not attempted. `on_exit` failures never route, full stop: they're
# always treated as a bug, regardless of whether some particular handler could have
# recovered safely.
EXIT_NEVER_ROUTES = """
event Go {}
machine m {
  initial Grand
  state Grand {
    initial M
    state M {
      initial Leaf
      state Leaf { on exit stm_actions.rec(at: "exit_leaf") }
      on exit stm_actions.boom
      from Leaf to Elsewhere on Go
    }
    from M to M on error
  }
  final Elsewhere success {}
}
"""


def _run(dsl: str, name: str) -> Execution:
    defn = definition_from_dsl(dsl, name, validate=True)  # validate accepts `on error`
    exe = Execution(definition_id=defn.id)
    _Runner(defn).start(exe)
    return exe


def test_on_error_routes_to_handler_state():
    exe = _run(ROUTE, "api")
    assert exe.active_path == "Failed" and exe.outcome == "failed"
    assert exe.context["_error"] == {"type": "RuntimeError", "message": "boom"}
    assert exe.context["trace"] == ["recovered"]  # the handler's on_enter ran


def test_on_error_can_be_guarded_by_exception_type():
    exe = _run(TYPED, "m")
    assert exe.active_path == "OnRuntime"  # matched `where type == "RuntimeError"`


def test_on_error_routes_when_the_failure_is_in_a_transition_target():
    """Regression: `on error` must also catch a failure entering a state reached by an
    ordinary transition, not just the machine's initial state (see TARGET_ROUTE)."""
    defn = definition_from_dsl(TARGET_ROUTE, "api", validate=True)
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind="Go"))
    assert exe.active_path == "Failed" and exe.outcome == "failed"
    assert exe.context["_error"] == {"type": "RuntimeError", "message": "boom"}
    assert exe.context["trace"] == ["recovered"]


def test_on_error_routes_during_composite_descent():
    """Regression: same as above, for a failure entering a composite's initial child
    (`_descend`) rather than a plain transition target (`_take`); see NESTED_ROUTE."""
    defn = definition_from_dsl(NESTED_ROUTE, "api", validate=True)
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind="Go"))
    assert exe.active_path == "Failed" and exe.outcome == "failed"
    assert exe.context["_error"] == {"type": "RuntimeError", "message": "boom"}
    assert exe.context["trace"] == ["recovered"]


def test_unhandled_action_error_reraises_in_the_base_driver():
    defn = definition_from_dsl(NO_HANDLER, "m")
    with pytest.raises(RuntimeError, match="boom"):
        _Runner(defn).start(Execution(definition_id=defn.id))


def test_unhandled_action_error_fails_the_execution_under_durable_runner():
    store = DictStore()
    defn = definition_from_dsl(NO_HANDLER, "m")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # the runtime policy: catch -> FAILED (dead-letter), no re-raise
    final = store.load(exe.id)
    assert final.status is Status.FAILED
    assert "boom" in (final.error or "")


def test_unmatched_guard_falls_through_to_runner_policy():
    """A guarded `on error where type == X` that doesn't match the raised exception
    must fall through to the runner policy (re-raise / FAILED), not silently succeed."""
    store = DictStore()
    defn = definition_from_dsl(GUARD_MISMATCH, "m")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    final = store.load(exe.id)
    assert final.status is Status.FAILED  # guard didn't match → runner policy
    assert "RuntimeError" in (final.error or "")


def test_on_error_routes_under_the_durable_runner():
    store = DictStore()
    defn = definition_from_dsl(ROUTE, "api")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    final = store.load(exe.id)
    assert final.active_path == "Failed" and final.status is Status.DONE  # routed, not FAILED
    assert final.context["_error"]["type"] == "RuntimeError"


def test_on_error_handler_failure_chains_the_original_exception():
    """If the `on error` handler's own action also raises, `in_error` blocks a second
    recovery attempt, but the original exception that triggered routing must not be
    silently discarded — it's chained onto the retry's exception via `__cause__`."""
    defn = definition_from_dsl(DOUBLE_FAILURE, "m")
    with pytest.raises(RuntimeError) as excinfo:
        _Runner(defn).start(Execution(definition_id=defn.id))
    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "boom"


def test_on_error_handler_failure_chain_reaches_the_durable_error_field():
    store = DictStore()
    defn = definition_from_dsl(DOUBLE_FAILURE, "m")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    final = store.load(exe.id)
    assert final.status is Status.FAILED
    assert "while recovering from RuntimeError: boom" in (final.error or "")


def test_on_error_does_not_misattribute_an_ancestors_exit_failure():
    """Leaf already exited cleanly by the time Outer's own `on_exit` raises — a
    handler scoped to Leaf must not fire (and, in particular, must not re-run
    Leaf's `on_exit` a second time as a side effect of a bogus recovery attempt)."""
    defn = definition_from_dsl(EXIT_MISATTRIBUTION, "m", validate=True)
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    with pytest.raises(RuntimeError, match="boom"):
        runner.inject(exe, Event(kind="Go"))
    assert exe.context["trace"] == ["exit_leaf"]  # ran once, not twice


def test_on_error_never_routes_an_exit_failure():
    """Even a self-loop handler (`from M to M on error`) that would recover cleanly
    without re-running M's own failing `on_exit` is not attempted: `on_exit` must
    always succeed, so a raise there always falls straight to the runner policy."""
    defn = definition_from_dsl(EXIT_NEVER_ROUTES, "m", validate=True)
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    with pytest.raises(RuntimeError, match="boom"):
        runner.inject(exe, Event(kind="Go"))
    assert "_error" not in exe.context  # no routing was even attempted


def test_on_error_never_routes_an_exit_failure_under_the_durable_runner():
    store = DictStore()
    defn = definition_from_dsl(EXIT_NEVER_ROUTES, "m", validate=True)
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))
    final = store.load(exe.id)
    assert final.status is Status.FAILED
    assert "boom" in (final.error or "")
    assert "_error" not in final.context
