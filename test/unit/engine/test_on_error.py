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
