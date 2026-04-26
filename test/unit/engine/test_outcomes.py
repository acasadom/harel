"""Typed terminal outcomes: a terminal state can declare `outcome:` (the model's
result label, e.g. "failed"). On reaching it the Execution ends `status=DONE` with
that `outcome`; a plain terminal leaves `outcome=None`. `status` stays the lifecycle
(infra); `outcome` is the program's result (domain) — orthogonal concerns.
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution, Status
from harel.spec.states import Event

# Done has no outcome (plain success); Failed declares outcome: failed.
M = """
machine M {
  initial Work
  state Work {}
  state Done {}
  final Failed failed
  from Work to Done on Ok
  from Work to Failed on Boom
}
"""


def _run(events):
    defn = definition_from_dsl(M, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    for kind in events:
        runner.inject(exe, Event(kind=kind))
    return exe


def test_terminal_with_outcome_records_it():
    exe = _run(["Boom"])
    assert exe.active_path == "Failed"
    assert exe.status is Status.DONE  # status = lifecycle (finished)
    assert exe.outcome == "failed"  # outcome = the model's result label


def test_plain_terminal_leaves_outcome_none():
    exe = _run(["Ok"])
    assert exe.active_path == "Done"
    assert exe.status is Status.DONE
    assert exe.outcome is None


def test_reset_clears_a_recorded_outcome():
    defn = definition_from_dsl(M, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind="Boom"))
    assert exe.outcome == "failed"

    runner.inject(exe, Event(kind="Reset"))
    assert exe.outcome is None
    assert exe.active_path == "Work"
    assert exe.status is Status.RUNNING


def test_outcome_round_trips_through_json():
    exe = _run(["Boom"])
    again = Execution.model_validate_json(exe.model_dump_json())
    assert again.outcome == "failed"
