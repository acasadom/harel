"""Deferred events (`defer`): an event with no transition in the current state is
held in a per-execution FIFO and re-delivered on entering a state that handles it,
instead of being dropped. `defer` is inherited down the tree (machine-level applies
everywhere, state-level to that state + substates, intermediate composite to its
substates)."""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution, Status
from harel.spec.states import Event

# machine-level defer: a webhook that can race ahead of the ack that reaches its handler.
MACHINE_LEVEL = """
event GatewayAck {}
event PaymentConfirmed {}
machine payment {
  defer PaymentConfirmed
  initial Processing
  state Processing {}
  state WaitingWebhook {}
  final Done success {}
  from Processing to WaitingWebhook on GatewayAck
  from WaitingWebhook to Done on PaymentConfirmed
}
"""

# state-level defer: only A holds Early; B handles it. `Other` is declared but never
# handled nor deferred — it must still be dropped.
PER_STATE = """
event Go {}
event Early {}
event Other {}
machine m {
  initial A
  state A { defer Early }
  state B {}
  final Done success {}
  from A to B on Go
  from B to Done on Early
}
"""

# FIFO ordering: both Alpha and Beta are handleable from B; Alpha must fire first.
FIFO_ORDER = """
event Next {}
event Alpha {}
event Beta {}
machine m {
  defer Alpha, Beta
  initial A
  state A {}
  state B {}
  final FromAlpha alpha {}
  final FromBeta  beta  {}
  from A to B        on Next
  from B to FromAlpha on Alpha
  from B to FromBeta  on Beta
}
"""

# Grandchild depth: defer on the machine root; active state is two levels deep (Outer.Inner).
# Inner has its own transition (declared inside Outer, its parent scope) so the drain keeps it active.
GRANDCHILD = """
event Trigger {}
event Signal {}
machine m {
  defer Signal
  initial Outer
  state Outer {
    initial Inner
    state Inner {}
    from Inner to After on Trigger
  }
  state After {}
  final Done success {}
  from After to Done on Signal
}
"""

# Intermediate composite: defer on Outer (not the root, not the leaf).
# Deep has its own transition (inside Outer) so it stays active at Outer.Deep.
INTERMEDIATE = """
event Go {}
event Ready {}
machine m {
  initial Outer
  state Outer {
    defer Ready
    initial Deep
    state Deep {}
    from Deep to After on Go
  }
  state After {}
  final Done success {}
  from After to Done on Ready
}
"""


def _started(dsl: str, name: str):
    defn = definition_from_dsl(dsl, name, validate=True)  # validate accepts `defer`
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    return runner, exe


def test_machine_level_defer_holds_then_redelivers():
    runner, exe = _started(MACHINE_LEVEL, "payment")
    runner.inject(exe, Event(kind="PaymentConfirmed"))  # arrives early, in Processing
    assert exe.active_path == "Processing"  # not dropped, not acted on
    assert [e.kind for e in exe.deferred] == ["PaymentConfirmed"]  # held

    runner.inject(exe, Event(kind="GatewayAck"))  # -> WaitingWebhook, which handles it
    assert exe.active_path == "Done"
    assert exe.status is Status.DONE
    assert exe.deferred == []  # drained


def test_state_level_defer():
    runner, exe = _started(PER_STATE, "m")
    runner.inject(exe, Event(kind="Early"))  # held by A
    assert exe.active_path == "A" and [e.kind for e in exe.deferred] == ["Early"]
    runner.inject(exe, Event(kind="Go"))  # -> B handles the deferred Early -> Done
    assert exe.active_path == "Done" and exe.deferred == []


def test_undeferred_unhandled_event_is_dropped():
    runner, exe = _started(PER_STATE, "m")
    runner.inject(exe, Event(kind="Other"))  # A neither handles nor defers Other
    assert exe.active_path == "A" and exe.deferred == []  # dropped, not held


def test_deferred_event_survives_json_round_trip():
    runner, exe = _started(MACHINE_LEVEL, "payment")
    runner.inject(exe, Event(kind="PaymentConfirmed"))  # held
    again = Execution.model_validate_json(exe.model_dump_json())
    assert [ev.kind for ev in again.deferred] == ["PaymentConfirmed"]


def test_fifo_order():
    runner, exe = _started(FIFO_ORDER, "m")
    runner.inject(exe, Event(kind="Alpha"))
    runner.inject(exe, Event(kind="Beta"))
    assert [e.kind for e in exe.deferred] == ["Alpha", "Beta"]
    # Next -> B; both Alpha and Beta handleable; Alpha (first in queue) must fire first
    runner.inject(exe, Event(kind="Next"))
    assert exe.outcome == "alpha"
    assert [e.kind for e in exe.deferred] == ["Beta"]  # Beta stays; machine is done


def test_grandchild_depth_inherits_machine_defer():
    runner, exe = _started(GRANDCHILD, "m")
    assert exe.active_path == "Outer.Inner"
    runner.inject(exe, Event(kind="Signal"))  # defer on root; active is Outer.Inner (grandchild)
    assert [e.kind for e in exe.deferred] == ["Signal"]
    runner.inject(exe, Event(kind="Trigger"))  # Inner -> After; drain re-delivers Signal -> Done
    assert exe.active_path == "Done" and exe.deferred == []


def test_intermediate_composite_defer():
    runner, exe = _started(INTERMEDIATE, "m")
    assert exe.active_path == "Outer.Deep"
    runner.inject(exe, Event(kind="Ready"))  # defer on Outer; active is Deep (substate)
    assert [e.kind for e in exe.deferred] == ["Ready"]
    runner.inject(exe, Event(kind="Go"))  # Deep -> After; drain re-delivers Ready -> Done
    assert exe.active_path == "Done" and exe.deferred == []
