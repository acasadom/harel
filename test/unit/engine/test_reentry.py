"""Regression tests for orthogonal / fan-out RE-ENTRY.

Re-entering an orthogonal (AND) state or a fan-out invoke in a loop must spawn
FRESH region child Executions each time. Before the fix, the deterministic region
child ids had no per-entry sequence and the finished children from the previous
entry were reused, so the relay's idempotent create skipped them: the regions never
re-ran, no `Finished` was re-emitted, and the join deadlocked forever (the parent
stayed parked on the AND-state / fan-out node). See core._fork / _fan_out /
_leave_regions, which mirror the single `invoke` per-entry `invoke_seq` id.
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.resolve import DictResolver
from harel.engine.store import DictStore
from harel.spec.states import Event

# --- orthogonal re-entry (in-memory Driver; no submachine resolver needed) ------

ORTHO_LOOP = """
event Go {}
event Again {}
machine M {
  initial Fork
  orthogonal Fork {
    state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on Go }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
  }
  state Mid {}
  from Fork to Mid
  from Mid to Fork on Again
}
"""


def test_orthogonal_state_can_be_re_entered():
    defn = definition_from_dsl(ORTHO_LOOP, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    assert exe.active_path == "Fork"
    runner.inject(exe, Event(kind="Go"))
    assert exe.active_path == "Mid"  # both regions finished -> join
    runner.inject(exe, Event(kind="Again"))
    assert exe.active_path == "Fork"  # re-entered the AND-state
    runner.inject(exe, Event(kind="Go"))
    assert exe.active_path == "Mid"  # pre-fix: deadlocked, stuck at Fork forever


# --- fan-out re-entry (DurableRunner + resolver; exercises the relay spawn) ------

CHILD = """
machine child {
  carry score
  initial Decide
  state Decide {}
  final Won  success
  final Lost failed
  from Decide select scenarios.decide { "won" to Won  "lost" to Lost }
}
"""

LOOPFAN = """
event Again {}
event Stop {}
machine loopfan {
  initial Process
  state Process {
    invoke acme.child for slice in slices
    with { ok: slice }
  }
  state Waiting {}
  final Done   success
  final Failed failed
  from Process join all to Waiting else to Failed
  from Waiting to Process on Again
  from Waiting to Done on Stop
}
"""


def test_fanout_invoke_can_be_re_entered():
    store = DictStore()
    child = definition_from_dsl(CHILD, "child")
    loopfan = definition_from_dsl(LOOPFAN, "loopfan")
    runner = DurableRunner(store, {loopfan.id: loopfan}, resolver=DictResolver({"acme.child": child}))
    exe = runner.create(loopfan.id, context={"slices": [True, True]})
    assert store.load(exe.id).active_path == "Waiting"  # first fan-out joined

    runner.process(exe.id, Event(kind="Again"))
    assert store.load(exe.id).active_path == "Waiting"  # pre-fix: stuck at Process (deadlock)

    # a fresh addressed instance per entry (seq 0 then seq 1), all completed
    for cid in (
        f"{exe.id}:Process:0:0",
        f"{exe.id}:Process:0:1",
        f"{exe.id}:Process:1:0",
        f"{exe.id}:Process:1:1",
    ):
        assert store.load(cid).status is Status.DONE

    runner.process(exe.id, Event(kind="Stop"))
    assert store.load(exe.id).active_path == "Done"
