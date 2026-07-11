"""An orthogonal region runs its OWN on_enter / on_exit / timeout.

A region is a child Execution whose `root_path` is the region composite. `start`
used to enter the region's initial child directly, skipping the region root's own
ENTER (its hook + arming its timer); the sink stopped exiting at the root, skipping
its EXIT. So a region composite's `on enter` / `on exit` never fired and a `timeout`
declared on a region was never scheduled. The engine now runs the region root's own
ENTER at start and its EXIT when the region completes (a top-level machine root has
no hook/timer, so nothing changes there).
"""

from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore
from harel.spec.states import Event

# region A carries its own enter/exit hooks on the region composite (plus an inner
# hook on A1 to pin the ordering). Both regions finish on Go, then the parent joins.
HOOKS = """
event Go {}
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      on enter stm_actions.rec(at: "A.enter")
      on exit  stm_actions.rec(at: "A.exit")
      initial A1
      state A1 { on enter stm_actions.rec(at: "A1.enter") }
      final A2 success {}
      from A1 to A2 on Go
    }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
  }
  state Done {}
  from Fork to Done
}
"""


def _region_id(store, parent_id, root_path):
    """The region child's execution id, found via its root_path — independent of the
    child-id scheme (which carries a per-entry seq)."""
    parent = store.load(parent_id)
    return next(cid for cid, cs in parent.children.items() if cs.root_path == root_path)


def test_region_composite_runs_its_own_enter_and_exit():
    store = DictStore()
    defn = definition_from_dsl(HOOKS, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # fork -> region A parked at A1
    region_id = _region_id(store, exe.id, "Fork.A")
    runner.process(exe.id, Event(kind="Go"))  # both regions finish -> join -> Done

    assert store.load(exe.id).active_path == "Done"
    region = store.load(region_id)
    assert region.status is Status.DONE
    # region root's OWN enter (outermost) then its initial child, then root's OWN exit
    assert region.context["trace"] == ["A.enter", "A1.enter", "A.exit"]


# region A declares a `timeout` on the region composite itself.
TIMEOUT = """
event Go {}
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      timeout 10
      initial A1
      state A1 {}
      final A2 success {}
      from A1 to A2 on Go
    }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
  }
  state Done {}
  from Fork to Done
}
"""


def test_region_composite_timeout_is_armed_and_cancelled():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(TIMEOUT, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)
    child = _region_id(store, exe.id, "Fork.A")

    # the region composite's own timeout is armed on the region child (fire_at=110)
    assert (child, "Fork.A", 110.0) in store.due_timers(200.0)

    # completing the region (Go -> A reaches its sink) cancels A's timer
    runner.process(exe.id, Event(kind="Go"))
    assert all(t[0] != child for t in store.due_timers(200.0))
    assert store.load(exe.id).active_path == "Done"
