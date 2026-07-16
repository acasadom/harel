"""Cancelling orthogonal regions on an early exit.

Leaving an orthogonal node while a region is still running emits a `Cancel` to it
(fire-and-forget). Two triggers: `cancel_on_failure` (a region reaches a non-success
terminal) and a `timeout` on the orthogonal node itself. A clean join — every region
finished — cancels nothing.
"""

from harel import DictStore, DurableRunner, Event, definition_from_dsl
from harel.engine.execution import Status

# cancel_on_failure: Fraud can fail; Stock is slower. Fraud's failure must cancel Stock
# and resolve the join now (routing via `else`).
EAGER = """
event FailFraud {}
event ReserveStock {}
machine order {
  initial Verifying
  orthogonal Verifying {
    cancel_on_failure
    state Fraud {
      initial Checking
      state Checking {}
      final Rejected failed {}
      from Checking to Rejected on FailFraud
    }
    state Stock {
      initial Reserving
      state Reserving {}
      final Reserved success {}
      from Reserving to Reserved on ReserveStock
    }
  }
  final Approved success {}
  final Failed failed {}
  from Verifying join all to Approved else to Failed
}
"""

# timeout on the orthogonal node: neither region finishes in time.
TIMED = """
event Go {}
machine order {
  initial Verifying
  orthogonal Verifying {
    timeout 50
    state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on Go }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
  }
  final Approved success {}
  final Failed failed {}
  final TimedOut failed {}
  from Verifying join all to Approved else to Failed
  from Verifying to TimedOut on Timeout
}
"""

# both regions finish on one event -> a clean join, nothing to cancel.
CLEAN = """
event Go {}
machine order {
  initial Verifying
  orthogonal Verifying {
    state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on Go }
    state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
  }
  final Approved success {}
  final Failed failed {}
  from Verifying join all to Approved else to Failed
}
"""


def _region_status(store, parent_id: str) -> dict[str, Status]:
    parent = store.load(parent_id)
    out = {}
    for cid in parent.children:
        child = store.load(cid)
        out[child.root_path] = child.status
    return out


def test_cancel_on_failure_cancels_the_other_region_and_joins_now():
    store = DictStore()
    defn = definition_from_dsl(EAGER, "order", validate=True)
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)

    exe = runner.process(exe.id, Event(kind="FailFraud"))  # Fraud fails; Stock still reserving
    assert exe.active_path == "Failed" and exe.outcome == "failed"  # joined now, via `else`
    status = _region_status(store, exe.id)
    assert status["Verifying.Fraud"] is Status.DONE  # reached its own terminal
    assert status["Verifying.Stock"] is Status.CANCELLED  # cancelled, not left running


def test_timeout_on_the_orthogonal_node_cancels_the_regions():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(TIMED, "order", validate=True)
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)

    clock[0] = 200.0  # past the node's 50s window, before either region finished
    assert runner.fire_due_timers() == 1
    final = store.load(exe.id)
    assert final.active_path == "TimedOut" and final.status is Status.DONE
    assert set(_region_status(store, exe.id).values()) == {Status.CANCELLED}


def test_clean_join_cancels_nothing():
    store = DictStore()
    defn = definition_from_dsl(CLEAN, "order", validate=True)
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)

    exe = runner.process(exe.id, Event(kind="Go"))  # both regions finish -> join all
    assert exe.active_path == "Approved"
    assert set(_region_status(store, exe.id).values()) == {Status.DONE}  # none cancelled
