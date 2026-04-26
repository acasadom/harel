"""Durable timers: a state with `timeout` arms a timer on enter; if it is still
active when the timer is due, a `Timeout` event is delivered and the model's own
transition reacts. Leaving the state cancels the timer (statechart-native: the
engine schedules, the model decides what the timeout does).

All deterministic via an injected clock — no wall-clock sleeps.
"""

import pytest

from harel import engine
from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.store import DictStore, SqliteStore, TimerOp
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

# Trying has a timeout: stay too long and the model's own Timeout transition fires
# (-> Failed); a Success first leaves the state, cancelling the timer.
RETRY = """
machine M {
   initial Trying

   state Trying { on enter stm_actions.rec(at: "trying")  timeout 10 }
   state Ok { on enter stm_actions.rec(at: "ok") }
   state Failed { on enter stm_actions.rec(at: "failed") }

   from Trying to Ok on Success
   from Trying to Failed on Timeout
}
"""


# --- engine schedules/cancels ---------------------------------------------------
def test_entering_a_timeout_state_arms_a_timer_and_leaving_cancels_it():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])

    exe = runner.create(defn.id)  # parked at Trying -> a timer is armed for fire_at=110
    assert store.due_timers(200.0) == [(exe.id, "Trying", 110.0)]

    # leaving Trying (Success -> Ok) cancels the timer
    runner.process(exe.id, Event(kind="Success"))
    assert store.due_timers(200.0) == []
    assert store.load(exe.id).active_path == "Ok"


# --- the timeout fires the modelled transition ----------------------------------
def test_due_timer_delivers_timeout_and_the_model_reacts():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)  # at Trying, timer fires at 110

    # not due yet: nothing happens
    assert runner.fire_due_timers() == 0
    assert store.load(exe.id).active_path == "Trying"

    clock[0] = 111.0  # past the timeout
    assert runner.fire_due_timers() == 1

    final = store.load(exe.id)
    assert final.active_path == "Failed"
    assert final.status is Status.DONE  # Failed is a sink
    assert final.context["trace"] == ["trying", "failed"]
    assert store.due_timers(10_000.0) == []  # the fired timer was removed


def test_a_stale_timeout_event_is_a_noop():
    # if the timed state was already left, a late Timeout for it does nothing
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Success"))  # -> Ok, timer cancelled

    # deliver a Timeout for Trying anyway (as a duplicate/late sweep would)
    from harel import engine

    after = runner.process(exe.id, engine.timeout_event(exe.id, "Trying", 110.0))
    assert after.active_path == "Ok"  # unchanged: Trying is no longer active


def test_fire_due_timers_is_idempotent_across_a_double_sweep():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)
    clock[0] = 111.0
    assert runner.fire_due_timers() == 1
    # the timer is gone; a second sweep fires nothing and the trace is unchanged
    assert runner.fire_due_timers() == 0
    assert store.load(exe.id).context["trace"] == ["trying", "failed"]


# --- store-level timer ops ------------------------------------------------------
@pytest.mark.parametrize("backend", ["dict", "sqlite"])
def test_store_schedule_cancel_due_delete(backend, tmp_path):
    store = DictStore() if backend == "dict" else SqliteStore(tmp_path / "stm.db")
    exe = Execution(definition_id="d")
    store.save(exe)

    store.commit(exe, [], timers=(TimerOp("schedule", "A", 110.0),))
    assert store.due_timers(105.0) == []  # not due yet
    assert store.due_timers(120.0) == [(exe.id, "A", 110.0)]

    # re-schedule (upsert) to a new time, then a cancel removes it
    store.commit(exe, [], timers=(TimerOp("schedule", "A", 130.0),))
    assert store.due_timers(120.0) == []
    store.commit(exe, [], timers=(TimerOp("cancel", "A"),))
    assert store.due_timers(10_000.0) == []

    # delete_timer only removes the row that still holds fire_at
    store.commit(exe, [], timers=(TimerOp("schedule", "A", 200.0),))
    store.delete_timer(exe.id, "A", 199.0)  # stale fire_at: no-op
    assert store.due_timers(10_000.0) == [(exe.id, "A", 200.0)]
    store.delete_timer(exe.id, "A", 200.0)
    assert store.due_timers(10_000.0) == []

    if backend == "sqlite":
        store.close()


# --- distributed: a worker sweeps the timer and the machine transitions ---------
def test_worker_sweeps_due_timers_and_machine_times_out():
    clock = [100.0]
    store = DictStore()
    transport = InMemoryTransport(clock=lambda: clock[0])
    defn = definition_from_dsl(RETRY, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn}, clock=lambda: clock[0])
    worker = runner.worker(clock=lambda: clock[0])
    exe = runner.create(defn.id)  # at Trying, timer fires at 110

    # nothing due, queue empty: a step does nothing
    assert worker.step() is False
    assert worker.fire_due_timers() == 0

    clock[0] = 111.0
    assert worker.fire_due_timers() == 1  # publishes the Timeout to the group
    assert worker.step() is True  # the worker delivers it

    final = store.load(exe.id)
    assert final.active_path == "Failed"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["trying", "failed"]


# --- a Timeout fires the transition of the PATH that timed out (not innermost) --
NESTED = """
machine M {
   initial Outer

   state Outer {
      timeout 100
      initial A

      state A { on enter stm_actions.rec(at: "A")  timeout 10 }
      state B { on enter stm_actions.rec(at: "B") }

      from A to B on Timeout
   }
   final Failed failed { on enter stm_actions.rec(at: "failed") }

   from Outer to Failed on Timeout
}
"""


def test_timeout_resolves_by_path_not_innermost():
    # while in A (which has its OWN Timeout transition -> B), the OUTER composite's
    # budget Timeout must fire Outer's exit (-> Failed), not A's inner transition.
    store = DictStore()
    defn = definition_from_dsl(NESTED, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at Outer.A
    assert exe.active_path == "Outer.A"

    # the A timer (path=Outer.A) fires A's own transition -> B (not Outer -> Failed)
    eid = exe.id
    after_a = runner.process(eid, engine.timeout_event(eid, "Outer.A", 0.0))
    assert "B" in after_a.context["trace"]
    assert after_a.outcome is None  # did NOT take Outer's -> Failed

    # reset and instead fire the OUTER timer (path=Outer) while in A -> Failed (the fix)
    store2 = DictStore()
    runner2 = DurableRunner(store2, {defn.id: defn})
    exe2 = runner2.create(defn.id)
    after_outer = runner2.process(exe2.id, engine.timeout_event(exe2.id, "Outer", 0.0))
    assert after_outer.active_path == "Failed"
    assert after_outer.outcome == "failed"


# --- full retry composite: selector + exponential backoff + budget + outcome ----
RETRY_COMPOSITE = """
machine M {
   initial Retrying

   state Retrying {
      timeout 100
      initial Send

      state Send { on enter stm_actions.rec(at: "send") }
      state Waiting {
         on enter harel.lib.exponential_backoff(base: 5, factor: 2, into: "backoff")
         timeout context backoff
      }
      state Succeeded { on enter stm_actions.rec(at: "ok") }

      from Send select stm_actions.sel {
         "ok"    to Succeeded
         "retry" to Waiting
      }
      from Waiting to Send on Timeout
   }
   state Done { on enter stm_actions.rec(at: "done") }
   final Failed failed { on enter stm_actions.rec(at: "failed") }

   from Retrying to Done                  # completion: inner Succeeded sink -> Done
   from Retrying to Failed on Timeout      # budget exhausted -> Failed
}
"""


def test_retry_composite_backs_off_then_succeeds():
    clock = [1000.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY_COMPOSITE, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    # the selector fails twice (retry) then succeeds (ok)
    exe = runner.create(defn.id, context={"picks": ["retry", "retry", "ok"]})
    assert exe.active_path == "Retrying.Waiting"  # parked on the 1st backoff
    assert exe.context["backoff"] == 5.0  # base * 2**0
    # both timers are armed: the composite budget (Retrying@1100) and the backoff
    due = store.due_timers(10_000.0)
    assert (exe.id, "Retrying.Waiting", 1005.0) in due
    assert (exe.id, "Retrying", 1100.0) in due

    clock[0] = 1005.1  # 1st backoff elapsed
    runner.fire_due_timers()
    assert store.load(exe.id).active_path == "Retrying.Waiting"  # retried -> Send -> retry -> Waiting
    assert store.load(exe.id).context["backoff"] == 10.0  # base * 2**1 (exponential)

    clock[0] = 1015.2  # 2nd backoff elapsed
    runner.fire_due_timers()

    final = store.load(exe.id)
    assert final.status is Status.DONE
    assert final.outcome is None  # plain success, not the budget terminal
    assert "ok" in final.context["trace"] and "done" in final.context["trace"]
    assert "failed" not in final.context["trace"]


def test_retry_composite_budget_exhaustion_fails():
    clock = [1000.0]
    store = DictStore()
    defn = definition_from_dsl(RETRY_COMPOSITE, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id, context={"picks": ["retry"] * 50})  # never succeeds

    clock[0] = 1101.0  # past the composite's 100s budget
    # fire repeatedly: the Retrying budget Timeout must win -> Failed
    for _ in range(5):
        if runner.fire_due_timers() == 0:
            break

    final = store.load(exe.id)
    assert final.active_path == "Failed"
    assert final.outcome == "failed"


# An inner state's timeout, with no handler of its own, bubbles up to the
# enclosing state's `on Timeout` — the parent scope owns the reaction.
NESTED_TIMEOUT = """
machine M {
  initial C
  state C {
    initial Inner
    state Inner { timeout 10 }
    state Other {}
    from Inner to Other on Step
  }
  final Failed failed
  from C to Failed on Timeout
}
"""


def test_inner_timeout_bubbles_up_to_an_enclosing_handler():
    clock = [100.0]
    store = DictStore()
    defn = definition_from_dsl(NESTED_TIMEOUT, "M")
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])

    exe = runner.create(defn.id)
    assert store.load(exe.id).active_path == "C.Inner"  # parked inside the composite

    clock[0] = 200.0
    runner.fire_due_timers()  # Inner has no own Timeout transition -> bubbles to C
    final = store.load(exe.id)
    assert final.active_path == "Failed"  # caught by `from C to Failed on Timeout`
    assert final.status is Status.DONE
