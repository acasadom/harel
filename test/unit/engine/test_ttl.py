"""A machine-level `ttl`: a root execution that receives no domain event for `ttl`
seconds is expired — the model handles `Expired` straight into a terminal, or it is
forcefully ended CANCELLED with outcome `expired`."""

import pytest

from harel import engine
from harel.definition.validate import validate
from harel.dsl import DslError, definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

LISTENER = """
event Ping {}
machine M {
   ttl 60
   initial Listening
   state Listening {}
   from Listening to Listening on Ping
}
"""

HANDLED = """
event Ping {}
machine M {
   ttl 60
   initial Listening
   state Listening {}
   final Closed expired {}
   from Listening to Listening on Ping
   from Listening to Closed on Expired
}
"""

POLLER = """
machine M {
   ttl 60
   initial Polling
   state Polling { timeout 10 }
   from Polling to Polling on Timeout
}
"""

FORK = """
event Go {}
machine M {
   ttl 60
   initial Fork
   orthogonal Fork {
      state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on Go }
      state B { initial B1  state B1 {}  final B2 success {}  from B1 to B2 on Go }
   }
   final Done success {}
   from Fork to Done
}
"""


def _durable(source: str, clock):
    defn = definition_from_dsl(source, "M", validate=True)
    store = DictStore()
    return DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0]), store, defn


def _at(runner, clock, t: float) -> None:
    clock[0] = t
    runner.fire_due_timers()


# --- the DSL + validator -------------------------------------------------------------
def test_ttl_is_a_machine_level_declaration():
    assert definition_from_dsl(LISTENER, "M").ttl == 60
    with pytest.raises(DslError, match="only allowed at the machine level, not on state A"):
        definition_from_dsl("machine M {\n  initial A\n  state A { ttl 5 }\n}", "M")
    with pytest.raises(DslError, match="inline `invoke` target"):
        definition_from_dsl(
            "machine M {\n  initial A\n  state A { invoke { ttl 5  initial X  state X {} } }\n}", "M"
        )


def test_validator_rules_for_ttl_and_expired():
    codes = lambda src: {i.code for i in validate(definition_from_dsl(src, "M"))}  # noqa: E731

    non_terminal = HANDLED.replace(
        "final Closed expired {}", "state Closed {}\n   final Gone expired {}\n   from Closed to Gone on Ping"
    )
    assert "expired_target_not_terminal" in codes(non_terminal)
    assert "ttl_not_positive" in codes(LISTENER.replace("ttl 60", "ttl 0"))
    assert "expired_without_ttl" in codes(HANDLED.replace("ttl 60\n", ""))
    assert not {c for c in codes(HANDLED) if "ttl" in c or "expired" in c}


# --- expiry ---------------------------------------------------------------------------
def test_an_idle_execution_is_forcefully_expired():
    clock = [0.0]
    runner, store, _ = _durable(LISTENER, clock)
    exe = runner.create(next(iter(runner._async.definitions)))
    assert store.load(exe.id).expires_at == 60.0

    _at(runner, clock, 59.0)
    assert store.load(exe.id).status is Status.RUNNING

    _at(runner, clock, 61.0)
    expired = store.load(exe.id)
    assert expired.status is Status.CANCELLED
    assert expired.outcome == "expired"
    assert expired.finished_at == 61.0


def test_a_domain_event_restarts_the_budget():
    clock = [0.0]
    runner, store, _ = _durable(LISTENER, clock)
    exe = runner.create(next(iter(runner._async.definitions)))

    clock[0] = 30.0
    runner.process(exe.id, Event(kind="Ping"))
    assert store.load(exe.id).expires_at == 90.0

    _at(runner, clock, 61.0)
    assert store.load(exe.id).status is Status.RUNNING  # the first deadline was superseded
    _at(runner, clock, 91.0)
    assert store.load(exe.id).status is Status.CANCELLED


def test_an_unhandled_domain_event_is_still_activity():
    clock = [0.0]
    runner, store, _ = _durable(LISTENER.replace("event Ping {}", "event Ping {}\nevent Noise {}"), clock)
    exe = runner.create(next(iter(runner._async.definitions)))

    clock[0] = 50.0
    runner.process(exe.id, Event(kind="Noise"))  # no transition for it, but it was received

    _at(runner, clock, 61.0)
    assert store.load(exe.id).status is Status.RUNNING


def test_a_stale_ttl_timeout_is_ignored():
    clock = [0.0]
    runner, store, defn = _durable(LISTENER, clock)
    exe = runner.create(defn.id)
    clock[0] = 30.0
    runner.process(exe.id, Event(kind="Ping"))  # re-armed to 90

    runner.process(exe.id, engine.timeout_event(exe.id, engine.TTL_PATH, 60.0))  # the old arming

    assert store.load(exe.id).status is Status.RUNNING


def test_the_models_own_timers_are_not_activity():
    clock = [0.0]
    runner, store, _ = _durable(POLLER, clock)
    exe = runner.create(next(iter(runner._async.definitions)))

    for t in range(10, 60, 10):  # the poll timer keeps firing, but nobody talks to it
        _at(runner, clock, float(t))
    assert store.load(exe.id).status is Status.RUNNING

    _at(runner, clock, 61.0)
    assert store.load(exe.id).outcome == "expired"


def test_a_modelled_expiry_ends_in_the_models_own_terminal():
    clock = [0.0]
    runner, store, _ = _durable(HANDLED, clock)
    exe = runner.create(next(iter(runner._async.definitions)))

    _at(runner, clock, 61.0)

    done = store.load(exe.id)
    assert done.status is Status.DONE
    assert done.active_path == "Closed"
    assert done.outcome == "expired"  # the model's own verdict


def test_an_unsafe_expired_handler_is_not_taken():
    # unvalidated: `on Expired` into a state that would keep running — expire forcefully
    source = HANDLED.replace(
        "final Closed expired {}", "state Closed {}\n   final Gone expired {}\n   from Closed to Gone on Ping"
    )
    defn = definition_from_dsl(source, "M")
    clock = [0.0]
    store = DictStore()
    runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id)

    _at(runner, clock, 61.0)

    expired = store.load(exe.id)
    assert (expired.status, expired.active_path, expired.outcome) == (
        Status.CANCELLED,
        "Listening",
        "expired",
    )


# --- regions and children --------------------------------------------------------------
def test_regions_carry_no_ttl_and_a_broadcast_is_activity_for_the_root():
    clock = [0.0]
    runner, store, defn = _durable(FORK.replace("event Go {}", "event Go {}\nevent Noise {}"), clock)
    exe = runner.create(defn.id)
    children = list(store.load(exe.id).children)
    assert all(store.load(cid).expires_at is None for cid in children)

    clock[0] = 50.0
    runner.process(exe.id, Event(kind="Noise"))  # broadcast to the regions only
    assert store.load(exe.id).expires_at == 110.0

    _at(runner, clock, 61.0)
    assert store.load(exe.id).status is Status.RUNNING


def test_a_forceful_expiry_cancels_the_live_regions():
    clock = [0.0]
    runner, store, defn = _durable(FORK, clock)
    exe = runner.create(defn.id)
    children = list(store.load(exe.id).children)

    _at(runner, clock, 61.0)

    assert store.load(exe.id).outcome == "expired"
    assert all(store.load(cid).status is Status.CANCELLED for cid in children)


def test_an_invoked_machines_ttl_does_not_apply_as_a_child():
    child = definition_from_dsl(LISTENER, "M")
    parent = definition_from_dsl(
        """
        machine P {
           initial Calling
           state Calling { invoke Child }
           final Done success {}
           from Calling to Done on Returned
        }
        """,
        "P",
    )
    child_defn = type(child)(
        id="Child", root=child.root, index=child.index, events=child.events, ttl=child.ttl
    )
    clock = [0.0]
    store = DictStore()
    runner = DurableRunner(store, {parent.id: parent, "Child": child_defn}, clock=lambda: clock[0])
    exe = runner.create(parent.id)
    (cid,) = store.load(exe.id).children
    assert store.load(cid).expires_at is None

    _at(runner, clock, 1000.0)
    assert store.load(cid).status is Status.RUNNING


# --- the worker path ---------------------------------------------------------------------
def _distributed(source: str, clock):
    defn = definition_from_dsl(source, "M", validate=True)
    store = DictStore()
    transport = InMemoryTransport(clock=lambda: clock[0])
    runner = DistributedRunner(store, transport, {defn.id: defn}, clock=lambda: clock[0])
    return runner, store, defn, runner.worker(suspend_recheck=5.0)


def _drain(worker) -> None:
    while worker.step():
        pass


def test_expiry_on_the_worker_path():
    clock = [0.0]
    runner, store, defn, worker = _distributed(LISTENER, clock)
    exe = runner.create(defn.id)
    _drain(worker)

    clock[0] = 61.0
    worker.fire_due_timers()
    _drain(worker)

    assert store.load(exe.id).outcome == "expired"


def test_a_broadcast_on_the_worker_path_restarts_the_roots_budget():
    clock = [0.0]
    runner, store, defn, worker = _distributed(
        FORK.replace("event Go {}", "event Go {}\nevent Noise {}"), clock
    )
    exe = runner.create(defn.id)
    _drain(worker)

    clock[0] = 50.0
    runner.send(exe.id, Event(kind="Noise"))
    _drain(worker)
    assert store.load(exe.id).expires_at == 110.0

    clock[0] = 61.0
    worker.fire_due_timers()
    _drain(worker)
    assert store.load(exe.id).status is Status.RUNNING


def test_a_suspended_execution_expires_once_resumed():
    clock = [0.0]
    runner, store, defn, worker = _distributed(LISTENER, clock)
    exe = runner.create(defn.id)
    _drain(worker)
    runner.suspend(exe.id)

    clock[0] = 61.0
    worker.fire_due_timers()
    _drain(worker)
    assert store.load(exe.id).status is Status.SUSPENDED  # parked while paused

    runner.resume(exe.id)
    clock[0] = 70.0  # past the suspend-recheck park window
    _drain(worker)
    assert store.load(exe.id).outcome == "expired"


def test_a_suspended_execution_expires_once_resumed_in_process():
    clock = [0.0]
    runner, store, defn = _durable(LISTENER, clock)
    exe = runner.create(defn.id)
    runner.suspend(exe.id)

    _at(runner, clock, 61.0)
    assert store.load(exe.id).status is Status.SUSPENDED  # the due timer is left armed

    runner.resume(exe.id)
    _at(runner, clock, 62.0)
    assert store.load(exe.id).outcome == "expired"


def test_redrive_rearms_the_budget():
    # a dead letter ignores its ttl (abandoning it is deliberate); once redriven it is
    # RUNNING again, so its budget restarts from the redrive
    source = """
    event Ping {}
    event Go {}
    machine M {
       ttl 60
       initial Listening
       state Listening {}
       state Broken { on enter stm_actions.boom }
       from Listening to Listening on Ping
       from Listening to Broken on Go
       from Broken to Listening on Ping
    }
    """
    clock = [0.0]
    runner, store, defn = _durable(source, clock)
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.FAILED

    _at(runner, clock, 61.0)
    assert store.load(exe.id).status is Status.FAILED  # the ttl never ends a dead letter

    clock[0] = 100.0
    runner.redrive(exe.id, "Listening")
    assert store.load(exe.id).expires_at == 160.0

    _at(runner, clock, 161.0)
    assert store.load(exe.id).outcome == "expired"
