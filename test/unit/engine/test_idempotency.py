"""Idempotency key exposure + the opt-in `idempotent` helper (the `B` approach).

The driver sets a stable `stm.idempotency_key = {execution_id}:{step}:{index}:{action}`
before each action — `step` the id of the event being processed (`start` when the execution
starts). Every attempt at the same event computes the same keys, even when another writer
moved the execution on in between: the hook a side effect uses to dedupe against an external
backend.
"""

from harel import (
    DictIdempotency,
    DurableRunner,
    Event,
    definition_from_dsl,
    idempotent,
)
from harel.engine.store import DictStore

SRC = """
machine M {
   initial A
   state A { on enter capture }
   state B { on enter capture }
   from A to B on Go
}
"""


def capture(stm, event, **inputs):
    """Record the idempotency key the driver assigned to this action call."""
    stm.execution_ctx.setdefault("keys", []).append(stm.idempotency_key)


def _runner():
    defn = definition_from_dsl(SRC, "M", actions={"capture": capture})
    return DurableRunner(DictStore(), {defn.id: defn}), defn


# --- key exposure / format -------------------------------------------------------------------


def test_key_exposed_and_formatted():
    runner, defn = _runner()
    exe = runner.create(defn.id)  # enters A
    assert exe.context["keys"] == [f"{exe.id}:start:0:capture"]
    exe = runner.process(exe.id, Event(kind="Go", id="go-1"))  # enters B
    assert exe.context["keys"] == [f"{exe.id}:start:0:capture", f"{exe.id}:go-1:0:capture"]


def test_index_is_deterministic_across_executions():
    # two independent runs differ only in the (random) execution id; the
    # `:step:index:action` part is identical — i.e. replay-stable per action
    a, defn = _runner()
    exe_a = a.create(defn.id)
    b, _ = _runner()
    exe_b = b.create(defn.id)
    suffix = lambda e: [k.split(":", 1)[1] for k in e.context["keys"]]  # noqa: E731
    assert suffix(exe_a) == suffix(exe_b) == ["start:0:capture"]


# --- DictIdempotency / the idempotent() helper -----------------------------------------------


class _Stm:
    def __init__(self, key):
        self.idempotency_key = key
        self.execution_ctx = {}


def test_dict_idempotency_runs_once_and_caches():
    backend = DictIdempotency()
    calls = []
    assert backend.run_once("k", lambda: (calls.append(1), "first")[1]) == "first"
    assert backend.run_once("k", lambda: (calls.append(1), "second")[1]) == "first"  # cached
    assert len(calls) == 1
    assert backend.run_once("other", lambda: "x") == "x"  # different key runs


def test_idempotent_dedupes_per_key():
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def charge(stm, event, **inputs):
        runs.append(stm.idempotency_key)
        return f"charged:{stm.idempotency_key}"

    ev = Event(kind="E")
    # same key (an at-least-once redelivery) -> body runs once, cached result returned
    assert charge(_Stm("e:1:0"), ev) == "charged:e:1:0"
    assert charge(_Stm("e:1:0"), ev) == "charged:e:1:0"
    assert runs == ["e:1:0"]
    # a different key (a different action/event) runs again
    assert charge(_Stm("e:2:0"), ev) == "charged:e:2:0"
    assert runs == ["e:1:0", "e:2:0"]


def test_idempotent_without_key_always_runs():
    # a non-durable run (no idempotency_key) just runs the body every time
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def act(stm, event, **inputs):
        runs.append(1)
        return "ok"

    nokey = _Stm(None)
    assert act(nokey, Event(kind="E")) == "ok"
    assert act(nokey, Event(kind="E")) == "ok"
    assert len(runs) == 2


def test_idempotent_action_in_a_real_run():
    # bind an idempotent-wrapped action and drive it through DurableRunner: the
    # body runs once per (state-entry) key, deduped through the backend
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def capture_once(stm, event, **inputs):
        runs.append(stm.idempotency_key)

    defn = definition_from_dsl(SRC, "M", actions={"capture": capture_once})
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go", id="go-1"))
    assert [k.rsplit(":", 1)[0] for k in runs] == [f"{exe.id}:start:0", f"{exe.id}:go-1:0"]  # one run per key


# --- a retry after another writer: the same keys ----------------------------------------------
def test_a_retry_after_another_writer_hands_the_actions_the_same_keys(tmp_path):
    """A worker's commit can lose to another writer with nothing crashing — here a `suspend`
    lands while the step's action runs. The event is redelivered, and the action runs again on
    an execution whose version moved on; its key is the same, so a side effect deduped on it
    happens once."""
    from harel.engine import control
    from harel.engine.distributed import DistributedRunner
    from harel.engine.store import SqliteStore
    from harel.engine.transport import SqliteTransport

    src = """
event Pay {}
machine M {
   initial Unpaid
   state Unpaid {}
   state Paid { on enter charge }
   from Unpaid to Paid on Pay
}
"""
    other_writer = SqliteStore(tmp_path / "s.db")
    keys: list[str] = []
    charged: list[str] = []
    backend = DictIdempotency()

    @idempotent(backend)
    def charge(stm, event, **inputs):
        charged.append(stm.idempotency_key)

    def charge_while_suspended(stm, event, **inputs):
        keys.append(stm.idempotency_key)
        charge(stm, event)
        if len(keys) == 1:  # the first attempt: someone suspends the order meanwhile
            control.suspend(other_writer, exe.id)

    defn = definition_from_dsl(src, "M", actions={"charge": charge_while_suspended})
    store = SqliteStore(tmp_path / "s.db")
    runner = DistributedRunner(store, SqliteTransport(tmp_path / "q.db"), {defn.id: defn}, execution="inline")
    worker = runner.worker(suspend_recheck=0.0)
    exe = runner.create(defn.id)
    while worker.step():
        if store.load(exe.id).active_path is not None:
            break
    runner.send(exe.id, Event(kind="Pay", id="pay-1"))

    worker.step()  # runs charge, loses the commit to the suspend, puts Pay back
    assert store.load(exe.id).active_path == "Unpaid"
    runner.resume(exe.id)
    worker.step()  # Pay again, on the newer version

    assert store.load(exe.id).active_path == "Paid"
    assert len(keys) == 2 and keys[0] == keys[1] == f"{exe.id}:pay-1:0:charge_while_suspended"
    assert charged == [keys[0]]  # the side effect, once
