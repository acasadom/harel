"""Opaque payloads on caller-injected lifecycle events: `cancel(reason=...)`
attaches a dict to the cooperative `Cancel` event (readable by the model's
cleanup transition), and a `Start` event seeds the execution context with its
data (start-with-parameters). Neither is a context projection (those are the
engine-emitted `Finished`); these payloads come from the caller.
"""

from harel import engine
from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Execution, Status
from harel.engine.store import DictStore
from harel.spec.states import Event

# Working owns its cancellation: on Cancel it runs Releasing, whose on_enter
# captures the triggering event's data (the cancel reason).
CRITICAL = """
machine M {
  initial Working
  state Working {}
  state Releasing { on enter scenarios.capture_event }
  from Working to Releasing on Cancel
}
"""

SEED = """
machine M {
  initial A
  state A {}
  state B {}
  from A to B on Go
}
"""


def test_cooperative_cancel_carries_reason_to_the_cleanup():
    store = DictStore()
    defn = definition_from_dsl(CRITICAL, "M")
    runner = DurableRunner(store, {defn.id: defn})
    exe = runner.create(defn.id)  # parked at Working

    final = runner.cancel(exe.id, reason={"who": "ops", "code": 42})

    # the cleanup ran (Releasing) and saw the opaque cancel payload, then sank
    assert final.active_path == "Releasing"
    assert final.status is Status.DONE
    assert final.context["seen"] == {"who": "ops", "code": 42}


def test_start_event_seeds_the_context_with_its_payload():
    defn = definition_from_dsl(SEED, "M")
    exe = Execution(definition_id=defn.id)

    # a Start carrying parameters seeds the context before the machine runs
    list(engine.process(defn, exe, Event(kind="Start", data={"tenant": "acme", "n": 3})))

    assert exe.status is Status.RUNNING
    assert exe.active_path == "A"
    assert exe.context["tenant"] == "acme"
    assert exe.context["n"] == 3
