"""A worker given `machines` claims only the work of the executions started from them — their
whole tree: regions, fan-out instances and invoked children travel with their root. A worker
given none claims everything, the dedicated ones' machines included (dedicated capacity, not
exclusivity)."""

import pytest

from harel import DictStore, Event, definition_from_dsl
from harel.config import Config
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.transport import InMemoryTransport

ORDER = """
event Go {}
machine order {
  initial Idle
  state Idle {}
  state Run { invoke { initial Wait state Wait {} final End done from Wait to End on Go } }
  orthogonal Checks {
    state A { initial A1 state A1 {} final A2 done from A1 to A2 on Go }
    state B { initial B1 state B1 {} final B2 done from B1 to B2 on Go }
  }
  final Done success
  from Idle to Run on Go
  from Run to Checks on Returned
  from Checks to Done
}
"""
OTHER = """
event Go {}
machine other { initial A state A {} final B done from A to B on Go }
"""


class _Recording(InMemoryTransport):
    """Records the routing key every publish carries."""

    def __init__(self):
        super().__init__()
        self.keys: list = []

    def publish(self, group_id, event, priority=0, routing_key=None):
        self.keys.append(routing_key)
        super().publish(group_id, event, priority, routing_key=routing_key)


def _runner(transport=None):
    defs = {d.id: d for d in (definition_from_dsl(ORDER, "order"), definition_from_dsl(OTHER, "other"))}
    store = DictStore()
    return DistributedRunner(store, transport or InMemoryTransport(), defs, execution="inline"), store


def _drain(*workers):
    while any(w.step() for w in workers):
        pass


def _go(runner, exe_id):
    runner.send(exe_id, Event(kind="Go"))


def test_a_dedicated_worker_runs_its_machines_whole_tree_and_nothing_else():
    runner, store = _runner()
    dedicated = runner.worker("orders", machines={"order"})
    order, other = runner.create("order"), runner.create("other")
    _drain(dedicated)
    _go(runner, order.id)
    _go(runner, other.id)
    _drain(dedicated)  # Run: the invoked child starts — on this worker
    [child_id] = store.load(order.id).children
    _go(runner, child_id)
    _drain(dedicated)  # the child returns; Checks forks its two regions
    for region in store.load(order.id).children:
        if not store.load(order.id).children[region].finished:
            _go(runner, region)
    _drain(dedicated)
    assert store.load(order.id).status is Status.DONE
    assert store.load(other.id).status is Status.PENDING  # never claimed by this worker

    _drain(runner.worker("anyone"))  # an unfiltered worker takes what's left: its Start and Go
    assert store.load(other.id).status is Status.DONE


def test_an_unfiltered_worker_also_runs_the_dedicated_machines():
    runner, store = _runner()
    order = runner.create("order")
    _drain(runner.worker("anyone"))
    assert store.load(order.id).status is Status.RUNNING


def test_every_execution_of_a_tree_carries_its_roots_key_and_every_publish_its_targets():
    transport = _Recording()
    runner, store = _runner(transport)
    worker = runner.worker()
    order = runner.create("order")
    _drain(worker)
    _go(runner, order.id)
    _drain(worker)
    [child_id] = store.load(order.id).children
    _go(runner, child_id)
    _drain(worker)
    tree = [
        store.load(order.id),
        *(store.load(c) for c in store.load(order.id).children),
        store.load(child_id),
    ]
    assert {e.routing_key for e in tree} == {"order"}
    assert set(transport.keys) == {"order"}  # the child's Returned, the regions' Go: all "order"


def test_a_worker_given_no_machine_is_refused():
    runner, _ = _runner()
    with pytest.raises(ValueError, match="names no machine"):
        runner.worker(machines=set())


def test_a_dedicated_worker_on_a_transport_that_cant_route_fails_loudly():
    fakeredis = pytest.importorskip("fakeredis")
    from harel.engine.transport import RedisTransport

    runner, _ = _runner(RedisTransport(fakeredis.FakeStrictRedis()))
    runner.create("order")
    with pytest.raises(NotImplementedError, match="can't restrict a claim"):
        runner.worker(machines={"order"}).step()


def test_the_environment_names_the_machines():
    assert Config.from_env({"HAREL_MACHINES": " order, payment ,"}).machines == {"order", "payment"}
    assert Config.from_env({}).machines is None
    assert Config.from_env({"HAREL_MACHINES": " "}).machines is None
