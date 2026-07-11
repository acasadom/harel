"""Execution priority propagates to every FIRST publish of a group.

Priority is stored on the transport group on its first publish and fixed there.
Three paths used to publish the first event of a group at priority 0, pinning a
high-priority execution to normal priority (only visible under a worker with
high_ratio>0, but that is the supported way to use priority):

  #4 the worker's due-timer sweep (a machine parked on `timeout:` — the Timeout is
     the first publish to its own group),
  #5 a cross-execution emit (a region's Finished -> the parent's group),
  #6 spawned children (orthogonal regions / invoke / fan-out) never inherited it.
"""

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.store import DictStore
from harel.engine.transport import InMemoryTransport

PRIORITY = 3

# regions park (just need them spawned to inspect their inherited priority).
ORTHO = """
machine M {
  initial Fork
  orthogonal Fork {
    state A { initial A1  state A1 {} }
    state B { initial B1  state B1 {} }
  }
  final Done success
  from Fork to Done
}
"""

# regions finish on their OWN timer, so each region's Finished -> parent is published
# by the REGION's relay cycle (the parent is NOT the primary there) — the path where
# the parent group's first publish would default to priority 0.
ORTHO_TIMED = """
machine M {
  initial Fork
  orthogonal Fork {
    state A { initial A1  state A1 { timeout 5 }  final A2 success {}  from A1 to A2 on Timeout }
    state B { initial B1  state B1 { timeout 5 }  final B2 success {}  from B1 to B2 on Timeout }
  }
  final Done success
  from Fork to Done
}
"""

# parks on a timeout state: nothing is published to its own group until the timer fires.
TIMED = """
machine T {
  initial Wait
  state Wait { timeout 10 }
  final Done success
  from Wait to Done on Timeout
}
"""


def test_spawned_regions_inherit_parent_priority():  # #6
    store, transport = DictStore(), InMemoryTransport()
    defn = definition_from_dsl(ORTHO, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn})
    exe = runner.create(defn.id, priority=PRIORITY)

    parent = store.load(exe.id)
    assert parent.children  # the regions were spawned
    for cid in parent.children:
        assert store.load(cid).priority == PRIORITY  # was 0 before the fix


def test_region_finished_published_at_parent_priority():  # #5
    clock = [100.0]
    store, transport = DictStore(), InMemoryTransport(clock=lambda: clock[0])
    defn = definition_from_dsl(ORTHO_TIMED, "M")
    runner = DistributedRunner(store, transport, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id, priority=PRIORITY)
    assert exe.id not in transport._groups  # parent's own group has no publish yet

    clock[0] = 200.0  # past the regions' timeouts
    worker = runner.worker(clock=lambda: clock[0])
    worker.fire_due_timers()  # Timeout -> each region's group (not the parent's)

    # step until the parent's group first appears: it is first published by a region's
    # Finished (a cross-execution emit), which must carry the PARENT's priority, not 0.
    captured = None
    for _ in range(20):
        if exe.id in transport._groups:
            captured = transport._groups[exe.id]["priority"]
            break
        if not worker.step():
            break
    assert captured == PRIORITY  # was 0 before the fix


def test_due_timer_published_at_execution_priority():  # #4
    clock = [100.0]
    store, transport = DictStore(), InMemoryTransport(clock=lambda: clock[0])
    defn = definition_from_dsl(TIMED, "T")
    runner = DistributedRunner(store, transport, {defn.id: defn}, clock=lambda: clock[0])
    exe = runner.create(defn.id, priority=PRIORITY)

    assert exe.id not in transport._groups  # nothing published to its own group yet
    clock[0] = 200.0  # past the timeout
    assert runner.worker(clock=lambda: clock[0]).fire_due_timers() == 1
    # the Timeout is the first publish to the group -> it sets the group priority
    assert transport._groups[exe.id]["priority"] == PRIORITY  # was 0 before the fix
