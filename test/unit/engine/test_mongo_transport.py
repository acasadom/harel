"""MongoTransport unit tests, backed by mongomock (no server, no Docker).

Same single-active-consumer-per-group contract as the other backends. MongoDB has
no native message groups, so — like RedisTransport — the per-group lock is built
by hand (a per-group lock document acquired via an atomic upsert whose collision
means "held elsewhere"; the lock's expiry is the lease).

NOTE: there is no threaded concurrency test here as there is for fakeredis —
mongomock executes pure-Python and is not faithfully atomic under preemption, so a
threaded test would be flaky against the mock rather than against the design. The
hand-rolled group lock's behaviour under genuine contention is covered by the real
MongoDB stack test in test/integration/test_mongo_store.py.
"""

import time

import pytest

mongomock = pytest.importorskip("mongomock")

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.distributed import DistributedRunner  # noqa: E402
from harel.engine.execution import Status  # noqa: E402
from harel.engine.store import DictStore  # noqa: E402
from harel.engine.transport import MongoTransport  # noqa: E402
from harel.spec.states import Event  # noqa: E402


def _h(label: str) -> str:
    return f'stm_actions.rec(at: "{label}")'


FLAT = f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")} }}
  state C {{ on enter {_h("C.enter")} }}
  from A to B
  from B to C on Go
}}
"""

ORTHO = f"""
machine M {{
  initial Fork
  orthogonal Fork {{
    state A {{
      initial A1
      state A1 {{ on enter {_h("A1")} }}
      state A2 {{ on enter {_h("A2")} }}
      from A1 to A2 on Go
    }}
    state B {{
      initial B1
      state B1 {{ on enter {_h("B1")} }}
      state B2 {{ on enter {_h("B2")} }}
      from B1 to B2 on Go
    }}
  }}
  state Done {{ on enter {_h("Done")} }}
  from Fork to Done
}}
"""


def _event(kind: str) -> Event:
    return Event(kind=kind)


@pytest.fixture
def client():
    return mongomock.MongoClient()


@pytest.fixture
def transport(client):
    return MongoTransport(client)


def test_fifo_within_a_group(transport):
    transport.publish("G", _event("e1"))
    transport.publish("G", _event("e2"))

    first = transport.claim("w", visibility=30)
    assert first.event.kind == "e1"
    transport.ack(first)
    second = transport.claim("w", visibility=30)
    assert second.event.kind == "e2"


def test_one_in_flight_per_group_but_other_groups_proceed(transport):
    transport.publish("G", _event("g1"))
    transport.publish("G", _event("g2"))
    transport.publish("H", _event("h1"))

    a = transport.claim("w1", visibility=30)
    b = transport.claim("w2", visibility=30)
    # two different groups are claimable concurrently; neither group is claimed twice
    assert {a.group_id, b.group_id} == {"G", "H"}

    # both groups now in-flight -> nothing else (G's g2 is blocked behind g1)
    assert transport.claim("w3", visibility=30) is None

    # release G's lease; its second message becomes claimable, in FIFO order
    g_lease = a if a.group_id == "G" else b
    transport.ack(g_lease)
    nxt = transport.claim("w3", visibility=30)
    assert nxt.group_id == "G" and nxt.event.kind == "g2"


def test_many_ready_groups_all_get_claimed(transport):
    # claim() leases the lowest-`available_at` group in one sorted find_one_and_update;
    # across successive claims it walks through every ready group — none is starved.
    n = 13
    for i in range(n):
        transport.publish(f"G{i}", _event(f"e{i}"))
    claimed = set()
    while (lease := transport.claim("w", visibility=30)) is not None:
        claimed.add(lease.group_id)
        transport.ack(lease)
    assert len(claimed) == n


def test_round_robin_fairness(client):
    """After processing group A, group B (available_at=0.0) must be preferred over
    A (available_at=recent_epoch) on the next claim."""
    clock = [0.0]
    t = MongoTransport(client, clock=lambda: clock[0])

    for i in range(5):
        t.publish("A", _event(f"a{i}"))

    clock[0] = 1.0
    lease_a = t.claim("w", visibility=30)
    assert lease_a is not None and lease_a.group_id == "A"

    clock[0] = 2.0
    t.ack(lease_a)  # A's available_at is now 2.0

    # B is fresh (available_at=0.0 < A's 2.0) — claim must prefer B
    t.publish("B", _event("b0"))

    clock[0] = 3.0
    lease_b = t.claim("w", visibility=30)
    assert lease_b is not None and lease_b.group_id == "B"


def test_min_priority_filters_low_priority_groups(client):
    """claim(min_priority=N) skips groups whose priority < N; fallback to 0 picks them."""
    t = MongoTransport(client)

    t.publish("lo", _event("e1"), priority=0)
    t.publish("hi", _event("e2"), priority=2)

    lease = t.claim("w", visibility=30, min_priority=2)
    assert lease is not None and lease.group_id == "hi"
    t.ack(lease)

    assert t.claim("w", visibility=30, min_priority=2) is None

    lo = t.claim("w", visibility=30)
    assert lo is not None and lo.group_id == "lo"


def test_priority_reset_on_drain(client):
    """When a group drains, its lock document is deleted so a re-publish can set
    a new (higher) priority.  Without the delete, the stale priority=0 would
    persist and claim(min_priority=2) would return None."""
    t = MongoTransport(client)

    t.publish("G", _event("e1"), priority=0)
    lease = t.claim("w", visibility=30)
    assert lease is not None
    t.ack(lease)  # group drains → lock document deleted

    t.publish("G", _event("e2"), priority=2)

    hi = t.claim("w", visibility=30, min_priority=2)
    assert hi is not None and hi.group_id == "G"


def test_ack_removes_the_message(transport):
    transport.publish("G", _event("only"))
    transport.ack(transport.claim("w", visibility=30))
    assert transport.claim("w", visibility=30) is None


def test_nack_returns_the_message_immediately(transport):
    transport.publish("G", _event("e1"))
    transport.nack(transport.claim("w", visibility=30))
    again = transport.claim("w", visibility=30)
    assert again.event.kind == "e1"


def test_nack_with_delay_parks_the_message(transport):
    transport.publish("G", _event("e1"))
    transport.nack(transport.claim("w", visibility=30), delay=0.2)
    assert transport.claim("w", visibility=30) is None  # parked: group stays blocked
    time.sleep(0.3)
    again = transport.claim("w", visibility=30)
    assert again is not None and again.event.kind == "e1"


def test_a_held_lease_blocks_other_claims(transport):
    transport.publish("G", _event("e1"))
    held = transport.claim("w1", visibility=30)
    assert held.event.kind == "e1"
    assert transport.claim("w2", visibility=30) is None  # G is leased


def test_ack_by_a_stale_owner_is_a_noop(transport):
    # a lease whose lock expired and was taken by someone else must not remove the
    # head (fencing): the new owner keeps the message.
    transport.publish("G", _event("e1"))
    stale = transport.claim("w1", visibility=0.05)
    time.sleep(0.25)  # w1's lock expires (well past the 50ms lease)
    fresh = transport.claim("w2", visibility=30)  # w2 grabs the same head
    assert fresh is not None and fresh.event.kind == "e1"
    transport.ack(stale)  # stale w1 must not pop w2's message
    assert transport.claim("w3", visibility=30) is None  # G is held by w2, message intact


def test_lease_expiry_makes_a_message_claimable_again(transport):
    transport.publish("G", _event("e1"))
    assert transport.claim("w1", visibility=0.05).event.kind == "e1"
    time.sleep(0.25)  # lease (50ms) elapsed with a generous margin
    recovered = transport.claim("w2", visibility=30)
    assert recovered is not None and recovered.event.kind == "e1"


# --- the full DistributedRunner + Worker pipeline over MongoTransport (mongomock) -----------


def test_pipeline_flat_over_mongo(client):
    defn = definition_from_dsl(FLAT, "M")
    store = DictStore()
    runner = DistributedRunner(store, MongoTransport(client), {defn.id: defn})

    exe = runner.create(defn.id)
    assert exe.active_path == "B"
    runner.send(exe.id, _event("Go"))
    w = runner.worker()
    while w.step():
        pass

    final = store.load(exe.id)
    assert final.active_path == "C"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


def test_pipeline_orthogonal_over_mongo(client):
    defn = definition_from_dsl(ORTHO, "M")
    store = DictStore()
    runner = DistributedRunner(store, MongoTransport(client), {defn.id: defn})

    exe = runner.create(defn.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    runner.send(exe.id, _event("Go"))
    w = runner.worker()
    while w.step():
        pass

    final = store.load(exe.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["Done"]
    regions = [store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
