"""RedisTransport unit tests, backed by fakeredis (no server, no Docker).

Same single-active-consumer-per-group contract as the other backends, but Redis
has no native message groups so the lock is built by hand (`SET NX PX` + a list
per group + the lock TTL as the lease). fakeredis runs it in-process; a shared
`FakeServer` lets several clients (and threads) see the same state, so the
concurrency test exercises the hand-rolled group lock under real contention.
"""

import threading
import time
from unittest import mock

import pytest

fakeredis = pytest.importorskip("fakeredis")

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.distributed import DistributedRunner  # noqa: E402
from harel.engine.execution import Status  # noqa: E402
from harel.engine.store import DictStore  # noqa: E402
from harel.engine.transport import Lease, RedisTransport  # noqa: E402
from harel.spec.states import Event  # noqa: E402


def _h(label: str) -> str:
    """DSL hook fragment referencing `rec` with its label."""
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
def server():
    return fakeredis.FakeServer()


@pytest.fixture
def transport(server):
    return RedisTransport(fakeredis.FakeStrictRedis(server=server))


def test_fifo_within_a_group(transport):
    transport.publish("G", _event("e1"))
    transport.publish("G", _event("e2"))

    first = transport.claim("w", visibility=30)
    assert first.event.kind == "e1"
    transport.ack(first)
    second = transport.claim("w", visibility=30)
    assert second.event.kind == "e2"


def test_one_in_flight_per_group_but_other_groups_proceed(transport):
    # NOTE: Redis gives FIFO *within* a group, not across groups (claim iterates an
    # unordered set of groups), so this test must not assume G-before-H.
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


def test_ack_removes_the_message(transport):
    transport.publish("G", _event("only"))
    transport.ack(transport.claim("w", visibility=30))
    assert transport.claim("w", visibility=30) is None


def test_nack_returns_the_message_immediately(transport):
    transport.publish("G", _event("e1"))
    transport.nack(transport.claim("w", visibility=30))
    again = transport.claim("w", visibility=30)
    assert again.event.kind == "e1"


def test_a_held_lease_blocks_other_claims(transport):
    # a long lease so this never races the lock TTL (the "still leased" check)
    transport.publish("G", _event("e1"))
    held = transport.claim("w1", visibility=30)
    assert held.event.kind == "e1"
    assert transport.claim("w2", visibility=30) is None  # G is leased


def test_ack_by_a_stale_owner_is_a_noop(transport):
    # a lease whose lock expired and was taken by someone else must not remove the
    # head (fencing): the new owner keeps the message. Short TTL + a generous sleep
    # (>> TTL) so expiry is reliable even under load.
    transport.publish("G", _event("e1"))
    stale = transport.claim("w1", visibility=0.05)
    time.sleep(0.25)  # w1's lock expires (well past the 50ms TTL)
    fresh = transport.claim("w2", visibility=30)  # w2 grabs the same head
    assert fresh is not None and fresh.event.kind == "e1"
    transport.ack(stale)  # stale w1 must not pop w2's message
    assert transport.claim("w3", visibility=30) is None  # G is held by w2, message intact


def test_lease_expiry_makes_a_message_claimable_again(transport):
    transport.publish("G", _event("e1"))
    assert transport.claim("w1", visibility=0.05).event.kind == "e1"
    time.sleep(0.25)  # lock TTL (50ms) elapsed with a generous margin
    recovered = transport.claim("w2", visibility=30)
    assert recovered is not None and recovered.event.kind == "e1"


def test_concurrency_preserves_group_exclusivity(server):
    groups, per_group, n_workers = ["G", "H", "I", "J"], 5, 4
    pub = RedisTransport(fakeredis.FakeStrictRedis(server=server))
    expected = []
    for g in groups:
        for i in range(per_group):
            pub.publish(g, _event(f"{g}-{i}"))
            expected.append(f"{g}-{i}")

    active: set[str] = set()
    active_lock = threading.Lock()
    processed: list[str] = []
    violations: list[str] = []
    errors: list[str] = []
    done = threading.Event()

    def worker(wid: str):
        t = RedisTransport(fakeredis.FakeStrictRedis(server=server))  # own client, shared server
        try:
            while not done.is_set():
                lease: Lease | None = t.claim(wid, visibility=30)
                if lease is None:
                    time.sleep(0.001)
                    continue
                with active_lock:
                    if lease.group_id in active:
                        violations.append(lease.group_id)
                    active.add(lease.group_id)
                time.sleep(0.002)
                with active_lock:
                    active.discard(lease.group_id)
                    processed.append(lease.event.kind)
                t.ack(lease)
        except Exception as exc:
            errors.append(f"{wid}: {exc!r}")

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(n_workers)]
    for th in threads:
        th.start()
    deadline = time.time() + 10
    while len(processed) < len(expected) and not errors and time.time() < deadline:
        time.sleep(0.005)
    done.set()
    for th in threads:
        th.join(timeout=5)

    assert errors == []
    assert violations == []  # never two workers in the same group at once
    assert sorted(processed) == sorted(expected)  # every message processed exactly once


# --- the full DistributedRunner + Worker pipeline over RedisTransport (fakeredis) -----------
# Single worker draining deterministically: proves the worker pipeline works over Redis without
# needing Docker. The genuinely multi-process variant (real Redis) lives in test/integration.


def _redis_transport(server):
    return RedisTransport(fakeredis.FakeStrictRedis(server=server))


def test_pipeline_flat_over_redis(server):
    defn = definition_from_dsl(FLAT, "M")
    store = DictStore()
    runner = DistributedRunner(store, _redis_transport(server), {defn.id: defn})

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


def test_pipeline_orthogonal_over_redis(server):
    defn = definition_from_dsl(ORTHO, "M")
    store = DictStore()
    runner = DistributedRunner(store, _redis_transport(server), {defn.id: defn})

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


# --- regression: claim must NOT scan every pending group (the O(N) SMEMBERS bug) ----


def test_claim_does_not_scan_all_groups(transport):
    # a large backlog of distinct pending groups
    for i in range(1000):
        transport.publish(f"G{i}", _event(f"e{i}"))

    with (
        mock.patch.object(transport._r, "smembers", wraps=transport._r.smembers) as smembers,
        mock.patch.object(transport, "_claim_script", wraps=transport._claim_script) as claim_script,
    ):
        lease = transport.claim("w", visibility=30)

    assert lease is not None  # leased one of the 1000 groups
    # the old O(N) path materialised every group via SMEMBERS — it must be gone
    assert smembers.call_count == 0
    # the bounded candidate window is applied inside the atomic Lua claim — a single call,
    # independent of the 1000 pending groups, with the limit threaded through as the last arg
    assert claim_script.call_count == 1
    assert RedisTransport._CANDIDATES in claim_script.call_args.kwargs["args"]
    # exactly one group was leased (its score bumped into the future); the rest stay at 0
    now_ms = int(time.time() * 1000)
    assert transport._r.zcount(transport._k_ready(0), now_ms + 1, "+inf") == 1


def test_round_robin_fairness(server):
    """After processing group A's first message, group B (never claimed, score=0)
    must be preferred over group A (score=now_ms > 0) on the next claim."""
    clock = [1000.0]
    t = RedisTransport(fakeredis.FakeStrictRedis(server=server), clock=lambda: clock[0])

    for i in range(5):
        t.publish("A", _event(f"a{i}"))
    t.publish("B", _event("b0"))

    clock[0] = 1001.0
    lease_a = t.claim("w", visibility=30)
    assert lease_a is not None and lease_a.group_id == "A"

    clock[0] = 1002.0
    t.ack(lease_a)  # A's score is now 1002000 ms; B's score stays at 0

    clock[0] = 1003.0
    lease_b = t.claim("w", visibility=30)
    assert lease_b is not None and lease_b.group_id == "B"


def test_min_priority_filters_low_priority_groups(server):
    """claim(min_priority=N) skips groups whose priority < N; fallback to 0 picks them."""
    t = RedisTransport(fakeredis.FakeStrictRedis(server=server))

    t.publish("lo", _event("e1"), priority=0)
    t.publish("hi", _event("e2"), priority=2)

    lease = t.claim("w", visibility=30, min_priority=2)
    assert lease is not None and lease.group_id == "hi"
    t.ack(lease)

    # low-priority group is invisible at min_priority=2
    assert t.claim("w", visibility=30, min_priority=2) is None

    lo = t.claim("w", visibility=30)
    assert lo is not None and lo.group_id == "lo"


def test_prio_hash_cleaned_up_on_drain(server):
    """After a group is fully drained, HDEL removes its prio hash entry so a
    re-publish can set a new (higher) priority via HSETNX.  Without the HDEL,
    HSETNX is a no-op and the stale priority silently persists."""
    t = RedisTransport(fakeredis.FakeStrictRedis(server=server))

    t.publish("G", _event("e1"), priority=0)
    lease = t.claim("w", visibility=30)
    assert lease is not None
    t.ack(lease)  # drains the group; HDEL must remove the prio hash entry

    # re-publish the same group with a higher priority
    t.publish("G", _event("e2"), priority=2)

    # must be claimable at min_priority=2 (returns None if stale prio=0 persisted)
    hi = t.claim("w", visibility=30, min_priority=2)
    assert hi is not None and hi.group_id == "G"


def test_high_priority_group_reachable_behind_a_large_low_priority_backlog(transport):
    """The reason for per-priority tiers: a high-priority group behind MANY more than the
    candidate-window (`_CANDIDATES`) of low-priority groups must still be found by
    claim(min_priority>0). With the old single ZSET + filter-inside-the-window, the high
    group sat outside the lowest-scored window and was starved until ~all the low ones drained."""
    n = RedisTransport._CANDIDATES * 12  # far more low-priority groups than the window
    for i in range(n):
        transport.publish(f"lo{i:04d}", _event("e"), priority=0)
    # id sorts AFTER every low group (all share score 0, so ties break lexically): on the old
    # single-ZSET design it fell outside the lowest-scored candidate window and was invisible.
    transport.publish("zzzz-hi", _event("e"), priority=3)

    lease = transport.claim("w", visibility=30, min_priority=1)
    assert lease is not None and lease.group_id == "zzzz-hi"  # found now, not after n drains


def test_concurrent_claims_get_distinct_groups(server):
    """The atomic Lua claim hands each concurrent claimer a DISTINCT group — no two
    workers ever lease the same one. This is the lost-`SET NX`-race the script removes:
    the old client-side claim let workers fish the same candidate head and collide."""
    n = 20
    pub = RedisTransport(fakeredis.FakeStrictRedis(server=server))
    for i in range(n):
        pub.publish(f"G{i}", _event(f"e{i}"))

    leased: list[str] = []
    lock = threading.Lock()

    def claim_one(wid: str) -> None:
        t = RedisTransport(fakeredis.FakeStrictRedis(server=server))  # own client, shared server
        lease = t.claim(wid, visibility=30)
        if lease is not None:
            with lock:
                leased.append(lease.group_id)

    threads = [threading.Thread(target=claim_one, args=(f"w{i}",)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(leased) == n  # every worker claimed a group
    assert len(set(leased)) == n  # and all distinct — no group double-leased
