"""Unit tests for the event `Transport`: single-active-consumer per group.

The contract: FIFO within a group, at most one in-flight message per group
(exclusivity), concurrency across groups, and lease recovery on expiry. The two
backends (in-memory, sqlite) are exercised by the same tests; the concurrency
test is what proves the exclusivity holds under real threads racing for claims.
"""

import threading
import time

import pytest

from harel.engine.transport import InMemoryTransport, Lease, SqliteTransport
from harel.spec.states import Event


def _event(kind: str) -> Event:
    return Event(kind=kind)


@pytest.fixture(params=["memory", "sqlite"])
def transport(request, tmp_path):
    if request.param == "memory":
        yield InMemoryTransport()
    else:
        t = SqliteTransport(tmp_path / "q.db")
        yield t
        t.close()


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
    assert a.event.kind == "g1"  # G now in-flight

    # the next claim skips G (in-flight) and takes H instead
    b = transport.claim("w2", visibility=30)
    assert b.event.kind == "h1"

    # with G and H both in-flight, nothing else is deliverable
    assert transport.claim("w3", visibility=30) is None

    # acking G's message frees the group; its second message is now claimable
    transport.ack(a)
    c = transport.claim("w3", visibility=30)
    assert c.event.kind == "g2"


def test_ack_removes_the_message(transport):
    transport.publish("G", _event("only"))
    lease = transport.claim("w", visibility=30)
    transport.ack(lease)
    assert transport.claim("w", visibility=30) is None


def test_nack_returns_the_message_immediately(transport):
    transport.publish("G", _event("e1"))
    lease = transport.claim("w", visibility=30)
    transport.nack(lease)
    again = transport.claim("w", visibility=30)
    assert again.event.kind == "e1"


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_nack_with_delay_parks_the_message_until_it_passes(backend, tmp_path):
    clock = [100.0]
    if backend == "memory":
        t = InMemoryTransport(clock=lambda: clock[0])
    else:
        t = SqliteTransport(tmp_path / "q.db", clock=lambda: clock[0])

    t.publish("G", _event("e1"))
    lease = t.claim("w", visibility=30)
    assert lease is not None
    t.nack(lease, delay=5.0)  # park until t=105
    assert t.claim("w", visibility=30) is None  # not claimable while parked

    clock[0] = 106.0  # past the park window
    again = t.claim("w", visibility=30)
    assert again is not None and again.event.kind == "e1"

    if backend == "sqlite":
        t.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_lease_expiry_makes_a_message_claimable_again(backend, tmp_path):
    clock = [100.0]  # an injectable clock so expiry is deterministic, not wall-clock
    if backend == "memory":
        t = InMemoryTransport(clock=lambda: clock[0])
    else:
        t = SqliteTransport(tmp_path / "q.db", clock=lambda: clock[0])

    t.publish("G", _event("e1"))
    held = t.claim("w1", visibility=10)  # lease until t=110
    assert held.event.kind == "e1"
    assert t.claim("w2", visibility=10) is None  # still leased

    clock[0] = 111.0  # past the lease
    recovered = t.claim("w2", visibility=10)
    assert recovered is not None and recovered.event.kind == "e1"

    if backend == "sqlite":
        t.close()


def _drain_concurrently(publisher, worker_transport, n_workers: int, groups: list[str], per_group: int):
    """Publish `per_group` messages into each group via `publisher`, then have
    `n_workers` threads (each with its own handle from `worker_transport()`)
    claim/ack until everything is consumed. The handles must share backing state
    (the same in-memory instance, or separate connections to the same sqlite
    file). Returns (processed, expected, violations, errors)."""
    expected = []
    for g in groups:
        for i in range(per_group):
            publisher.publish(g, _event(f"{g}-{i}"))
            expected.append(f"{g}-{i}")

    active: set[str] = set()
    active_lock = threading.Lock()
    processed: list[str] = []
    violations: list[str] = []
    errors: list[str] = []
    done = threading.Event()

    def worker(wid: str):
        t = worker_transport()
        try:
            while not done.is_set():
                lease: Lease | None = t.claim(wid, visibility=30)
                if lease is None:
                    time.sleep(0.001)  # yield instead of busy-spinning
                    continue
                with active_lock:
                    if lease.group_id in active:
                        violations.append(lease.group_id)  # two workers in one group
                    active.add(lease.group_id)
                time.sleep(0.002)  # widen the race window so any break would show
                with active_lock:
                    active.discard(lease.group_id)
                    processed.append(lease.event.kind)
                t.ack(lease)
        except Exception as exc:  # a dead worker would otherwise hang the drain
            errors.append(f"{wid}: {exc!r}")

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(n_workers)]
    for th in threads:
        th.start()
    deadline = time.time() + 10  # hard cap so a bug surfaces as a failure, not a hang
    while len(processed) < len(expected) and not errors and time.time() < deadline:
        time.sleep(0.005)
    done.set()
    for th in threads:
        th.join(timeout=5)
    return sorted(processed), sorted(expected), violations, errors


def test_memory_concurrency_preserves_group_exclusivity():
    shared = InMemoryTransport()  # all workers share the one in-memory instance
    processed, expected, violations, errors = _drain_concurrently(
        shared, lambda: shared, n_workers=4, groups=["G", "H", "I", "J"], per_group=5
    )
    assert errors == []
    assert violations == []  # never two workers in the same group at once
    assert processed == expected  # every message processed exactly once


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_round_robin_fairness(backend, tmp_path):
    """A group that was just processed should yield to groups that haven't been claimed yet.
    Group A publishes N messages first; group B publishes 1 message after.
    With round-robin, B must be served before A exhausts its queue."""
    clock = [0.0]
    if backend == "memory":
        t = InMemoryTransport(clock=lambda: clock[0])
    else:
        t = SqliteTransport(tmp_path / "q.db", clock=lambda: clock[0])

    for i in range(5):
        t.publish("A", _event(f"a{i}"))
    t.publish("B", _event("b0"))

    # claim A's first message, then ack it — A's last_claimed_at is now > B's (0)
    clock[0] = 1.0
    lease_a = t.claim("w", visibility=30)
    assert lease_a is not None and lease_a.group_id == "A"
    clock[0] = 2.0
    t.ack(lease_a)

    # next claim must prefer B (last_claimed_at=0) over A (last_claimed_at=1.0)
    clock[0] = 3.0
    lease_b = t.claim("w", visibility=30)
    assert lease_b is not None and lease_b.group_id == "B"

    if backend == "sqlite":
        t.close()


def test_sqlite_concurrency_preserves_group_exclusivity(tmp_path):
    db = tmp_path / "q.db"  # workers open separate connections to the same file
    processed, expected, violations, errors = _drain_concurrently(
        SqliteTransport(db),
        lambda: SqliteTransport(db),
        n_workers=4,
        groups=["G", "H", "I", "J"],
        per_group=5,
    )
    assert errors == []
    assert violations == []
    assert processed == expected
