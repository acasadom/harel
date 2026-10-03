"""The `Transport` contract: `assert_transport_contract(make)` (and
`assert_async_transport_contract` for an async transport) checks a transport delivers as harel
expects. harel runs it on its own backends; run it on yours.

`make(clock)` returns a fresh, empty transport whose idea of "now" is `clock()` — each check
builds its own. The contract:

- **FIFO within a group**, and **at most one message of a group in flight**, while other
  groups proceed;
- `ack` removes the message and frees its group; `nack` makes it claimable again at once;
- with `timing` (a transport that measures every deadline with the injected clock): `nack(delay)`
  parks the message until the delay passes, an expired lease makes it claimable again, and a
  group just served yields to one never claimed (round-robin). Pass `timing=False` for a
  transport that keeps deadlines elsewhere (server-side TTLs, a queue service's visibility);
- with `priorities`: `claim(min_priority=n)` skips groups below `n`.
"""

from __future__ import annotations

from typing import Any, Callable

from harel.spec.states import Event


class _Clock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def _close(transport: Any) -> None:
    close = getattr(transport, "close", None)
    if close is not None:
        close()


async def _aclose(transport: Any) -> None:
    close = getattr(transport, "close", None)
    if close is not None:
        await close()


def _kind(lease: Any) -> Any:
    return lease.event.kind if lease is not None else None


def assert_transport_contract(
    make: Callable[[Callable[[], float]], Any], *, timing: bool = True, priorities: bool = True
) -> None:
    """Check the transports `make(clock)` builds against the contract (see the module)."""

    def fresh() -> tuple[Any, _Clock]:
        clock = _Clock()
        return make(clock), clock

    t, _ = fresh()
    try:  # FIFO within a group
        t.publish("G", Event(kind="e1"))
        t.publish("G", Event(kind="e2"))
        first = t.claim("w", 30)
        assert _kind(first) == "e1", "a group's messages come out in the order they went in"
        t.ack(first)
        assert _kind(t.claim("w", 30)) == "e2"
    finally:
        _close(t)

    t, _ = fresh()
    try:  # one in flight per group; other groups proceed
        t.publish("G", Event(kind="g1"))
        t.publish("G", Event(kind="g2"))
        t.publish("H", Event(kind="h1"))
        a = t.claim("w1", 30)
        assert _kind(a) == "g1"
        assert _kind(t.claim("w2", 30)) == "h1", "a group in flight is skipped; another group proceeds"
        assert t.claim("w3", 30) is None, "with every group in flight, nothing is claimable"
        t.ack(a)
        assert _kind(t.claim("w3", 30)) == "g2", "acking frees the group"
    finally:
        _close(t)

    t, _ = fresh()
    try:  # ack removes; nack returns at once
        t.publish("G", Event(kind="only"))
        t.nack(t.claim("w", 30))
        again = t.claim("w", 30)
        assert _kind(again) == "only", "nack makes the message claimable again"
        t.ack(again)
        assert t.claim("w", 30) is None, "ack removes the message"
    finally:
        _close(t)

    if timing:
        t, clock = fresh()
        try:  # nack(delay) parks
            t.publish("G", Event(kind="e1"))
            t.nack(t.claim("w", 30), delay=5.0)
            assert t.claim("w", 30) is None, "a parked message isn't claimable before its delay"
            clock.now += 6
            assert _kind(t.claim("w", 30)) == "e1", "a parked message comes back after its delay"
        finally:
            _close(t)

        t, clock = fresh()
        try:  # an expired lease
            t.publish("G", Event(kind="e1"))
            assert _kind(t.claim("w1", 10)) == "e1"
            assert t.claim("w2", 10) is None, "a leased message isn't claimable"
            clock.now += 11
            assert _kind(t.claim("w2", 10)) == "e1", "an expired lease makes the message claimable"
        finally:
            _close(t)

        t, clock = fresh()
        try:  # round-robin
            for i in range(5):
                t.publish("A", Event(kind=f"a{i}"))
            t.publish("B", Event(kind="b0"))
            clock.now += 1
            served = t.claim("w", 30)
            assert served is not None and served.group_id == "A"
            clock.now += 1
            t.ack(served)
            clock.now += 1
            nxt = t.claim("w", 30)
            assert nxt is not None and nxt.group_id == "B", "a group just served yields to one never claimed"
        finally:
            _close(t)

    if priorities:
        t, _ = fresh()
        try:
            t.publish("lo", Event(kind="e1"), priority=0)
            t.publish("hi", Event(kind="e2"), priority=2)
            hi = t.claim("w", 30, min_priority=2)
            assert hi is not None and hi.group_id == "hi"
            t.ack(hi)
            assert t.claim("w", 30, min_priority=2) is None, "claim(min_priority) skips lower groups"
            lo = t.claim("w", 30)
            assert lo is not None and lo.group_id == "lo"
        finally:
            _close(t)


async def assert_async_transport_contract(
    make: Callable[[Callable[[], float]], Any], *, timing: bool = True, priorities: bool = True
) -> None:
    """`assert_transport_contract` for an async transport; `make(clock)` may be a coroutine
    function."""

    async def fresh() -> tuple[Any, _Clock]:
        clock = _Clock()
        t = make(clock)
        if hasattr(t, "__await__"):
            t = await t
        return t, clock

    t, _ = await fresh()
    try:
        await t.publish("G", Event(kind="e1"))
        await t.publish("G", Event(kind="e2"))
        first = await t.claim("w", 30)
        assert _kind(first) == "e1", "a group's messages come out in the order they went in"
        await t.ack(first)
        assert _kind(await t.claim("w", 30)) == "e2"
    finally:
        await _aclose(t)

    t, _ = await fresh()
    try:
        await t.publish("G", Event(kind="g1"))
        await t.publish("G", Event(kind="g2"))
        await t.publish("H", Event(kind="h1"))
        a = await t.claim("w1", 30)
        assert _kind(a) == "g1"
        assert _kind(await t.claim("w2", 30)) == "h1", "a group in flight is skipped; another group proceeds"
        assert await t.claim("w3", 30) is None, "with every group in flight, nothing is claimable"
        await t.ack(a)
        assert _kind(await t.claim("w3", 30)) == "g2", "acking frees the group"
    finally:
        await _aclose(t)

    t, _ = await fresh()
    try:
        await t.publish("G", Event(kind="only"))
        await t.nack(await t.claim("w", 30))
        again = await t.claim("w", 30)
        assert _kind(again) == "only", "nack makes the message claimable again"
        await t.ack(again)
        assert await t.claim("w", 30) is None, "ack removes the message"
    finally:
        await _aclose(t)

    if timing:
        t, clock = await fresh()
        try:
            await t.publish("G", Event(kind="e1"))
            await t.nack(await t.claim("w", 30), delay=5.0)
            assert await t.claim("w", 30) is None, "a parked message isn't claimable before its delay"
            clock.now += 6
            assert _kind(await t.claim("w", 30)) == "e1", "a parked message comes back after its delay"
        finally:
            await _aclose(t)

        t, clock = await fresh()
        try:
            await t.publish("G", Event(kind="e1"))
            assert _kind(await t.claim("w1", 10)) == "e1"
            assert await t.claim("w2", 10) is None, "a leased message isn't claimable"
            clock.now += 11
            assert _kind(await t.claim("w2", 10)) == "e1", "an expired lease makes the message claimable"
        finally:
            await _aclose(t)

        t, clock = await fresh()
        try:
            for i in range(5):
                await t.publish("A", Event(kind=f"a{i}"))
            await t.publish("B", Event(kind="b0"))
            clock.now += 1
            served = await t.claim("w", 30)
            assert served is not None and served.group_id == "A"
            clock.now += 1
            await t.ack(served)
            clock.now += 1
            nxt = await t.claim("w", 30)
            assert nxt is not None and nxt.group_id == "B", "a group just served yields to one never claimed"
        finally:
            await _aclose(t)

    if priorities:
        t, _ = await fresh()
        try:
            await t.publish("lo", Event(kind="e1"), priority=0)
            await t.publish("hi", Event(kind="e2"), priority=2)
            hi = await t.claim("w", 30, min_priority=2)
            assert hi is not None and hi.group_id == "hi"
            await t.ack(hi)
            assert await t.claim("w", 30, min_priority=2) is None, "claim(min_priority) skips lower groups"
            lo = await t.claim("w", 30)
            assert lo is not None and lo.group_id == "lo"
        finally:
            await _aclose(t)
