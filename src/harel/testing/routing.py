"""The contract of routing claims by machine: `assert_machine_routing_contract(make)` (and
`assert_async_machine_routing_contract`) checks a transport that restricts a claim to some
machines does it as harel expects; `assert_refuses_machine_routing(make)` checks one that can't
says so. harel runs them on its own transports; run the one that applies on yours.

`make(clock)` returns a fresh, empty transport. The contract, for a transport that routes:

- a claim given `routing_keys` takes only the groups published with one of them; one given none
  takes any group, keyed or not;
- a group's key is the one its first publish gave — later publishes into it don't change it —
  until it drains: the publish that creates it again sets it again;
- an empty `routing_keys` claims nothing.

A transport that can't route raises `NotImplementedError` on a claim given `routing_keys`, and
still takes publishes that carry a `routing_key`.
"""

from __future__ import annotations

from typing import Any, Callable

from harel.spec.states import Event
from harel.testing.transport import _aclose, _Clock, _close, _kind


def assert_machine_routing_contract(make: Callable[[Callable[[], float]], Any]) -> None:
    """Check the transports `make(clock)` builds route claims by machine (see the module)."""
    t = make(_Clock())
    try:  # a filtered claim takes only its machines' groups; an unfiltered one takes any
        t.publish("A", Event(kind="a"), routing_key="order")
        t.publish("B", Event(kind="b"), routing_key="payment")
        t.publish("C", Event(kind="c"))
        b = t.claim("w1", 30, routing_keys=frozenset({"payment"}))
        assert _kind(b) == "b", "a claim given routing_keys takes a group with one of them"
        assert t.claim("w2", 30, routing_keys=frozenset({"payment"})) is None, (
            "and no group without one of them"
        )
        assert t.claim("w3", 30, routing_keys=frozenset()) is None, "an empty routing_keys claims nothing"
        got = {_kind(t.claim("w4", 30)), _kind(t.claim("w5", 30))}
        assert got == {"a", "c"}, "a claim given no routing_keys takes any group, keyed or not"
    finally:
        _close(t)

    t = make(_Clock())
    try:  # several keys; the key is the group's first publish's, until the group drains
        t.publish("G", Event(kind="g1"), routing_key="order")
        t.publish("G", Event(kind="g2"), routing_key="other")
        t.publish("H", Event(kind="h1"), routing_key="payment")
        assert t.claim("w1", 30, routing_keys=frozenset({"other"})) is None, (
            "a publish into an existing group doesn't change its key"
        )
        both = frozenset({"order", "payment"})
        first, second = t.claim("w2", 30, routing_keys=both), t.claim("w3", 30, routing_keys=both)
        assert {_kind(first), _kind(second)} == {"g1", "h1"}, "a claim takes any of its routing_keys"
        g = first if _kind(first) == "g1" else second
        t.ack(g)
        t.ack(t.claim("w4", 30, routing_keys=frozenset({"order"})))  # g2: G drains
        t.publish("G", Event(kind="g3"), routing_key="other")
        assert _kind(t.claim("w5", 30, routing_keys=frozenset({"other"}))) == "g3", (
            "the publish creating a drained group again sets its key again"
        )
    finally:
        _close(t)


def assert_refuses_machine_routing(make: Callable[[Callable[[], float]], Any]) -> None:
    """Check a transport that can't route claims by machine refuses to (see the module)."""
    t = make(_Clock())
    try:
        t.publish("A", Event(kind="a"), routing_key="order")
        try:
            t.claim("w", 30, routing_keys=frozenset({"order"}))
        except NotImplementedError:
            pass
        else:
            raise AssertionError("a claim given routing_keys must raise NotImplementedError")
        assert _kind(t.claim("w", 30)) == "a", "a claim given no routing_keys still works"
    finally:
        _close(t)


async def _fresh(make: Callable[[Callable[[], float]], Any]) -> Any:
    t = make(_Clock())
    if hasattr(t, "__await__"):
        t = await t
    return t


async def assert_async_machine_routing_contract(make: Callable[[Callable[[], float]], Any]) -> None:
    """`assert_machine_routing_contract` for an async transport; `make(clock)` may be a
    coroutine function."""
    t = await _fresh(make)
    try:
        await t.publish("A", Event(kind="a"), routing_key="order")
        await t.publish("B", Event(kind="b"), routing_key="payment")
        await t.publish("C", Event(kind="c"))
        assert _kind(await t.claim("w1", 30, routing_keys=frozenset({"payment"}))) == "b"
        assert await t.claim("w2", 30, routing_keys=frozenset({"payment"})) is None
        assert await t.claim("w3", 30, routing_keys=frozenset()) is None
        got = {_kind(await t.claim("w4", 30)), _kind(await t.claim("w5", 30))}
        assert got == {"a", "c"}
    finally:
        await _aclose(t)

    t = await _fresh(make)
    try:
        await t.publish("G", Event(kind="g1"), routing_key="order")
        await t.publish("G", Event(kind="g2"), routing_key="other")
        await t.publish("H", Event(kind="h1"), routing_key="payment")
        assert await t.claim("w1", 30, routing_keys=frozenset({"other"})) is None
        both = frozenset({"order", "payment"})
        first, second = await t.claim("w2", 30, routing_keys=both), await t.claim("w3", 30, routing_keys=both)
        assert {_kind(first), _kind(second)} == {"g1", "h1"}
        await t.ack(first if _kind(first) == "g1" else second)
        await t.ack(await t.claim("w4", 30, routing_keys=frozenset({"order"})))
        await t.publish("G", Event(kind="g3"), routing_key="other")
        assert _kind(await t.claim("w5", 30, routing_keys=frozenset({"other"}))) == "g3"
    finally:
        await _aclose(t)


async def assert_async_refuses_machine_routing(make: Callable[[Callable[[], float]], Any]) -> None:
    """`assert_refuses_machine_routing` for an async transport."""
    t = await _fresh(make)
    try:
        await t.publish("A", Event(kind="a"), routing_key="order")
        try:
            await t.claim("w", 30, routing_keys=frozenset({"order"}))
        except NotImplementedError:
            pass
        else:
            raise AssertionError("a claim given routing_keys must raise NotImplementedError")
        assert _kind(await t.claim("w", 30)) == "a"
    finally:
        await _aclose(t)
