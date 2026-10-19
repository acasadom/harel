"""The contract of `Transport.backlog`: `assert_backlog_contract(make)` (and
`assert_async_backlog_contract`). harel runs it on its own transports; run it on yours.

`make(clock)` returns a fresh, empty transport whose idea of "now" is `clock()`. The contract:
an empty transport has no backlog; otherwise one `MachineBacklog` per routing key with work
queued — None for messages published without one — counting its waiting messages, the
executions (groups) they are for, those held (their next message leased), and the publish time
of the oldest; an acked message no longer counts. With `by_machine=False` (a transport that
can't tell machines apart, like SQS) the backlog is one total, `by_machine` False.
"""

from __future__ import annotations

from typing import Any, Callable

from harel.spec.states import Event
from harel.testing.transport import _aclose, _Clock, _close

_PUBLISHES = (  # (group, routing key, at)
    ("G1", "order", 100.0),
    ("G1", "order", 101.0),
    ("G2", "order", 102.0),
    ("G3", "payment", 103.0),
    ("G4", None, 104.0),
)
_KEY = {"G1": "order", "G2": "order", "G3": "payment", "G4": None}


def _expected(held_group: str) -> dict:
    rows = {
        "order": [3, 2, 0, 100.0],
        "payment": [1, 1, 0, 103.0],
        None: [1, 1, 0, 104.0],
    }
    rows[_KEY[held_group]][2] = 1
    return rows


def _by_key(backlog: Any) -> dict:
    assert backlog.by_machine, "a transport that routes by machine reports its backlog by machine"
    keys = [b.routing_key for b in backlog.machines]
    assert len(keys) == len(set(keys)), "one row per routing key"
    return {
        b.routing_key: [b.messages, b.executions, b.held, b.oldest_published_at] for b in backlog.machines
    }


def _check_total(backlog: Any, messages: int) -> None:
    assert not backlog.by_machine, "a transport that can't tell machines apart says so"
    assert [(b.routing_key, b.messages) for b in backlog.machines] == ([(None, messages)] if messages else [])


def assert_backlog_contract(make: Callable[[Callable[[], float]], Any], *, by_machine: bool = True) -> None:
    """Check the transports `make(clock)` builds report their backlog (see the module)."""
    clock = _Clock()
    t = make(clock)
    try:
        assert t.backlog().machines == [], "an empty transport has no backlog"
        for group, key, at in _PUBLISHES:
            clock.now = at
            t.publish(group, Event(kind="E"), routing_key=key)
        clock.now = 110.0
        lease = t.claim("w", 30)
        assert lease is not None
        if not by_machine:
            _check_total(t.backlog(), len(_PUBLISHES))
            t.ack(lease)
            _check_total(t.backlog(), len(_PUBLISHES) - 1)
            return
        assert _by_key(t.backlog()) == _expected(lease.group_id), (
            "messages, executions, held and the oldest publish time, by routing key"
        )
        t.ack(lease)
        after = _by_key(t.backlog())
        assert after[_KEY[lease.group_id]][0] == _expected(lease.group_id)[_KEY[lease.group_id]][0] - 1, (
            "an acked message no longer counts"
        )
        assert all(row[2] == 0 for row in after.values()), "a group no longer leased isn't held"
    finally:
        _close(t)


async def assert_async_backlog_contract(
    make: Callable[[Callable[[], float]], Any], *, by_machine: bool = True
) -> None:
    """`assert_backlog_contract` for an async transport; `make(clock)` may be a coroutine
    function."""
    clock = _Clock()
    t = make(clock)
    if hasattr(t, "__await__"):
        t = await t
    try:
        assert (await t.backlog()).machines == []
        for group, key, at in _PUBLISHES:
            clock.now = at
            await t.publish(group, Event(kind="E"), routing_key=key)
        clock.now = 110.0
        lease = await t.claim("w", 30)
        assert lease is not None
        if not by_machine:
            _check_total(await t.backlog(), len(_PUBLISHES))
            await t.ack(lease)
            _check_total(await t.backlog(), len(_PUBLISHES) - 1)
            return
        assert _by_key(await t.backlog()) == _expected(lease.group_id)
        await t.ack(lease)
        after = _by_key(await t.backlog())
        assert after[_KEY[lease.group_id]][0] == _expected(lease.group_id)[_KEY[lease.group_id]][0] - 1
        assert all(row[2] == 0 for row in after.values())
    finally:
        await _aclose(t)
