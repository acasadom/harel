"""The contract of the outbox seqs `ExecutionStore.commit` returns: `assert_outbox_contract(store)`
(and `assert_async_outbox_contract` for an async store). harel runs it on its own backends;
run it on yours.

The contract: `commit` returns the `seq` of each outbox entry it enqueued, in `emits`
order — the same seqs `pending_outbox` reports for them — and acking one removes exactly
that entry. A commit with no emits (state-only, or only spawns/timers/trace) returns `[]`.

Everything is namespaced by `ns`, and every assertion filters by the seeded ids, so it
holds on a real backend shared with other executions.
"""

from harel.engine.execution import Execution, Status
from harel.engine.store import Step, TimerOp
from harel.spec.states import Event

_KINDS = ("First", "Second", "Third")


def _new(ns: str, suffix: str) -> Execution:
    return Execution(id=f"{ns}{suffix}", definition_id=f"{ns}d")


def _emits(exe: Execution, other: Execution) -> list:
    # mixed targets, so the seqs can't be told apart by target alone
    return [
        (exe.id, Event(kind=_KINDS[0])),
        (other.id, Event(kind=_KINDS[1])),
        (exe.id, Event(kind=_KINDS[2])),
    ]


def _side_effects(exe: Execution) -> dict:
    return dict(
        processed_event_id=f"{exe.id}-ev",
        timers=(TimerOp("schedule", "P", 1.0),),
        spawns=((f"{exe.id}-child", "", {}),),
        trace={"event": "Tick"},
        # every writer passes one: a store's `commit` must take it (and may ignore it)
        step=Step(cause="event", from_status=Status.RUNNING, to_status=Status.RUNNING, event_kind="Tick"),
    )


def _mine(ids: set, entries) -> dict:
    return {e.seq: (e.target_id, e.event.kind) for e in entries if e.target_id in ids}


def assert_outbox_contract(store, ns: str = "") -> None:
    exe, other = _new(ns, "sender"), _new(ns, "other")
    ids = {exe.id, other.id}
    assert store.commit(other, []) == []  # state-only

    seqs = store.commit(exe, _emits(exe, other), **_side_effects(exe))
    assert len(seqs) == 3 and len(set(seqs)) == 3
    assert _mine(ids, store.pending_outbox()) == {
        seqs[0]: (exe.id, _KINDS[0]),
        seqs[1]: (other.id, _KINDS[1]),
        seqs[2]: (exe.id, _KINDS[2]),
    }

    # no emits, other side effects present: still nothing enqueued
    assert store.commit(exe, [], spawns=((f"{exe.id}-child2", "", {}),), trace={"event": "Tock"}) == []

    store.ack_outbox(seqs[1])
    assert set(_mine(ids, store.pending_outbox())) == {seqs[0], seqs[2]}

    later = store.commit(exe, [(other.id, Event(kind="Later"))])
    assert len(later) == 1 and later[0] not in seqs
    for seq in [seqs[0], seqs[2], *later]:
        store.ack_outbox(seq)
    assert _mine(ids, store.pending_outbox()) == {}

    for e in (exe, other):  # leave a shared backend clean
        assert store.purge(e.id, store.load(e.id).version) is True


async def assert_async_outbox_contract(store, ns: str = "") -> None:
    exe, other = _new(ns, "sender"), _new(ns, "other")
    ids = {exe.id, other.id}
    assert await store.commit(other, []) == []

    seqs = await store.commit(exe, _emits(exe, other), **_side_effects(exe))
    assert len(seqs) == 3 and len(set(seqs)) == 3
    assert _mine(ids, await store.pending_outbox()) == {
        seqs[0]: (exe.id, _KINDS[0]),
        seqs[1]: (other.id, _KINDS[1]),
        seqs[2]: (exe.id, _KINDS[2]),
    }

    assert await store.commit(exe, [], spawns=((f"{exe.id}-child2", "", {}),), trace={"event": "Tock"}) == []

    await store.ack_outbox(seqs[1])
    assert set(_mine(ids, await store.pending_outbox())) == {seqs[0], seqs[2]}

    later = await store.commit(exe, [(other.id, Event(kind="Later"))])
    assert len(later) == 1 and later[0] not in seqs
    for seq in [seqs[0], seqs[2], *later]:
        await store.ack_outbox(seq)
    assert _mine(ids, await store.pending_outbox()) == {}

    for e in (exe, other):
        assert await store.purge(e.id, (await store.load(e.id)).version) is True
