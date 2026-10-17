"""The contract of `ExecutionStore.pending_work`: `assert_pending_work_contract(store)` (and
`assert_async_pending_work_contract`). harel runs it on its own backends; run it on yours.

The contract: `pending_work(id)` returns what is waiting for that execution only — its armed
timers (soonest first), the outbox entries addressed to it whoever emitted them, and the child
spawns it issued (in `seq` order) — and none of it once delivered: an acked entry, a deleted
timer. An execution with nothing waiting, or none at all, has an empty `PendingWork`.

Everything is namespaced by `ns`, so it holds on a backend shared with other executions.
"""

from harel.engine.execution import Execution, Status
from harel.engine.store import Step, TimerOp
from harel.spec.states import Event

_STEP = Step(cause="event", from_status=Status.RUNNING, to_status=Status.RUNNING)


def _seed(ns: str) -> tuple[Execution, Execution, list]:
    a, b = Execution(id=f"{ns}a", definition_id=f"{ns}d"), Execution(id=f"{ns}b", definition_id=f"{ns}d")
    timers = (TimerOp("schedule", "Wait.Slow", 5.0), TimerOp("schedule", "Wait", 3.0))
    spawns = ((f"{ns}a:R:0", "R", {"k": 1}),)
    emits_a = [(a.id, Event(kind="ToSelf")), (b.id, Event(kind="ToB"))]
    emits_b = [(a.id, Event(kind="FromB"))]
    return a, b, [(a, emits_a, timers, spawns), (b, emits_b, (), ())]


def _check(work_a, work_b, a, b, ns: str) -> None:
    assert work_a.timers == [("Wait", 3.0), ("Wait.Slow", 5.0)], "an execution's timers, soonest first"
    assert [e.event.kind for e in work_a.outbox] == ["ToSelf", "FromB"], (
        "the outbox entries addressed to it, whoever emitted them, in seq order"
    )
    assert all(e.target_id == a.id for e in work_a.outbox)
    assert [(s.parent_id, s.child_id, s.root_path, s.context) for s in work_a.spawns] == [
        (a.id, f"{ns}a:R:0", "R", {"k": 1})
    ], "the child spawns it issued"
    assert work_b.timers == [] and work_b.spawns == [], "nothing of another execution's"
    assert [e.event.kind for e in work_b.outbox] == ["ToB"]


def assert_pending_work_contract(store, ns: str = "pw-") -> None:
    a, b, commits = _seed(ns)
    for exe, emits, timers, spawns in commits:
        store.commit(exe, emits, timers=timers, spawns=spawns, step=_STEP)
    _check(store.pending_work(a.id), store.pending_work(b.id), a, b, ns)

    work = store.pending_work(a.id)
    store.ack_outbox(work.outbox[0].seq)
    store.ack_spawn(work.spawns[0].seq)
    store.delete_timer(a.id, "Wait", 3.0)
    after = store.pending_work(a.id)
    assert after.timers == [("Wait.Slow", 5.0)], "a deleted timer is gone"
    assert [e.event.kind for e in after.outbox] == ["FromB"], "an acked entry is gone"
    assert after.spawns == [], "an acked spawn is gone"

    empty = store.pending_work(f"{ns}nobody")
    assert (empty.timers, empty.outbox, empty.spawns) == ([], [], [])


async def assert_async_pending_work_contract(store, ns: str = "pw-") -> None:
    a, b, commits = _seed(ns)
    for exe, emits, timers, spawns in commits:
        await store.commit(exe, emits, timers=timers, spawns=spawns, step=_STEP)
    _check(await store.pending_work(a.id), await store.pending_work(b.id), a, b, ns)

    work = await store.pending_work(a.id)
    await store.ack_outbox(work.outbox[0].seq)
    await store.ack_spawn(work.spawns[0].seq)
    await store.delete_timer(a.id, "Wait", 3.0)
    after = await store.pending_work(a.id)
    assert after.timers == [("Wait.Slow", 5.0)]
    assert [e.event.kind for e in after.outbox] == ["FromB"]
    assert after.spawns == []

    empty = await store.pending_work(f"{ns}nobody")
    assert (empty.timers, empty.outbox, empty.spawns) == ([], [], [])
