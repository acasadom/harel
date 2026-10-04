"""The `ExecutionStore.purge` and `ids_with_prefix` contract: `assert_purge_contract(store)`
(and `assert_async_purge_contract` for an async store) seeds executions and checks a store
purges them as harel expects. harel runs it on its own backends; run it on yours.

The contract: `purge(id, expected_version)` deletes the Execution and everything keyed
by it (dedupe, trace, timers, outbox entries addressed to it, spawn intents it issued)
iff the version still matches; a wrong version is a no-op returning False; purging
again is a harmless False; a stale copy can't resurrect it; and an unrelated
Execution's rows are untouched.

Everything is namespaced by `ns`, and every assertion filters by the seeded ids, so it
holds on a real backend shared with other executions.
"""

from harel.engine.execution import Execution, Status
from harel.engine.store import Step, StoreConflict, TimerOp
from harel.spec.states import Event

FIRE_AT = 1.0
NOW = 10.0


def _commit_args(exe: Execution) -> dict:
    return dict(
        emits=[(exe.id, Event(kind="Tick"))],
        processed_event_id=f"{exe.id}-ev",
        timers=(TimerOp("schedule", "P", FIRE_AT),),
        spawns=((f"{exe.id}-child", "", {}),),
        trace={"event": "Tick"},
        # every writer passes one: a store's `commit` must take it (and may ignore it)
        step=Step(cause="event", from_status=Status.RUNNING, to_status=Status.RUNNING, event_kind="Tick"),
    )


def _new(ns: str, suffix: str) -> Execution:
    return Execution(id=f"{ns}{suffix}", definition_id=f"{ns}d", status=Status.DONE)


def _mine(ids: set, store_rows) -> set:
    return {r for r in store_rows if r in ids}


def assert_purge_contract(store, ns: str = "") -> None:
    victim, bystander = _new(ns, "victim"), _new(ns, "bystander")
    for exe in (victim, bystander):
        store.commit(exe, **_commit_args(exe))
    ids = {victim.id, bystander.id}
    stale = store.load(victim.id)
    version = store.load(victim.id).version

    assert store.purge(victim.id, version + 7) is False  # moved on: untouched
    assert store.load(victim.id) is not None
    assert store.is_processed(victim.id, f"{victim.id}-ev")

    assert store.purge(victim.id, version) is True
    assert store.load(victim.id) is None
    assert not store.is_processed(victim.id, f"{victim.id}-ev")
    assert store.read_trace(victim.id) == []
    assert _mine(ids, {t[0] for t in store.due_timers(NOW)}) == {bystander.id}
    assert _mine(ids, {e.target_id for e in store.pending_outbox()}) == {bystander.id}
    assert _mine(ids, {s.parent_id for s in store.pending_spawns()}) == {bystander.id}

    assert store.load(bystander.id) is not None  # an unrelated Execution is untouched
    assert store.is_processed(bystander.id, f"{bystander.id}-ev")

    assert store.purge(victim.id, version) is False  # already gone: harmless
    try:
        store.commit(stale, [])  # a stale copy can't resurrect it
    except StoreConflict:
        pass
    else:
        raise AssertionError("committing a purged execution's stale copy must raise StoreConflict")
    assert store.load(victim.id) is None

    reborn = _new(ns, "victim")  # a brand-new Execution may reuse the id
    store.commit(reborn, [])
    assert store.load(victim.id).version == 1

    for exe in (reborn, store.load(bystander.id)):  # leave a shared backend clean
        assert store.purge(exe.id, exe.version) is True

    assert_ids_with_prefix_contract(store, ns)


async def assert_async_purge_contract(store, ns: str = "") -> None:
    victim, bystander = _new(ns, "victim"), _new(ns, "bystander")
    for exe in (victim, bystander):
        await store.commit(exe, **_commit_args(exe))
    ids = {victim.id, bystander.id}
    stale = await store.load(victim.id)
    version = (await store.load(victim.id)).version

    assert await store.purge(victim.id, version + 7) is False
    assert await store.load(victim.id) is not None
    assert await store.is_processed(victim.id, f"{victim.id}-ev")

    assert await store.purge(victim.id, version) is True
    assert await store.load(victim.id) is None
    assert not await store.is_processed(victim.id, f"{victim.id}-ev")
    assert await store.read_trace(victim.id) == []
    assert _mine(ids, {t[0] for t in await store.due_timers(NOW)}) == {bystander.id}
    assert _mine(ids, {e.target_id for e in await store.pending_outbox()}) == {bystander.id}
    assert _mine(ids, {s.parent_id for s in await store.pending_spawns()}) == {bystander.id}

    assert await store.load(bystander.id) is not None
    assert await store.is_processed(bystander.id, f"{bystander.id}-ev")

    assert await store.purge(victim.id, version) is False
    try:
        await store.commit(stale, [])
    except StoreConflict:
        pass
    else:
        raise AssertionError("committing a purged execution's stale copy must raise StoreConflict")
    assert await store.load(victim.id) is None

    reborn = _new(ns, "victim")
    await store.commit(reborn, [])
    assert (await store.load(victim.id)).version == 1

    for exe in (reborn, await store.load(bystander.id)):
        assert await store.purge(exe.id, exe.version) is True

    await assert_async_ids_with_prefix_contract(store, ns)


# ids around a tree `r`, plus pairs where one id's prefix is another's with a LIKE / glob
# metacharacter in place of a plain character — each must be matched literally
_PREFIX_SEED = [
    "r",
    "r:A:0",
    "r:A:0:B:0",
    "rx",
    "a_b:1",
    "aXb:1",
    "p%q:1",
    "p1q:1",
    "s*t:1",
    "sxt:1",
    "u[v]:1",
    "uv:1",
]
_PREFIX_CASES = {
    "r:": {"r:A:0", "r:A:0:B:0"},
    "a_b:": {"a_b:1"},
    "p%q:": {"p%q:1"},
    "s*t:": {"s*t:1"},
    "u[v]:": {"u[v]:1"},
}


def _prefix_ok(ns: str, prefix: str, found: list) -> None:
    seeded = {ns + i for i in _PREFIX_SEED}
    assert {i for i in found if i in seeded} == {ns + i for i in _PREFIX_CASES[prefix]}, prefix


def assert_ids_with_prefix_contract(store, ns: str = "") -> None:
    for i in _PREFIX_SEED:
        store.commit(Execution(id=ns + i, definition_id=f"{ns}d"), [])
    for prefix in _PREFIX_CASES:
        _prefix_ok(ns, prefix, store.ids_with_prefix(ns + prefix))
    for i in _PREFIX_SEED:
        assert store.purge(ns + i, 1) is True


async def assert_async_ids_with_prefix_contract(store, ns: str = "") -> None:
    for i in _PREFIX_SEED:
        await store.commit(Execution(id=ns + i, definition_id=f"{ns}d"), [])
    for prefix in _PREFIX_CASES:
        _prefix_ok(ns, prefix, await store.ids_with_prefix(ns + prefix))
    for i in _PREFIX_SEED:
        assert await store.purge(ns + i, 1) is True
