"""`aio.control.purge_finished`: the async mirror of `control.purge_finished`, over the async
stores' `list_executions` (see test/unit/engine/test_retention.py for the sync one)."""

import pytest

from harel.engine.aio.control import purge_finished
from harel.engine.aio_store import AsyncDictStore, AsyncSqliteStore
from harel.engine.execution import ChildState, Execution, Status


async def _finished(store, eid, finished_at, status=Status.DONE, **kw):
    await store.save(Execution(id=eid, definition_id="M", status=status, finished_at=finished_at, **kw))


@pytest.fixture(params=["dict", "sqlite"])
async def store(request, tmp_path):
    s = (
        AsyncDictStore()
        if request.param == "dict"
        else await AsyncSqliteStore.create(str(tmp_path / "stm.db"))
    )
    yield s
    await s.close()


async def test_purges_only_roots_finished_before_the_cutoff(store):
    await _finished(store, "old", 100.0)
    await _finished(store, "old-cancelled", 150.0, status=Status.CANCELLED)
    await _finished(store, "recent", 950.0)
    await store.save(Execution(id="running", definition_id="M", status=Status.RUNNING))

    report = await purge_finished(store, older_than=500, now=1000.0)

    assert sorted(report.purged) == ["old", "old-cancelled"]
    assert await store.load("old") is None and await store.load("old-cancelled") is None
    assert await store.load("recent") is not None and await store.load("running") is not None


async def test_statuses_narrow_the_candidates_and_failed_is_not_purgeable(store):
    await _finished(store, "done", 100.0)
    await _finished(store, "cancelled", 100.0, status=Status.CANCELLED)

    report = await purge_finished(store, older_than=1, statuses=[Status.CANCELLED], now=1000.0)
    assert report.purged == ["cancelled"]

    with pytest.raises(ValueError, match="can be purged"):
        await purge_finished(store, older_than=1, statuses=[Status.FAILED], now=1000.0)


async def test_undated_roots_are_skipped_unless_included(store):
    await _finished(store, "undated", None)

    report = await purge_finished(store, older_than=1, now=1000.0)
    assert report.purged == [] and report.skipped_undated == 1

    report = await purge_finished(store, older_than=1, include_undated=True, now=1000.0)
    assert report.purged == ["undated"]


async def test_dry_run_and_limit(store):
    for i in range(3):
        await _finished(store, f"e{i}", 100.0)

    report = await purge_finished(store, older_than=1, dry_run=True, now=1000.0)
    assert sorted(report.purged) == ["e0", "e1", "e2"]
    assert all([await store.load(f"e{i}") is not None for i in range(3)])

    report = await purge_finished(store, older_than=1, limit=2, now=1000.0)
    assert len(report.purged) == 2
    assert sum([await store.load(f"e{i}") is None for i in range(3)]) == 2


async def test_a_refused_tree_is_reported_by_a_dry_run_and_a_real_run_alike(store):
    await store.save(Execution(id="child", definition_id="M", status=Status.RUNNING, parent_id="stuck"))
    await _finished(store, "stuck", 100.0, children={"child": ChildState(root_path="R")})
    await _finished(store, "fine", 100.0)

    dry = await purge_finished(store, older_than=1, dry_run=True, now=1000.0)
    assert dry.purged == ["fine"] and "not finished" in dry.refused["stuck"]

    real = await purge_finished(store, older_than=1, now=1000.0)
    assert (real.purged, set(real.refused)) == (dry.purged, set(dry.refused))
    assert await store.load("stuck") is not None and await store.load("child") is not None


async def test_a_coroutine_archiver_gets_each_tree_and_its_error_stops_the_run(store):
    await _finished(store, "a", 100.0)
    archived = []

    async def archive(bundle):
        archived.append(bundle["root_id"])

    report = await purge_finished(store, older_than=1, archive=archive, now=1000.0)
    assert report.purged == ["a"] and archived == ["a"]

    await _finished(store, "b", 100.0)

    async def failing(bundle):
        raise ValueError("archive rejected the bundle")

    with pytest.raises(ValueError, match="archive rejected"):
        await purge_finished(store, older_than=1, archive=failing, now=1000.0)
    assert await store.load("b") is not None
