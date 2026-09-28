"""Retention: the wall-clock bookkeeping every commit stamps on an Execution
(`created_at`/`updated_at`/`finished_at`), and `purge_finished`, which purges the
root trees that finished longer ago than a given age (plus the `harel purge` CLI)."""

import json

import pytest

from harel.cli import main
from harel.dsl import definition_from_dsl
from harel.engine.control import purge_finished
from harel.engine.durable import DurableRunner
from harel.engine.execution import ChildState, Execution, Status
from harel.engine.store import DictStore, SqliteStore
from harel.spec.states import Event

FLAT = """
event Go {}
machine M {
   initial A
   state A {}
   state B {}
   final Done success {}
   from A to B on Go
   from B to Done on Go
}
"""


def _runner(clock):
    defn = definition_from_dsl(FLAT, "M")
    store = DictStore()
    return DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0]), store, defn.id


def test_commits_stamp_created_updated_and_finished():
    clock = [100.0]
    runner, store, defn_id = _runner(clock)

    exe = runner.create(defn_id)
    assert (exe.created_at, exe.updated_at, exe.finished_at) == (100.0, 100.0, None)

    clock[0] = 200.0
    runner.process(exe.id, Event(kind="Go"))
    exe = store.load(exe.id)
    assert (exe.created_at, exe.updated_at, exe.finished_at) == (100.0, 200.0, None)

    clock[0] = 300.0
    runner.process(exe.id, Event(kind="Go"))
    exe = store.load(exe.id)
    assert exe.status is Status.DONE
    assert (exe.created_at, exe.updated_at, exe.finished_at) == (100.0, 300.0, 300.0)


def test_a_revived_execution_is_no_longer_finished():
    clock = [100.0]
    runner, store, defn_id = _runner(clock)
    exe = runner.create(defn_id)
    runner.process(exe.id, Event(kind="Go"))
    runner.process(exe.id, Event(kind="Go"))
    assert store.load(exe.id).finished_at == 100.0

    clock[0] = 500.0
    runner.process(exe.id, Event(kind="Reset"))  # a Reset revives a DONE execution

    exe = store.load(exe.id)
    assert exe.status is Status.RUNNING
    assert exe.finished_at is None and exe.updated_at == 500.0


def test_terminate_stamps_finished_at():
    runner, store, defn_id = _runner([100.0])
    exe = runner.create(defn_id)

    runner.terminate(exe.id)

    assert store.load(exe.id).finished_at is not None  # control-plane writes use wall-clock time


# --- purge_finished ----------------------------------------------------------------
def _finished(store, eid, finished_at, status=Status.DONE, **kw):
    store.save(Execution(id=eid, definition_id="M", status=status, finished_at=finished_at, **kw))


@pytest.fixture(params=["dict", "sqlite"])
def store(request, tmp_path):
    # SqliteStore projects finished_at with json_extract; DictStore from the object
    s = DictStore() if request.param == "dict" else SqliteStore(tmp_path / "stm.db")
    yield s
    s.close()


def test_purges_only_roots_finished_before_the_cutoff(store):
    _finished(store, "old", 100.0)
    _finished(store, "old-cancelled", 150.0, status=Status.CANCELLED)
    _finished(store, "recent", 950.0)
    store.save(Execution(id="running", definition_id="M", status=Status.RUNNING))

    report = purge_finished(store, older_than=500, now=1000.0)

    assert sorted(report.purged) == ["old", "old-cancelled"]
    assert store.load("old") is None and store.load("old-cancelled") is None
    assert store.load("recent") is not None and store.load("running") is not None


def test_statuses_narrow_the_candidates_and_failed_is_not_purgeable(store):
    _finished(store, "done", 100.0)
    _finished(store, "cancelled", 100.0, status=Status.CANCELLED)

    report = purge_finished(store, older_than=1, statuses=[Status.CANCELLED], now=1000.0)
    assert report.purged == ["cancelled"]

    with pytest.raises(ValueError, match="can be purged"):
        purge_finished(store, older_than=1, statuses=[Status.FAILED], now=1000.0)


def test_undated_roots_are_skipped_unless_included(store):
    _finished(store, "undated", None)

    report = purge_finished(store, older_than=1, now=1000.0)
    assert report.purged == [] and report.skipped_undated == 1
    assert store.load("undated") is not None

    report = purge_finished(store, older_than=1, include_undated=True, now=1000.0)
    assert report.purged == ["undated"]


def test_dry_run_and_limit(store):
    for i in range(3):
        _finished(store, f"e{i}", 100.0)

    report = purge_finished(store, older_than=1, dry_run=True, now=1000.0)
    assert sorted(report.purged) == ["e0", "e1", "e2"]
    assert all(store.load(f"e{i}") is not None for i in range(3))

    report = purge_finished(store, older_than=1, limit=2, now=1000.0)
    assert len(report.purged) == 2
    assert sum(store.load(f"e{i}") is None for i in range(3)) == 2


def test_a_refused_tree_is_reported_and_the_run_goes_on(store):
    # a root that finished while one of its regions is still live
    store.save(Execution(id="child", definition_id="M", status=Status.RUNNING, parent_id="stuck"))
    _finished(store, "stuck", 100.0, children={"child": ChildState(root_path="R")})
    _finished(store, "fine", 100.0)

    report = purge_finished(store, older_than=1, now=1000.0)

    assert report.purged == ["fine"]
    assert "not finished" in report.refused["stuck"]
    assert store.load("stuck") is not None and store.load("child") is not None


# --- harel purge ---------------------------------------------------------------------
def test_cli_purge(tmp_path, monkeypatch, capsys):
    db = tmp_path / "stm.db"
    store = SqliteStore(db)
    _finished(store, "old", 100.0)
    store.close()
    monkeypatch.setenv("STM_STORE_BACKEND", "sqlite")
    monkeypatch.setenv("STM_STORE_DB", str(db))
    archive = tmp_path / "archive.jsonl"

    assert main(["purge", "--older-than", "1d", "--dry-run", "-v"]) == 0
    assert capsys.readouterr().out.splitlines() == ["would purge: 1", "  old"]

    assert main(["purge", "--older-than", "1d", "--archive", str(archive)]) == 0
    assert capsys.readouterr().out.splitlines() == ["purged: 1"]
    assert json.loads(archive.read_text())["root_id"] == "old"

    store = SqliteStore(db)
    assert store.load("old") is None
    store.close()


def test_cli_rejects_a_malformed_age(capsys):
    with pytest.raises(SystemExit):
        main(["purge", "--older-than", "soon"])
    assert "invalid age" in capsys.readouterr().err


def test_an_archiver_error_stops_the_run_even_if_it_is_a_value_error(store):
    # only purge's own refusals (PurgeRefused / StoreConflict) are recorded and skipped
    _finished(store, "a", 100.0)
    _finished(store, "b", 100.0)

    def archive(bundle):
        raise ValueError("archive rejected the bundle")

    with pytest.raises(ValueError, match="archive rejected"):
        purge_finished(store, older_than=1, archive=archive, now=1000.0)
    assert store.load("a") is not None and store.load("b") is not None


def test_cli_reports_a_missing_store_setting_cleanly(monkeypatch, capsys):
    monkeypatch.setenv("STM_STORE_BACKEND", "sqlite")
    monkeypatch.delenv("STM_STORE_DB", raising=False)

    assert main(["purge", "--older-than", "1d"]) == 1
    assert "STM_STORE_DB is required" in capsys.readouterr().err


def test_cli_reports_an_unwritable_archive_cleanly(tmp_path, monkeypatch, capsys):
    db = tmp_path / "stm.db"
    store = SqliteStore(db)
    _finished(store, "old", 100.0)
    store.close()
    monkeypatch.setenv("STM_STORE_BACKEND", "sqlite")
    monkeypatch.setenv("STM_STORE_DB", str(db))

    assert main(["purge", "--older-than", "1d", "--archive", str(tmp_path / "missing" / "a.jsonl")]) == 1
    assert capsys.readouterr().err.startswith("error:")
    store = SqliteStore(db)
    assert store.load("old") is not None  # the archive failed first, so nothing was deleted
    store.close()
