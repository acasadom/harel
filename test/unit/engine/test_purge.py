"""Control-plane `purge`: permanently delete a finished execution tree (root, regions,
invokes and all their store rows), optionally archiving it first. The store-level
`purge` primitive is covered backend by backend in test_store_purge.py."""

import json

import pytest

from harel import JsonlArchive
from harel.dsl import definition_from_dsl
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore, SqliteStore, StoreConflict
from harel.spec.states import Event

FLAT = """
event Go {}
machine M {
   initial A
   state A {}
   final B success {}
   from A to B on Go
}
"""

ORTHO = """
event Go {}
machine M {
   initial Fork
   orthogonal Fork {
      state R1 {
         initial W1
         state W1 {}
         final D1 success {}
         from W1 to D1 on Go
      }
      state R2 {
         initial W2
         state W2 {}
         final D2 success {}
         from W2 to D2 on Go
      }
   }
   final Done success {}
   from Fork to Done
}
"""

DEAD_LETTERS = """
event Go {}
machine M {
   initial A
   state A {}
   state B { on enter stm_actions.boom }
   from A to B on Go
}
"""


def _runner(source: str, store=None):
    defn = definition_from_dsl(source, "M")
    store = store if store is not None else DictStore()
    return DurableRunner(store, {defn.id: defn}), store


def _terminated_fork(store=None):
    runner, store = _runner(ORTHO, store)
    exe = runner.create(runner_defn_id(runner))
    children = list(store.load(exe.id).children)
    assert len(children) == 2
    runner.terminate(exe.id)  # regions follow: the whole tree is CANCELLED
    return runner, store, exe.id, children


def runner_defn_id(runner) -> str:
    return next(iter(runner._async.definitions))


def test_purge_deletes_the_whole_finished_tree():
    runner, store, root_id, children = _terminated_fork()

    assert runner.purge(root_id) is True

    assert store.load(root_id) is None
    assert all(store.load(cid) is None for cid in children)
    assert runner.purge(root_id) is False  # already gone: harmless


def test_purge_of_a_normally_finished_execution():
    runner, store = _runner(FLAT)
    exe = runner.create(runner_defn_id(runner))
    runner.process(exe.id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.DONE

    assert runner.purge(exe.id) is True
    assert store.load(exe.id) is None


def test_purge_refuses_a_child():
    runner, store, root_id, children = _terminated_fork()

    with pytest.raises(ValueError, match="is a child of"):
        runner.purge(children[0])
    assert store.load(children[0]) is not None


def test_purge_refuses_a_tree_that_has_not_finished():
    runner, store = _runner(ORTHO)
    exe = runner.create(runner_defn_id(runner))

    with pytest.raises(ValueError, match="not finished"):
        runner.purge(exe.id)
    assert store.load(exe.id) is not None


def test_purge_refuses_a_dead_letter_until_it_is_terminated():
    runner, store = _runner(DEAD_LETTERS)
    exe = runner.create(runner_defn_id(runner))
    runner.process(exe.id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.FAILED

    with pytest.raises(ValueError, match="terminate a dead letter"):
        runner.purge(exe.id)

    runner.terminate(exe.id)  # abandon the dead letter deliberately
    assert runner.purge(exe.id) is True


def test_purge_archives_the_tree_before_deleting_it(tmp_path):
    runner, store, root_id, children = _terminated_fork()
    path = tmp_path / "archive.jsonl"

    runner.purge(root_id, archive=JsonlArchive(path))

    (line,) = path.read_text().splitlines()
    bundle = json.loads(line)
    assert bundle["root_id"] == root_id
    assert [e["id"] for e in bundle["executions"]][0] == root_id
    assert {e["id"] for e in bundle["executions"]} == {root_id, *children}
    assert set(bundle["traces"]) == {root_id, *children}


def test_a_failing_archiver_aborts_the_purge():
    runner, store, root_id, children = _terminated_fork()

    def broken(bundle):
        raise OSError("archive unavailable")

    with pytest.raises(OSError):
        runner.purge(root_id, archive=broken)
    assert store.load(root_id) is not None
    assert all(store.load(cid) is not None for cid in children)


def test_a_member_changed_mid_purge_stops_it_and_a_retry_completes_it(tmp_path):
    # a serializing store: DictStore hands back the same object, which can't model a
    # concurrent writer moving a member on between the check and the delete
    store = SqliteStore(tmp_path / "stm.db")
    runner, store, root_id, children = _terminated_fork(store)

    def concurrent_write(bundle):
        moved = store.load(children[0])
        store.save(moved)  # someone else writes it after purge checked it

    with pytest.raises(StoreConflict):
        runner.purge(root_id, archive=concurrent_write)
    assert store.load(root_id) is not None  # the root goes last, so the purge can resume

    assert runner.purge(root_id) is True
    assert store.load(root_id) is None
    assert all(store.load(cid) is None for cid in children)
    store.close()


async def test_async_purge_with_a_coroutine_archiver():
    from harel.engine.aio import control as aio_control
    from harel.engine.aio_store import AsyncDictStore
    from harel.engine.execution import Execution

    store = AsyncDictStore()
    exe = Execution(definition_id="M", status=Status.CANCELLED)
    await store.save(exe)
    archived = []

    async def archive(bundle):
        archived.append(bundle["root_id"])

    assert await aio_control.purge(store, exe.id, archive=archive) is True
    assert archived == [exe.id]
    assert await store.load(exe.id) is None


# --- descendants the parent no longer lists ------------------------------------------
CHILD = "event Go {}\nmachine Child { initial W  state W {}  final D success {}  from W to D on Go }"
CALLER = """
machine P {
   initial Run
   state Run { invoke Child }
   final Done success {}
   from Run to Done on Returned
}
"""
REFORK = """
event Go {}
event Again {}
event Stop {}
machine M {
   initial Fork
   orthogonal Fork {
      state A { initial A1  state A1 {}  final A2 success {}  from A1 to A2 on Go }
   }
   state Between {}
   final End success {}
   from Fork to Between
   from Between to Fork on Again
   from Between to End on Stop
}
"""


def _stored_ids(store) -> set:
    return set(store._by_id)


def test_purge_reaches_a_returned_invoke_child():
    child, caller = definition_from_dsl(CHILD, "Child"), definition_from_dsl(CALLER, "P")
    store = DictStore()
    runner = DurableRunner(store, {caller.id: caller, child.id: child})
    exe = runner.create(caller.id)
    (cid,) = store.load(exe.id).children
    runner.process(cid, Event(kind="Go"))
    assert store.load(exe.id).children == {}  # the caller dropped the returned child

    assert runner.purge(exe.id) is True
    assert _stored_ids(store) == set()


def test_purge_reaches_the_regions_of_an_earlier_fork_entry():
    runner, store = _runner(REFORK)
    exe = runner.create(runner_defn_id(runner))
    for kind in ("Go", "Again", "Go", "Stop"):  # the second entry replaced the first's regions
        runner.process(exe.id, Event(kind=kind))
    assert len(_stored_ids(store)) == 3

    assert runner.purge(exe.id) is True
    assert _stored_ids(store) == set()


def test_purge_reaches_the_children_a_reset_discarded():
    runner, store = _runner(REFORK)
    exe = runner.create(runner_defn_id(runner))
    for kind in ("Go", "Reset", "Go", "Stop"):
        runner.process(exe.id, Event(kind=kind))

    assert runner.purge(exe.id) is True
    assert _stored_ids(store) == set()


def test_purge_leaves_an_unrelated_execution_whose_id_shares_the_prefix():
    # ids are caller-suppliable: "job:x" is its own root, not a child of "job"
    from harel.engine.execution import Execution

    store = DictStore()
    store.save(Execution(id="job", definition_id="M", status=Status.DONE))
    store.save(Execution(id="job:x", definition_id="M", status=Status.RUNNING))

    from harel.engine import control

    assert control.purge(store, "job") is True
    assert _stored_ids(store) == {"job:x"}


async def test_async_purge_reaches_a_child_the_parent_no_longer_lists():
    from harel.engine.aio import control as aio_control
    from harel.engine.aio_store import AsyncDictStore
    from harel.engine.execution import Execution

    store = AsyncDictStore()
    await store.save(Execution(id="p", definition_id="M", status=Status.DONE))
    await store.save(Execution(id="p:Run:0", definition_id="C", status=Status.DONE, parent_id="p"))

    assert await aio_control.purge(store, "p") is True
    assert await store.load("p:Run:0") is None
