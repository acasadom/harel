"""Machine versions: `machine order version 2 { ... }`. Several versions of a machine run side
by side — a new execution gets the highest loaded, an existing one keeps the version it was
created with (its children too). Each execution also records its version's source digest, so
a machine edited without a new version fails the executions running on it
(`DefinitionChanged`) instead of silently driving them with different content."""

import asyncio
import logging

import pytest

from harel import (
    DefinitionNotLoaded,
    DictStore,
    DslError,
    Event,
    definition_from_dsl,
    definition_registry,
    machine_version,
)
from harel.cli import main
from harel.dsl import parse
from harel.engine.aio.distributed import AsyncDistributedRunner, AsyncWorker
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DefinitionRecord
from harel.engine.transport import InMemoryTransport
from harel.published import (
    DefinitionConflict,
    PublishedDefinitions,
    allowed_action_modules,
    bundle,
    check_action_modules,
    definition_from_record,
    definition_records,
    publish_definitions,
)
from harel.worker import load_definitions

V1 = """
event Go {}
event Ok {}
machine order {
  initial Draft
  state Draft {}
  state Review {}
  final Done success
  from Draft to Review on Go
  from Review to Done on Ok
}
"""
# v2 adds a step between Review and Done
V2 = """
event Go {}
event Ok {}
machine order version 2 {
  initial Draft
  state Draft {}
  state Review {}
  state Shipping {}
  final Done success
  from Draft to Review on Go
  from Review to Shipping on Ok
  from Shipping to Done on Ok
}
"""


def _v(source, name="order"):
    return definition_from_dsl(source, name)


# --- the DSL -------------------------------------------------------------------------------


def test_a_machine_declares_its_version_and_defaults_to_1():
    assert _v(V1).version == 1
    assert _v(V2).version == 2
    assert parse(V2).versions == {"order": 2}


def test_a_version_is_a_positive_integer():
    with pytest.raises(DslError, match="positive integer"):
        parse("machine m version 0 { initial A final A done }")
    with pytest.raises(DslError):
        parse("machine m version two { initial A final A done }")


def test_the_digest_ignores_comments_layout_and_the_version_clause():
    base = _v(V1).digest
    assert base is not None
    relaid = "# a comment\n" + V1.replace("\n  ", "\n      ").replace("Review {}", "Review {   } // here")
    assert _v(relaid).digest == base
    assert _v(V1.replace("machine order {", "machine order version 7 {")).digest == base


def test_the_digest_covers_the_machine_and_what_its_file_shares():
    base = _v(V1).digest
    assert _v(V1.replace("from Review to Done on Ok", "from Review to Done on Go")).digest != base
    assert _v(V1.replace("event Ok {}", "event Ok { why: string? }")).digest != base  # an event
    assert _v("guard g = x == 1\n" + V1).digest != base  # a guard
    assert _v("fragment F { initial A final A done }\n" + V1).digest != base  # a fragment


def test_the_other_machines_of_the_file_dont_count():
    other = "\nmachine other { initial A state A {} final B done from A to B on Go }\n"
    assert _v(V1 + other).digest == _v(V1).digest
    edited = other.replace("from A to B", "from B to A").replace("initial A", "initial B")
    assert _v(V1 + other).digest != _v(V1 + edited, "other").digest
    assert _v(V1 + edited).digest == _v(V1).digest


def test_an_imported_file_counts(tmp_path):
    lib = tmp_path / "lib.stm"
    lib.write_text("event Go {}\n")
    main_src = 'import "lib.stm"\nevent Ok {}\n' + V1.replace("event Go {}\nevent Ok {}\n", "")
    (tmp_path / "main.stm").write_text(main_src)
    before = machine_version(main_src, "order", base_path=tmp_path)
    lib.write_text("event Go { n: int? }\n")
    after = machine_version(main_src, "order", base_path=tmp_path)
    assert before[0] == after[0] == 1 and before[1] != after[1]


def test_an_inline_submachine_carries_its_parents_version():
    src = """
    machine parent version 3 {
      initial Run
      state Run { invoke { initial A final A done } }
      final Done success
      from Run to Done on Returned
    }
    """
    defn = _v(src, "parent")
    [sub] = defn.submachines.values()
    assert (sub.version, sub.digest) == (3, defn.digest)


# --- the registry --------------------------------------------------------------------------


def test_the_registry_holds_every_version_and_points_the_name_at_the_highest():
    v1, v2 = _v(V1), _v(V2)
    reg = definition_registry([v2, v1])
    assert reg["order"] is v2
    assert reg["order@1"] is v1 and reg["order@2"] is v2


def test_the_same_version_twice_with_different_content_is_refused():
    edited = _v(V1.replace("from Review to Done on Ok", "from Review to Done on Go"))
    with pytest.raises(ValueError, match="version 1 is loaded twice"):
        definition_registry([_v(V1), edited])
    definition_registry([_v(V1), _v(V1)])  # the same content twice is fine


def test_the_worker_loads_every_version_from_its_directory(tmp_path):
    (tmp_path / "order.v1.stm").write_text(V1)
    (tmp_path / "order.v2.stm").write_text(V2)
    reg = load_definitions(str(tmp_path))
    assert (reg["order"].version, reg["order@1"].version) == (2, 1)

    (tmp_path / "order.copy.stm").write_text(V1.replace("on Ok", "on Go"))
    with pytest.raises(RuntimeError, match="loaded twice"):
        load_definitions(str(tmp_path))


# --- running ------------------------------------------------------------------------------


def _review(store, defs):
    runner = DurableRunner(store, defs)
    exe = runner.create("order")
    return runner.process(exe.id, Event(kind="Go"))


def test_an_execution_is_stamped_with_its_version_and_digest():
    exe = _review(DictStore(), definition_registry([_v(V1)]))
    assert (exe.definition_version, exe.definition_digest) == (1, _v(V1).digest)


def test_an_execution_keeps_its_version_and_a_new_one_gets_the_highest():
    store = DictStore()
    old = _review(store, definition_registry([_v(V1)]))

    runner = DurableRunner(store, definition_registry([_v(V1), _v(V2)]))
    assert runner.process(old.id, Event(kind="Ok")).status is Status.DONE  # v1: Review -> Done

    new = runner.create("order")
    runner.process(new.id, Event(kind="Go"))
    new = runner.process(new.id, Event(kind="Ok"))
    assert (new.definition_version, new.active_path) == (2, "Shipping")


def test_create_can_ask_for_an_older_version():
    store = DictStore()
    runner = DurableRunner(store, definition_registry([_v(V1), _v(V2)]))
    assert runner.create("order", version=1).definition_version == 1
    with pytest.raises(KeyError, match="version 5"):
        runner.create("order", version=5)


def test_a_version_the_runner_hasnt_loaded_is_an_error_not_a_failure():
    store = DictStore()
    old = _review(store, definition_registry([_v(V1)]))
    runner = DurableRunner(store, definition_registry([_v(V2)]))
    with pytest.raises(DefinitionNotLoaded, match="'order' version 1"):
        runner.process(old.id, Event(kind="Ok"))
    assert store.load(old.id).status is Status.RUNNING  # untouched


TIMED = """
event Go {}
machine timed version {v} {
  initial Wait
  state Wait { timeout 5 }
  final Late done
  from Wait to Late on Timeout
}
"""


def test_a_timer_of_a_version_not_loaded_stays_armed_and_the_others_fire():
    store, now = DictStore(), [0.0]
    v1 = _v(TIMED.replace("{v}", "1"), "timed")
    v2 = _v(TIMED.replace("{v}", "2"), "timed")
    old = DurableRunner(store, definition_registry([v1]), clock=lambda: now[0]).create("timed")
    runner = DurableRunner(store, definition_registry([v2]), clock=lambda: now[0])
    new = runner.create("timed")
    now[0] = 10.0
    assert runner.fire_due_timers() == 1
    assert store.load(new.id).status is Status.DONE
    assert store.load(old.id).active_path == "Wait" and store.due_timers(now[0])


def test_a_worker_without_the_version_leaves_the_message_for_one_that_has_it():
    store, transport = DictStore(), InMemoryTransport()
    old = DistributedRunner(store, transport, definition_registry([_v(V1)]), execution="inline")
    exe = old.create("order")
    _drain(old.worker(visibility=0.0))
    old.send(exe.id, Event(kind="Go"))
    _drain(old.worker(visibility=0.0))

    old.send(exe.id, Event(kind="Ok"))
    new_only = DistributedRunner(store, transport, definition_registry([_v(V2)]), execution="inline")
    with pytest.raises(DefinitionNotLoaded):
        new_only.worker(visibility=0.0).step()  # not acked: the message stays queued
    assert store.load(exe.id).status is Status.RUNNING

    both = DistributedRunner(store, transport, definition_registry([_v(V1), _v(V2)]), execution="inline")
    _drain(both.worker(visibility=0.0))
    assert store.load(exe.id).status is Status.DONE


def _drain(worker):
    while worker.step():
        pass


async def test_the_worker_loop_logs_it_and_retries_until_a_worker_has_it(caplog):
    store, transport = AsyncDictStore(), AsyncInMemoryTransport()
    old = AsyncDistributedRunner(store, transport, definition_registry([_v(V1)]))
    exe = await old.create("order")
    await _drain_async(old.worker())
    await old.send(exe.id, Event(kind="Go"))
    await _drain_async(old.worker())
    await old.send(exe.id, Event(kind="Ok"))

    new_only = AsyncWorker(store, transport, definition_registry([_v(V2)]), suspend_recheck=0.01)
    stop = asyncio.Event()
    task = asyncio.create_task(new_only.run(stop, idle_sleep=0.001))
    with caplog.at_level(logging.ERROR):
        for _ in range(500):
            if "hasn't loaded" in caplog.text:
                break
            await asyncio.sleep(0.002)
    stop.set()
    await task
    assert "'order' version 1, which this runner hasn't loaded" in caplog.text
    assert (await store.load(exe.id)).status is Status.RUNNING

    both = AsyncWorker(store, transport, definition_registry([_v(V1), _v(V2)]))
    await asyncio.sleep(0.02)  # past the nack's delay
    await _drain_async(both)
    assert (await store.load(exe.id)).status is Status.DONE


async def _drain_async(worker):
    while await worker.step():
        pass


def test_an_execution_from_before_versions_runs_as_version_1_and_is_stamped():
    store = DictStore()
    exe = _review(store, definition_registry([_v(V1)]))
    legacy = store.load(exe.id)
    legacy.definition_version = legacy.definition_digest = None
    store.save(legacy)

    runner = DurableRunner(store, definition_registry([_v(V1), _v(V2)]))
    exe = runner.process(exe.id, Event(kind="Ok"))
    assert (exe.status, exe.definition_version, exe.definition_digest) == (Status.DONE, 1, _v(V1).digest)


def test_a_redrive_moves_a_failed_execution_onto_the_current_source():
    store = DictStore()
    exe = _review(store, definition_registry([_v(V1)]))
    edited = _v(V1.replace("final Done success", "final Done success\n  state Spare {}"))
    runner = DurableRunner(store, definition_registry([edited]))
    assert runner.process(exe.id, Event(kind="Ok")).status is Status.FAILED

    exe = runner.redrive(exe.id, "Review")
    assert exe.definition_digest == edited.digest
    assert runner.process(exe.id, Event(kind="Ok")).status is Status.DONE


REGIONS = """
event Go {}
machine split version {v} {
  initial Fork
  orthogonal Fork {
    state A { initial A1 state A1 {} final A2 done from A1 to A2 on Go }
    state B { initial B1 state B1 {} final B2 done from B1 to B2 on Go }
  }
  final Done success
  from Fork to Done
}
"""


def test_regions_run_on_their_parents_version():
    store = DictStore()
    v1 = _v(REGIONS.replace("{v}", "1"), "split")
    runner = DurableRunner(store, definition_registry([v1]))
    exe = runner.create("split")
    v2 = _v(REGIONS.replace("{v}", "2").replace("final Done success", "final Done done"), "split")
    runner = DurableRunner(store, definition_registry([v1, v2]))
    for child_id in exe.children:
        child = store.load(child_id)
        assert (child.definition_version, child.definition_digest) == (1, v1.digest)
    assert runner.process(exe.id, Event(kind="Go")).outcome == "success"  # v1's verdict


INVOKER = """
event Go {}
machine parent version {v} {
  initial Idle
  state Idle {}
  state Run { invoke { initial {first} state {first} {} final End done from {first} to End on Go } }
  final Done success
  from Idle to Run on Go
  from Run to Done on Returned
}
"""


def test_an_inline_invoke_runs_its_parents_version_of_the_submachine():
    store = DictStore()
    v1 = _v(INVOKER.replace("{v}", "1").replace("{first}", "Old"), "parent")
    exe = DurableRunner(store, definition_registry([v1])).create("parent")

    v2 = _v(INVOKER.replace("{v}", "2").replace("{first}", "New"), "parent")
    runner = DurableRunner(store, definition_registry([v1, v2]))
    exe = runner.process(exe.id, Event(kind="Go"))  # v1 enters Run: spawns its own inline child
    [child_id] = exe.children
    child = store.load(child_id)
    assert (child.definition_version, child.active_path) == (1, "Old")
    runner.process(child_id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.DONE


# --- the CLI ------------------------------------------------------------------------------


def test_harel_hash_prints_each_machines_version_and_digest(tmp_path, capsys):
    path = tmp_path / "order.stm"
    path.write_text(V2)
    assert main(["hash", str(path)]) == 0
    assert capsys.readouterr().out.split() == ["order", "version", "2", "digest", _v(V2).digest]
    assert main(["hash", str(path), "nope"]) == 1


def test_the_monitor_shows_an_execution_its_own_version(tmp_path):
    from harel.tui.resolve import DefinitionSource

    (tmp_path / "order.v1.stm").write_text(V1)
    (tmp_path / "order.v2.stm").write_text(V2)
    source = DefinitionSource.from_dir(str(tmp_path))
    assert source.get("order", version=1).version == 1
    assert source.get("order", version=2).version == 2
    assert "version 2" not in source.source("order", 1) and "version 2" in source.source("order", 2)


# --- several files ---------------------------------------------------------------------------

LIB = """
import "deep.stm"
fragment Approve(done: state) {
  initial Waiting
  state Waiting {}
  from Waiting to done on Ok
}
"""
DEEP = "event Ok {}\nevent Go {}\n"
ORDER = """
import "lib/common.stm" as c
machine order {
  initial Draft
  state Draft {}
  use c.Approve(done = Done) as Review
  final Done success
  from Draft to Review on Go
}
machine payment { initial A final A done }
"""


def _project(tmp_path):
    (tmp_path / "lib").mkdir()
    (tmp_path / "lib" / "common.stm").write_text(LIB)
    (tmp_path / "lib" / "deep.stm").write_text(DEEP)
    (tmp_path / "order.stm").write_text(ORDER)
    return tmp_path / "order.stm"


def test_two_versions_of_a_machine_in_one_file_are_refused():
    with pytest.raises(DslError, match="'order' is declared twice"):
        parse(V1 + V2.replace("event Go {}\nevent Ok {}\n", ""))


def test_where_an_imported_file_is_doesnt_count(tmp_path):
    path = _project(tmp_path)
    before = machine_version(path.read_text(), "order", base_path=tmp_path)
    (tmp_path / "moved").mkdir()
    for f in ("common.stm", "deep.stm"):
        (tmp_path / "moved" / f).write_text((tmp_path / "lib" / f).read_text())
    moved = path.read_text().replace("lib/common.stm", "moved/common.stm")
    assert machine_version(moved, "order", base_path=tmp_path) == before


# --- published versions --------------------------------------------------------------------


def test_a_record_holds_the_file_and_its_imports_and_builds_the_same_machine(tmp_path):
    path = _project(tmp_path)
    order = next(r for r in definition_records(path) if r.definition_id == "order")
    assert order.source["main"] == "order.stm"
    assert sorted(order.source["files"]) == ["lib/common.stm", "lib/deep.stm", "order.stm"]
    assert (order.version, order.digest) == machine_version(ORDER, "order", base_path=tmp_path)
    assert definition_from_record(order).digest == order.digest


def test_a_record_lays_out_imports_from_above_the_file(tmp_path):
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "events.stm").write_text("event Go {}\nevent Ok {}\n")
    (tmp_path / "app").mkdir()
    path = tmp_path / "app" / "order.stm"
    path.write_text('import "../shared/events.stm"\n' + V1.replace("event Go {}\nevent Ok {}\n", ""))
    [record] = definition_records(path)
    assert record.source["main"] == "app/order.stm"
    assert sorted(record.source["files"]) == ["app/order.stm", "shared/events.stm"]
    assert definition_from_record(record).digest == record.digest


def test_a_record_may_not_write_outside_its_bundle(tmp_path):
    [record] = definition_records(_file(tmp_path, V1))
    evil = DefinitionRecord("order", 1, record.digest, {"main": "../x.stm", "files": {"../x.stm": V1}})
    with pytest.raises(ValueError, match="outside its bundle"):
        definition_from_record(evil)


def _file(tmp_path, source, name="order.stm"):
    path = tmp_path / name
    path.write_text(source)
    return path


def test_publishing_stores_a_version_once_and_refuses_other_content(tmp_path):
    store = DictStore()
    publish_definitions(store, definition_records(_file(tmp_path, V1)))
    publish_definitions(store, definition_records(_file(tmp_path, V1)))  # the same again: fine
    assert store.load_definition("order", 1).digest == _v(V1).digest

    edited = V1.replace("on Ok", "on Go") + "\nmachine other { initial A final A done }\n"
    with pytest.raises(DefinitionConflict, match="'order' version 1 is published with other content"):
        publish_definitions(store, definition_records(_file(tmp_path, edited)))
    assert store.load_definition("order", 1).digest == _v(V1).digest
    assert store.load_definition("other", 1) is not None  # the others are published all the same


def test_a_runner_reads_a_version_it_hasnt_loaded_from_the_store(tmp_path):
    store = DictStore()
    old = _review(store, definition_registry([_v(V1)]))
    publish_definitions(store, definition_records(_file(tmp_path, V1)))

    runner = DurableRunner(store, definition_registry([_v(V2)]))  # v1's file is gone from here
    assert runner.process(old.id, Event(kind="Ok")).status is Status.DONE


def test_a_local_copy_edited_without_a_bump_doesnt_reach_executions_started_on_the_published_one(
    tmp_path, caplog
):
    store = DictStore()
    old = _review(store, definition_registry([_v(V1)]))
    publish_definitions(store, definition_records(_file(tmp_path, V1)))

    edited = _v(V1.replace("final Done success", "final Done success\n  state Spare {}"))
    with caplog.at_level(logging.WARNING):
        exe = DurableRunner(store, definition_registry([edited])).process(old.id, Event(kind="Ok"))
    assert exe.status is Status.DONE
    assert "differs from the published one" in caplog.text


EVIL = """
event Go {}
machine order {
  initial A
  state A { on enter os.system }
  final B done
  from A to B on Go
}
"""


def test_a_published_version_may_call_only_the_modules_the_runner_allows(tmp_path):
    store = DictStore()
    exe = DurableRunner(store, definition_registry([_v(V1)])).create("order")
    evil = _v(EVIL)
    store.publish_definition(  # someone with write access to the store
        DefinitionRecord("order", 1, evil.digest, bundle(_file(tmp_path, EVIL)))
    )
    stored = store.load(exe.id)
    stored.definition_digest = evil.digest
    store.save(stored)

    runner = DurableRunner(store, definition_registry([_v(V2)]))
    with pytest.raises(DefinitionNotLoaded, match="calls actions in os, which this runner doesn't allow"):
        runner.process(exe.id, Event(kind="Go"))

    published = PublishedDefinitions(allowed_action_modules([], ["os"], env=False))
    assert published.build(store.load_definition("order", 1)).digest == evil.digest


def test_the_modules_a_runner_allows_come_from_its_own_machines_and_the_environment(monkeypatch):
    defn = _v(
        "event Go {}\nmachine m { initial A state A { on enter harel.lib.reset_backoff } final B done from A to B on Go }",
        "m",
    )
    monkeypatch.setenv("HAREL_ACTION_MODULES", "acme.actions, other ")
    assert allowed_action_modules([defn], ["x.y"]) == {"harel.lib", "acme.actions", "other", "x.y"}
    check_action_modules(defn, frozenset({"harel"}))  # a package allows its modules
    with pytest.raises(PermissionError):
        check_action_modules(defn, frozenset({"harel.li"}))


def test_an_invoked_child_finds_its_machine_through_its_published_parent(tmp_path):
    store = DictStore()
    v1_src = INVOKER.replace("{v}", "1").replace("{first}", "Old")
    v1 = _v(v1_src, "parent")
    exe = DurableRunner(store, definition_registry([v1])).create("parent")
    exe = DurableRunner(store, definition_registry([v1])).process(exe.id, Event(kind="Go"))
    [child_id] = exe.children
    publish_definitions(store, definition_records(_file(tmp_path, v1_src)))

    v2 = _v(INVOKER.replace("{v}", "2").replace("{first}", "New"), "parent")
    runner = DurableRunner(store, definition_registry([v2]))  # v1 only in the store
    runner.process(child_id, Event(kind="Go"))
    assert store.load(exe.id).status is Status.DONE


def test_a_worker_runs_a_version_another_one_published(tmp_path):
    store, transport = DictStore(), InMemoryTransport()
    old = DistributedRunner(store, transport, definition_registry([_v(V1)]), execution="inline")
    publish_definitions(store, definition_records(_file(tmp_path, V1)))
    exe = old.create("order")
    _drain(old.worker())
    old.send(exe.id, Event(kind="Go"))
    old.send(exe.id, Event(kind="Ok"))

    new = DistributedRunner(store, transport, definition_registry([_v(V2)]), execution="inline")
    _drain(new.worker())
    assert store.load(exe.id).status is Status.DONE


def test_harel_publish(tmp_path, monkeypatch, capsys):
    defs = tmp_path / "defs"
    defs.mkdir()
    _file(defs, V1)
    monkeypatch.setenv("HAREL_STORE_BACKEND", "sqlite")
    monkeypatch.setenv("HAREL_STORE_DB", str(tmp_path / "store.db"))
    assert main(["publish", str(defs)]) == 0
    assert capsys.readouterr().out.split()[:3] == ["order", "version", "1"]

    _file(defs, V1.replace("on Ok", "on Go"))
    assert main(["publish", str(defs)]) == 1
    assert "published with other content" in capsys.readouterr().err


def test_the_monitor_shows_a_version_from_the_store(tmp_path):
    from harel.tui.model import MonitorModel

    store = DictStore()
    exe = _review(store, definition_registry([_v(V1)]))
    publish_definitions(store, definition_records(_file(tmp_path, V1)))
    detail = MonitorModel(store).detail(exe.id)  # no definitions of its own
    assert detail.tree.resolved and "machine order" in detail.source


# --- what the code binds, for a version read from the store --------------------------------

BOUND = """
event Go {}
machine order version {v} {
  initial A
  state A {}
  state B { on enter mark }
  final C done
  from A to B on Go where ok
  from B to C
}
"""
MARKED: list = []


def mark(stm, event):
    MARKED.append(event.kind)


_ACTIONS = {"mark": mark}
_GUARDS = {"ok": {"fine__eq": True}}


def _bound(v):
    return definition_from_dsl(BOUND.replace("{v}", str(v)), "order", actions=_ACTIONS, guards=_GUARDS)


def _bound_v1_in_store(tmp_path):
    store = DictStore()
    exe = DurableRunner(store, definition_registry([_bound(1)]), actions=_ACTIONS, guards=_GUARDS).create(
        "order"
    )
    publish_definitions(store, definition_records(_file(tmp_path, BOUND.replace("{v}", "1"))))
    return store, exe


def test_a_version_from_the_store_binds_the_runners_actions_and_guards(tmp_path):
    store, exe = _bound_v1_in_store(tmp_path)
    MARKED.clear()
    runner = DurableRunner(store, definition_registry([_bound(2)]), actions=_ACTIONS, guards=_GUARDS)
    exe = runner.process(exe.id, Event(kind="Go", data={"fine": True}))
    assert (exe.status, MARKED) == (Status.DONE, ["Go"])


def test_without_the_bindings_it_cant_be_built(tmp_path):
    store, exe = _bound_v1_in_store(tmp_path)
    runner = DurableRunner(store, definition_registry([_bound(2)]))
    with pytest.raises(DefinitionNotLoaded, match="unbound action handlers: mark"):
        runner.process(exe.id, Event(kind="Go", data={"fine": True}))


def test_a_worker_binds_them_too(tmp_path):
    store, transport = DictStore(), InMemoryTransport()
    old = DistributedRunner(
        store,
        transport,
        definition_registry([_bound(1)]),
        execution="inline",
        actions=_ACTIONS,
        guards=_GUARDS,
    )
    publish_definitions(store, definition_records(_file(tmp_path, BOUND.replace("{v}", "1"))))
    exe = old.create("order")
    _drain(old.worker())
    old.send(exe.id, Event(kind="Go", data={"fine": True}))

    MARKED.clear()
    new = DistributedRunner(
        store,
        transport,
        definition_registry([_bound(2)]),
        execution="inline",
        actions=_ACTIONS,
        guards=_GUARDS,
    )
    _drain(new.worker())
    assert (store.load(exe.id).status, MARKED) == (Status.DONE, ["Go"])


def test_a_path_the_runner_binds_is_allowed():
    published = PublishedDefinitions(frozenset(), actions={"mark": "harel.lib.reset_backoff"})
    assert "harel.lib" in published.allowed


def test_a_version_whose_actions_this_process_cant_import_is_left_for_another(tmp_path):
    """A rolling deploy: an older worker meets a version a newer one published, calling an
    action its code doesn't have yet. It must leave it (retry), not run it and fail."""
    newer = BOUND.replace("{v}", "2").replace("on enter mark", "on enter harel.lib.not_there_yet")
    store = DictStore()
    exe = DurableRunner(store, definition_registry([_bound(1)]), actions=_ACTIONS, guards=_GUARDS).create(
        "order", version=1
    )
    stored = store.load(exe.id)
    stored.definition_version, stored.definition_digest = machine_version(newer, "order")
    store.save(stored)
    publish_definitions(store, definition_records(_file(tmp_path, newer)))

    runner = DurableRunner(
        store, definition_registry([_bound(1)]), guards=_GUARDS, action_modules=["harel.lib"]
    )
    with pytest.raises(DefinitionNotLoaded, match="harel.lib.not_there_yet, which this process can't import"):
        runner.process(exe.id, Event(kind="Go", data={"fine": True}))
    assert store.load(exe.id).status is Status.RUNNING
