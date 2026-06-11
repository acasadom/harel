"""Execution trace (opt-in timeline): the Driver records one step per event in the commit
txn; `load` is unaffected. Covers the in-process backends (dict, sqlite, libsql); the
networked SQL backends share the same code and are exercised by their integration tests."""

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution
from harel.engine.runtime import Driver
from harel.engine.store import DictStore, SqliteStore
from harel.spec.states import Event

PING_PONG = """
machine M {
  initial A
  state A {}
  state B {}
  from A to B on Go
  from B to A on Go
}
"""

WITH_ACTION = """
machine M {
  initial A
  state A {}
  state B { on enter stm_actions.rec(at: "B") }
  final Done success
  from A to B on Go
  from B to Done on Go
}
"""


def _sqlite():
    return SqliteStore(":memory:")


def _libsql():
    pytest.importorskip("libsql")
    from harel.engine.store import LibsqlStore

    return LibsqlStore(":memory:")


STORES = [DictStore, _sqlite, _libsql]
STORE_IDS = ["dict", "sqlite", "libsql"]


@pytest.mark.parametrize("make_store", STORES, ids=STORE_IDS)
def test_trace_records_transitions(make_store):
    store = make_store()
    defn = definition_from_dsl(PING_PONG, "M")
    driver = Driver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id)
    driver.start(exe)
    driver.inject(exe, Event(kind="Go"))
    driver.inject(exe, Event(kind="Go"))
    trace = store.read_trace(exe.id)
    assert [s["index"] for s in trace] == [0, 1, 2]
    assert [s["event_kind"] for s in trace] == ["Start", "Go", "Go"]
    assert [(s["from_path"], s["to_path"]) for s in trace] == [(None, "A"), ("A", "B"), ("B", "A")]
    store.close()


@pytest.mark.parametrize("make_store", STORES, ids=STORE_IDS)
def test_trace_default_off_writes_nothing(make_store):
    store = make_store()
    defn = definition_from_dsl(PING_PONG, "M")
    driver = Driver(defn, store)  # trace not enabled
    exe = Execution(definition_id=defn.id)
    driver.start(exe)
    driver.inject(exe, Event(kind="Go"))
    assert store.read_trace(exe.id) == []
    store.close()


@pytest.mark.parametrize("make_store", STORES, ids=STORE_IDS)
def test_trace_ring_cap_keeps_last_n(make_store):
    store = make_store()
    store.trace_max = 3
    defn = definition_from_dsl(PING_PONG, "M")
    driver = Driver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id)
    driver.start(exe)  # index 0
    for _ in range(5):  # indices 1..5
        driver.inject(exe, Event(kind="Go"))
    trace = store.read_trace(exe.id)
    assert [s["index"] for s in trace] == [3, 4, 5]  # only the last 3, indices stay monotonic
    store.close()


def test_trace_captures_actions_and_context_out():
    store = SqliteStore(":memory:")
    defn = definition_from_dsl(WITH_ACTION, "M")
    driver = Driver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id, context={"trace": []})
    driver.start(exe)
    driver.inject(exe, Event(kind="Go"))  # enters B, runs rec(at="B")
    trace = store.read_trace(exe.id)
    go_step = trace[1]
    assert go_step["actions"] == ["stm_actions.rec"]
    assert go_step["context_out"]["trace"] == ["B"]  # the action's effect is in context_out
    store.close()


def test_load_is_unaffected_by_trace():
    """The snapshot path is independent: load returns the Execution, not a trace replay."""
    store = SqliteStore(":memory:")
    defn = definition_from_dsl(PING_PONG, "M")
    driver = Driver(defn, store, trace=True)
    exe = Execution(definition_id=defn.id)
    driver.start(exe)
    driver.inject(exe, Event(kind="Go"))
    loaded = store.load(exe.id)
    assert loaded is not None and loaded.active_path == "B"
    store.close()
