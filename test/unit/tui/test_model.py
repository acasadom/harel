"""MonitorModel — the read facade over a store + DefinitionSource."""

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution, Status
from harel.engine.store import DictStore, TimerOp
from harel.spec.states import Event
from harel.tui.model import MonitorModel
from harel.tui.resolve import DefinitionSource

DEFN = definition_from_dsl("machine M { initial A  state A {}  state B {}  from A to B on Go }", "M")


def _model(store, *, resolvable=True):
    source = DefinitionSource(registry={"M": DEFN} if resolvable else {})
    return MonitorModel(store, source)


def test_detail_collects_scoped_pending_work():
    store = DictStore()
    a_path = next(p for p, n in DEFN.index.items() if n.name == "A")
    exe = Execution(id="e1", definition_id="M", active_path=a_path)
    other = Execution(id="e2", definition_id="M")
    store.commit(other, [(other.id, Event(kind="Other"))])  # noise scoped to e2
    store.commit(
        exe,
        [(exe.id, Event(kind="Ping"))],  # inbound to e1
        timers=(TimerOp("schedule", a_path, fire_at=123.0),),
        spawns=(("e1:child:0", "A", {}),),
    )

    detail = _model(store).detail("e1")
    assert detail is not None
    assert detail.execution.id == "e1"
    assert detail.tree.resolved and detail.tree.active_path == a_path
    assert detail.timers == [(a_path, 123.0)]
    assert [e.target_id for e in detail.inbound] == ["e1"]  # e2's outbox excluded
    assert [s.child_id for s in detail.spawns] == ["e1:child:0"]


def test_detail_missing_execution_returns_none():
    assert _model(DictStore()).detail("ghost") is None


def test_detail_unresolved_definition_is_data_only():
    store = DictStore()
    store.save(Execution(id="e1", definition_id="M", active_path="A"))
    detail = _model(store, resolvable=False).detail("e1")
    assert detail is not None and detail.tree.resolved is False and detail.tree.root is None


def test_list_executions_delegates_with_filters():
    store = DictStore()
    for i in range(4):
        store.save(
            Execution(id=f"e{i}", definition_id="M", status=Status.RUNNING if i % 2 == 0 else Status.DONE)
        )
    page = _model(store).list_executions(status=[Status.RUNNING])
    assert sorted(s.id for s in page.items) == ["e0", "e2"]
