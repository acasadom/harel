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


def test_a_trace_step_shows_what_a_set_wrote():
    from harel.tui.trace import TraceStep
    from harel.tui.widgets import step_markup

    step = TraceStep.from_dict(
        {"index": 1, "event_kind": "Fail", "from_path": "A", "to_path": "A", "assigned": {"attempts": 2}}
    )
    assert step.assigned == {"attempts": 2}
    assert "context.attempts = 2" in step_markup(step)
    assert "[b]set[/]" not in step_markup(
        TraceStep.from_dict({"index": 0, "event_kind": "Start", "from_path": None, "to_path": "A"})
    )


def test_redrive_brings_a_failed_execution_back_at_a_leaf():
    store = DictStore()
    b = next(p for p, n in DEFN.index.items() if n.name == "B")
    store.save(Execution(id="e1", definition_id="M", status=Status.FAILED, active_path=b, error="x"))
    model = _model(store)
    assert model.can_redrive("e1")
    model.redrive("e1", b)
    assert store.load("e1").status is Status.RUNNING
    assert not model.can_redrive("e1")


# --- the queue view ----------------------------------------------------------------------


def _queued(now):
    from harel.engine.store import WorkerRecord
    from harel.engine.transport import InMemoryTransport

    store, transport = DictStore(), InMemoryTransport(clock=lambda: now - 60)
    for i in range(3):
        transport.publish(f"o{i}", Event(kind="Go"), routing_key="order")
    transport.publish("p0", Event(kind="Go"), routing_key="payment")
    store.record_worker(WorkerRecord("pay", ("payment", "refunds"), now - 100, now - 1, 10.0))
    store.record_worker(WorkerRecord("old", None, now - 9000, now - 900, 10.0))  # gone
    return store, transport


def test_the_queue_view_counts_by_machine_and_names_the_live_workers_claiming_it():
    now = 10_000.0
    store, transport = _queued(now)
    view = MonitorModel(store, DefinitionSource(), transport).queues(now)
    rows = {r.machine: r for r in view.rows}
    assert (rows["order"].messages, rows["order"].executions, rows["order"].oldest_published_at) == (
        3,
        3,
        now - 60,
    )
    assert rows["order"].workers == () and rows["order"].unserved  # "old" is gone
    assert rows["payment"].workers == ("pay",) and not rows["payment"].unserved
    assert (rows["refunds"].messages, rows["refunds"].workers) == (
        0,
        ("pay",),
    )  # a dedicated worker's capacity
    assert [w.worker_id for w in view.workers] == ["old", "pay"]


def test_a_queue_view_without_a_transport_shows_the_workers_and_why():
    now = 10_000.0
    store, _ = _queued(now)
    view = MonitorModel(store, DefinitionSource(), transport_note="no transport (HAREL_REDIS_URL)").queues(
        now
    )
    assert view.rows == [] and "HAREL_REDIS_URL" in view.unavailable and len(view.workers) == 2


def test_a_transport_that_counts_its_whole_queue_is_served_by_any_live_worker():
    from harel.engine.transport import Backlog, MachineBacklog

    class _Total:
        def backlog(self):
            return Backlog([MachineBacklog(None, 7)], by_machine=False)

    now = 10_000.0
    store, _ = _queued(now)
    view = MonitorModel(store, DefinitionSource(), _Total()).queues(now)
    assert not view.by_machine
    [row] = view.rows
    assert (row.machine, row.messages, row.workers) == (None, 7, ("pay",))
