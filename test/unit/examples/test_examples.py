"""The examples under examples/ run and reach the outcomes they describe — so they keep up
with the engine. Each runs headless: no NiceGUI, FastAPI or server (the wizard has its own
spec in test_wizard.py)."""

from pathlib import Path

import pytest

from harel import DictStore, Event, definition_from_dsl_file
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.engine.store import SqliteStore
from harel.engine.transport import InMemoryTransport


def test_minimal(capsys):
    from examples.minimal.run import main

    main()
    assert capsys.readouterr().out.splitlines()[-1] == "status: DONE  outcome: success"


@pytest.mark.parametrize(
    "name,expected",
    [
        ("happy path (express)", ("Delivered", "success", "express courier booked")),
        ("international parcel (standard)", ("Delivered", "success", "standard carrier booked")),
        ("payment retried, then paid", ("Delivered", "success", "payment declined (insufficient funds)")),
        (
            "payment keeps failing -> cancelled",
            ("Cancelled", "cancelled", "order cancelled after 2 declines"),
        ),
        ("cancelled while awaiting payment", ("Cancelled", "cancelled", "order cancelled")),
        ("capture_payment raises -> PaymentError", ("PaymentError", "failed", "payment error: RuntimeError")),
        ("pick raises -> FulfilmentError", ("FulfilmentError", "failed", "fulfilment error: OSError")),
    ],
)
def test_place_order(name, expected):
    from examples.place_order.run import ORDER_STM, SCENARIOS, run_scenario

    defn = definition_from_dsl_file(ORDER_STM, "order", validate=True)
    events, context = next((e, c) for n, e, c in SCENARIOS if n == name)
    exe = run_scenario(defn, name, events, context)
    path, outcome, in_history = expected
    assert (exe.active_path, exe.status, exe.outcome) == (path, Status.DONE, outcome)
    assert any(step.startswith(in_history) for step in exe.context["history"])


# --- webhook_payment: its machine on the distributed runner, without the HTTP layer ------
@pytest.fixture
def payment():
    stm = Path(__file__).parents[3] / "examples" / "webhook_payment" / "payment.stm"
    return definition_from_dsl_file(stm, "payment", validate=True)


def _drive(defn, *, events=(), wait=0.0):
    now = [1000.0]
    store = DictStore()
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn}, clock=lambda: now[0])
    worker = runner.worker(clock=lambda: now[0])
    exe = runner.create(defn.id)
    while worker.step():
        pass
    for event in events:
        runner.send(exe.id, event)
    now[0] += wait
    worker.fire_due_timers()
    while worker.step():
        pass
    return store.load(exe.id)


def _succeeded(event_id="evt_1"):
    return Event(kind="PaymentSucceeded", id=event_id, data={"payment_id": "pi_1", "amount": 100})


@pytest.mark.parametrize(
    "events,wait,expected",
    [
        ([_succeeded()], 0, ("Done", "success")),
        ([Event(kind="PaymentFailed", data={"reason": "card_declined"})], 0, ("Failed", "failed")),
        ([], 20, ("Abandoned", "abandoned")),  # the 15 s timeout fires
        ([_succeeded(), _succeeded()], 0, ("Done", "success")),  # a redelivered webhook: no-op
    ],
)
def test_webhook_payment(payment, events, wait, expected):
    exe = _drive(payment, events=events, wait=wait)
    assert (exe.active_path, exe.outcome) == expected
    if len(events) == 2:
        assert exe.context["log"].count("order complete") == 1


def test_webhook_payment_routes_a_failing_fulfillment_to_svc_error(payment, monkeypatch):
    from examples.webhook_payment import actions

    def down(stm, event, **kw):
        raise ConnectionError("fulfillment service unavailable")

    monkeypatch.setattr(actions, "start_fulfillment", down)
    exe = _drive(payment, events=[_succeeded()])
    assert (exe.active_path, exe.outcome) == ("SvcError", "failed")


def test_monitor_demo_seed(tmp_path):
    from examples.monitor_demo.seed import seed

    db = tmp_path / "demo.db"
    seed(db)
    store = SqliteStore(db)
    try:
        status = {s.id: (s.status, s.active_path) for s in store.list_executions(limit=100).items}
        assert status["order-in-cart"] == (Status.RUNNING, "Cart")
        assert status["order-awaiting-payment"] == (Status.RUNNING, "Checkout.Payment")
        assert status["order-suspended"] == (Status.SUSPENDED, "Shipped")
        assert status["order-delivered"] == (Status.DONE, "Delivered")
        assert status["order-failed"][0] is Status.FAILED
        assert status["fulfillment-joining"] == (Status.RUNNING, "Fork")
        assert status["fulfillment-cancel-on-failure"] == (Status.DONE, "Failed")
        assert status["fulfillment-cancel-on-failure:Fork.Billing:0"][0] is Status.CANCELLED
        assert len(store.read_trace("order-delivered")) == 5
        assert any(step.get("assigned") for step in store.read_trace("order-delivered"))
    finally:
        store.close()


# --- inline_transaction: the machine's advance in the shop's own transaction ----------------
def test_inline_transaction_commits_and_rolls_back_with_the_shop():
    from examples.inline_transaction import actions
    from examples.inline_transaction.run import open_shop, pay, place_order

    conn, runner = open_shop()

    def state(order_id):
        row = conn.execute("SELECT status FROM orders WHERE id = ?", (order_id,)).fetchone()
        exe = runner.store.load(order_id)
        (stock,) = conn.execute("SELECT available FROM stock WHERE item = 'widget'").fetchone()
        return (row[0] if row else None, exe.active_path if exe else None, stock)

    place_order(conn, runner, "o1", "widget", 2)
    assert state("o1") == ("placed", "AwaitingPayment", 3)
    with pytest.raises(actions.OutOfStock):
        place_order(conn, runner, "o2", "widget", 9)
    assert state("o2") == (None, None, 3)  # the row, the stock and the execution all rolled back
    pay(conn, runner, "o1")
    assert state("o1") == ("paid", "Paid", 3)


def test_inline_transaction_store_meets_the_contracts():
    import sqlite3

    from examples.inline_transaction.store import ConnectionStore
    from harel.engine.schema import store_schema
    from harel.testing import assert_listing_contract, assert_outbox_contract, assert_purge_contract

    conn = sqlite3.connect(":memory:")
    for statement in store_schema("sqlite"):
        conn.execute(statement)
    store = ConnectionStore(conn)
    assert_listing_contract(store, ordered=True)
    assert_purge_contract(store)
    assert_outbox_contract(store)
