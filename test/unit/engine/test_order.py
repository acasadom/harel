"""Behavioural drive of `test/data/order.stm` — the full-feature showcase.

The actions are no-ops in `order_actions`; only the state reached after each event
matters. Proves the machine drives correctly: declared events, verdict terminals, and
the error/fail guards split by event family (a single guard cannot AND across the
status/result and carrier_status vocabularies — under strict missing-field semantics it
would never fire).
"""

from pathlib import Path

from harel.dsl import definition_from_dsl_file
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore
from harel.spec.states import Event

DATA = Path(__file__).parents[2] / "data"


def _runner():
    defn = definition_from_dsl_file(DATA / "order.stm", "order")
    store = DictStore()
    return DurableRunner(store, {defn.id: defn}), store, defn.id


def _drive(events):
    """Create the order, deliver each event, return the execution reached."""
    runner, store, mid = _runner()
    exe = runner.create(mid)
    for ev in events:
        runner.process(exe.id, ev)
    return store.load(exe.id)


def _inventory(**data):
    return Event(kind="InventoryResult", data=data)


def test_happy_path_runs_to_a_delivered_success():
    runner, store, mid = _runner()
    exe = runner.create(mid)

    def active():
        return store.load(exe.id).active_path

    assert active() == "Processing.Validating"  # Received is transient (auto)
    runner.process(exe.id, _inventory(status="started"))
    assert active() == "Processing.Reserving Stock"
    runner.process(exe.id, _inventory(status="done", result="reserved"))
    assert active() == "Processing.Charging"  # Authorizing is transient (auto)
    runner.process(exe.id, Event(kind="PaymentResult", data={"status": "done", "result": "captured"}))
    assert active() == "Processing.Packing"
    runner.process(exe.id, Event(kind="PackingResult", data={"status": "done", "result": "packed"}))
    # needs_carrier -> Shipping; the carrier confirms -> parks at the inner sink
    assert active() == "Processing.Shipping.Carrier Confirmed"
    runner.process(exe.id, Event(kind="CarrierUpdate", data={"carrier_status": "Confirmed"}))

    final = store.load(exe.id)
    assert final.active_path == "Delivered"
    assert final.status is Status.DONE
    assert final.outcome == "success"


def test_status_result_notification_routes_to_rejected():
    # a status/result notification reporting failure bubbles up to Processing -> Rejected
    final = _drive([_inventory(status="ERROR", result="failed")])
    assert final.active_path == "Rejected"
    assert final.outcome == "errored"


def test_carrier_update_error_routes_to_rejected():
    # the carrier update carries carrier_status, not status/result
    final = _drive(
        [
            _inventory(status="started"),
            _inventory(status="done", result="reserved"),
            Event(kind="PaymentResult", data={"status": "done", "result": "captured"}),
            Event(kind="PackingResult", data={"status": "done", "result": "packed"}),
            Event(kind="CarrierUpdate", data={"carrier_status": "ERROR"}),
        ]
    )
    assert final.active_path == "Rejected"
    assert final.outcome == "errored"


def test_carrier_update_fail_routes_to_failed():
    final = _drive(
        [
            _inventory(status="started"),
            _inventory(status="done", result="reserved"),
            Event(kind="PaymentResult", data={"status": "done", "result": "captured"}),
            Event(kind="PackingResult", data={"status": "done", "result": "packed"}),
            Event(kind="CarrierUpdate", data={"carrier_status": "FAIL"}),
        ]
    )
    assert final.active_path == "Failed"
    assert final.outcome == "failed"


def test_cancel_routes_to_a_cancelled_verdict():
    final = _drive([Event(kind="Cancel")])
    assert final.active_path == "Cancelled"
    assert final.outcome == "cancelled"
