"""Seed a SQLite store with a spread of executions so you can drive the monitor TUI
against real-looking data (different statuses, a hierarchy, an orthogonal join, a timer,
rich context). Run it, then launch the monitor against the same DB:

    uv run python examples/monitor_demo/seed.py /tmp/harel-demo.db
    STM_STORE_BACKEND=sqlite STM_STORE_DB=/tmp/harel-demo.db \
        uv run harel monitor --definitions-dir examples/monitor_demo/machines

(or `python -m harel.tui` with the same env). Re-running reseeds a fresh DB.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from harel.dsl import definition_from_dsl_file
from harel.engine.execution import ChildState, Execution, Status
from harel.engine.store import SqliteStore, TimerOp

HERE = Path(__file__).parent
MACHINES = HERE / "machines"


def _path(defn, name: str) -> str:
    """The full_path of the state named `name` in `defn`."""
    return next(p for p, n in defn.index.items() if n.name == name)


def _step(idx, event, frm, to, cin, cout, *, actions=(), guards=(), note=""):
    """Build one trace-step dict for the PREVIEW timeline (see harel.tui.trace)."""
    return {
        "index": idx,
        "event_kind": event,
        "from_path": frm,
        "to_path": to,
        "context_in": cin,
        "context_out": cout,
        "actions": list(actions),
        "guards": list(guards),
        "note": note,
    }


def main() -> None:
    db = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/harel-demo.db")
    if db.exists():
        db.unlink()  # fresh DB each run
    order = definition_from_dsl_file(MACHINES / "order.stm", "Order")
    fulfillment = definition_from_dsl_file(MACHINES / "fulfillment.stm", "Fulfillment")
    store = SqliteStore(db)
    now = time.time()

    # --- a spread of Order executions in different lifecycle states ----------------
    store.save(
        Execution(
            id="order-running-cart",
            definition_id="Order",
            status=Status.RUNNING,
            active_path=_path(order, "Cart"),
            context={"user": "ana", "cart_items": 3, "currency": "EUR"},
        )
    )
    # one parked on a sub-state of the composite, with a durable timer armed
    pay = Execution(
        id="order-awaiting-payment",
        definition_id="Order",
        status=Status.RUNNING,
        active_path=_path(order, "Payment"),
        context={"user": "bruno", "total": 49.90, "payment_attempts": 1},
    )
    store.commit(pay, [], timers=(TimerOp("schedule", _path(order, "Payment"), fire_at=now + 300),))
    cart, payment = _path(order, "Cart"), _path(order, "Payment")
    store.append_trace(
        pay.id,
        _step(
            0,
            "Checkout",
            cart,
            payment,
            {"user": "bruno", "cart_items": 2},
            {"user": "bruno", "total": 49.90, "payment_attempts": 1},
            actions=["reserve_inventory", "open_payment"],
            guards=["cart_not_empty"],
        ),
    )
    store.save(
        Execution(
            id="order-suspended",
            definition_id="Order",
            status=Status.SUSPENDED,
            active_path=_path(order, "Shipped"),
            context={"user": "carla", "tracking": "TRK-99812"},
        )
    )
    store.save(
        Execution(
            id="order-done",
            definition_id="Order",
            status=Status.DONE,
            outcome="success",
            active_path=_path(order, "Delivered"),
            context={"user": "dario", "delivered_at": "2026-06-04T11:02:00Z"},
        )
    )
    confirm, shipped, delivered = (_path(order, n) for n in ("Confirm", "Shipped", "Delivered"))
    for s in (
        _step(
            0,
            "Checkout",
            cart,
            payment,
            {"user": "dario", "cart_items": 1},
            {"user": "dario", "total": 19.0},
            actions=["reserve_inventory"],
        ),
        _step(
            1,
            "Paid",
            payment,
            confirm,
            {"total": 19.0},
            {"total": 19.0, "charge_id": "ch_77"},
            actions=["charge_card"],
            guards=["funds_ok"],
        ),
        _step(
            2,
            "Ship",
            confirm,
            shipped,
            {"charge_id": "ch_77"},
            {"charge_id": "ch_77", "tracking": "TRK-7"},
            actions=["create_shipment"],
        ),
        _step(
            3,
            "Deliver",
            shipped,
            delivered,
            {"tracking": "TRK-7"},
            {"tracking": "TRK-7", "delivered_at": "2026-06-04T11:02:00Z"},
            actions=["notify_customer"],
        ),
    ):
        store.append_trace("order-done", s)
    store.save(
        Execution(
            id="order-failed",
            definition_id="Order",
            status=Status.FAILED,
            error="PaymentError: card declined (code 51)",
            active_path=_path(order, "Payment"),
            context={"user": "elena", "total": 120.0, "payment_attempts": 3},
        )
    )
    for s in (
        _step(
            0,
            "Checkout",
            cart,
            payment,
            {"user": "elena", "cart_items": 5},
            {"user": "elena", "total": 120.0},
            actions=["reserve_inventory"],
        ),
        _step(
            1,
            "Paid",
            payment,
            payment,
            {"total": 120.0, "payment_attempts": 2},
            {"total": 120.0, "payment_attempts": 3},
            actions=["charge_card"],
            guards=["funds_ok"],
            note="charge raised PaymentError: card declined (code 51) — execution FAILED",
        ),
    ):
        store.append_trace("order-failed", s)

    # --- an orthogonal Fulfillment mid-join: Picking done, Billing still running ----
    store.save(
        Execution(
            id="fulfillment-joining",
            definition_id="Fulfillment",
            status=Status.RUNNING,
            active_path=_path(fulfillment, "Fork"),
            context={"order": "order-awaiting-payment", "warehouse": "MAD-1"},
            children={
                "Fork.Picking": ChildState(
                    root_path=_path(fulfillment, "Picking"), finished=True, outcome="success"
                ),
                "Fork.Billing": ChildState(root_path=_path(fulfillment, "Billing"), finished=False),
            },
        )
    )
    # an unknown-definition execution (definition not in the machines dir) -> the
    # monitor shows it data-only, proving graceful degradation
    store.save(
        Execution(
            id="legacy-unknown-def",
            definition_id="LegacyJob",
            status=Status.RUNNING,
            active_path="Step.Inner",
            context={"note": "definition not in --definitions-dir; renders data-only"},
        )
    )
    store.close()

    print(f"seeded {db}")
    print("launch the monitor with:")
    print(f"  STM_STORE_BACKEND=sqlite STM_STORE_DB={db} uv run harel monitor --definitions-dir {MACHINES}")


if __name__ == "__main__":
    main()
