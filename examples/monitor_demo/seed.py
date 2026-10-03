"""Seed a SQLite store with a spread of executions so you can drive the monitor TUI
against real data: different statuses, a hierarchy, an orthogonal join, a durable timer,
a dead letter, with their execution traces. Every execution is produced by running the
machines in `machines/` with the headless runner (tracing on), on a clock set a little in
the past so the timestamps differ. Run it, then launch the monitor on the same DB:

    uv run python -m examples.monitor_demo.seed /tmp/harel-demo.db
    STM_STORE_BACKEND=sqlite STM_STORE_DB=/tmp/harel-demo.db \
        uv run harel monitor --definitions-dir examples/monitor_demo/machines

(or `python -m harel.tui` with the same env). Re-running reseeds a fresh DB.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from harel import DurableRunner, Event
from harel.dsl import definition_from_dsl_file
from harel.engine.execution import Execution, Status
from harel.engine.store import SqliteStore

MACHINES = Path(__file__).parent / "machines"


class _Clock:
    """A clock that starts an hour ago and moves a few seconds per step."""

    def __init__(self) -> None:
        self.now = time.time() - 3600

    def __call__(self) -> float:
        self.now += 7
        return self.now


def seed(db: Path) -> None:
    for leftover in db.parent.glob(db.name + "*"):  # fresh DB each run (with its -wal/-shm)
        leftover.unlink()
    order = definition_from_dsl_file(MACHINES / "order.stm", "Order", validate=True)
    fulfillment = definition_from_dsl_file(MACHINES / "fulfillment.stm", "Fulfillment", validate=True)
    store = SqliteStore(db)
    runner = DurableRunner(store, {order.id: order, fulfillment.id: fulfillment}, clock=_Clock(), trace=True)

    def new_order(eid: str, user: str, total: float, *events: Event, **extra) -> str:
        runner.create(order.id, execution_id=eid, context={"user": user, "total": total, **extra})
        for event in events:
            runner.process(eid, event)
        return eid

    # --- Order executions in different lifecycle states ----------------------------
    new_order("order-in-cart", "ana", 35.0)
    # parked on a sub-state of the composite, with its durable timer armed
    new_order("order-awaiting-payment", "bruno", 49.9, Event(kind="Checkout"))
    shipped = new_order(
        "order-suspended",
        "carla",
        12.5,
        Event(kind="Checkout"),
        Event(kind="Paid", data={"charge_id": "ch_31"}),
        Event(kind="Ship", data={"tracking": "TRK-99812"}),
    )
    runner.suspend(shipped)
    new_order(
        "order-delivered",
        "dario",
        19.0,
        Event(kind="Checkout"),
        Event(kind="Paid", data={"charge_id": "ch_77"}),
        Event(kind="Ship", data={"tracking": "TRK-7"}),
        Event(kind="Deliver"),
    )
    # the charge raises and nothing models the error: the runner dead-letters it
    new_order(
        "order-failed",
        "elena",
        120.0,
        Event(kind="Checkout"),
        Event(kind="Paid", data={"charge_id": "ch_12"}),
        decline=True,
    )

    # --- orthogonal Fulfillment: one mid-join, one ended by cancel_on_failure -------
    runner.create(fulfillment.id, execution_id="fulfillment-joining", context={"warehouse": "MAD-1"})
    runner.process("fulfillment-joining", Event(kind="Picked"))  # Billing still running
    runner.create(
        fulfillment.id, execution_id="fulfillment-cancel-on-failure", context={"warehouse": "MAD-1"}
    )
    runner.process("fulfillment-cancel-on-failure", Event(kind="PickFailed"))  # cancels Billing

    # an execution of a definition the monitor isn't given -> shown data-only, proving
    # graceful degradation (written directly: there is no machine to run)
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


def main() -> None:
    db = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/harel-demo.db")
    seed(db)
    print(f"seeded {db}")
    print("launch the monitor with:")
    print(f"  STM_STORE_BACKEND=sqlite STM_STORE_DB={db} uv run harel monitor --definitions-dir {MACHINES}")


if __name__ == "__main__":
    main()
