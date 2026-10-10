# monitor_demo — data for the monitor

## What it does

Seeds a SQLite store with executions in every state the [monitor](../guide/monitor.md) can
show, then points the monitor at it. Every execution is produced by running real machines —
none is written by hand, except one whose machine is deliberately missing:

- orders in the cart, waiting for payment on a sub-state with a durable timer armed, suspended,
  delivered, and one dead-lettered by a card charge that raises;
- an orthogonal fulfilment mid-join, and one ended by `cancel_on_failure`, its other region
  cancelled;
- an execution of a definition the monitor isn't given, to show it degrades to data-only.

Each carries its execution trace, so the monitor's timeline has steps to show.

## The machines

An order, with a composite `Checkout` and a timeout on its `Payment` step:

<!-- machine: examples/monitor_demo/machines/order.stm Order -->
```{mermaid}
stateDiagram-v2
[*] --> Cart
state "Checkout<br/>on enter: reserve_inventory" as Checkout {
  [*] --> Checkout_Payment
  state "Payment" as Checkout_Payment
  Checkout_Payment : timeout#58; 300
  state "Confirm" as Checkout_Confirm
  Checkout_Confirm : on enter#58; charge_card
  Checkout_Payment --> Checkout_Confirm : Paid<br/>/ context.charge_id = event.charge_id
  Checkout_Confirm --> [*]
}
state "Delivered" as Delivered
Delivered : on enter#58; notify_customer
Delivered : outcome#58; success
Cart --> Checkout : Checkout
Checkout --> Shipped : Ship<br/>/ context.tracking = event.tracking
Shipped --> Delivered : Deliver
Delivered --> [*]
Checkout_Payment --> Cart : Timeout
```

A fulfilment, picking and billing in parallel regions, with `cancel_on_failure`:

<!-- machine: examples/monitor_demo/machines/fulfillment.stm Fulfillment -->
```{mermaid}
stateDiagram-v2
[*] --> Fork
state Fork {
  state "Picking" as Fork_Picking {
    [*] --> Fork_Picking_P1
    state "P1" as Fork_Picking_P1
    state "P2" as Fork_Picking_P2
    Fork_Picking_P2 : outcome#58; success
    state "P3" as Fork_Picking_P3
    Fork_Picking_P3 : outcome#58; failed
    Fork_Picking_P1 --> Fork_Picking_P2 : Picked
    Fork_Picking_P1 --> Fork_Picking_P3 : PickFailed
    Fork_Picking_P2 --> [*]
    Fork_Picking_P3 --> [*]
  }
  --
  state "Billing" as Fork_Billing {
    [*] --> Fork_Billing_B1
    state "B1" as Fork_Billing_B1
    state "B2" as Fork_Billing_B2
    Fork_Billing_B2 : outcome#58; success
    Fork_Billing_B1 --> Fork_Billing_B2 : Billed
    Fork_Billing_B2 --> [*]
  }
}
state "Done" as Done
Done : outcome#58; success
state "Failed" as Failed
Failed : outcome#58; failed
state Fork__join_success <<choice>>
Fork --> Fork__join_success
Fork__join_success --> Done : join_success=pass
Fork__join_success --> Failed : else
Done --> [*]
Failed --> [*]
```

## How it is built

`seed.py` builds one `DurableRunner` over a `SqliteStore` with `trace=True` and an injected
clock that starts an hour ago and moves a few seconds per step, so the timestamps differ. It
creates each execution with a chosen id and feeds it events (`runner.suspend(...)` for the
suspended one); `actions.py`'s `charge_card` raises when the order's context says `decline`,
leaving a real dead letter.

```{mermaid}
sequenceDiagram
  participant Seed as seed.py
  participant R as DurableRunner (trace on)
  participant S as SqliteStore
  participant M as harel monitor
  Seed->>R: create(Order, "order-delivered") + Checkout, Paid, Ship, Deliver
  R->>S: commit each step, with its trace step
  Seed->>R: create(Order, "order-failed", decline) + Checkout, Paid
  R->>S: commit: FAILED (the charge raised, and nothing models it)
  Note over Seed,S: and the other executions, the same way
  M->>S: list_executions, load, read_trace
```

## How it runs

- **Semantics: synchronous** — `DurableRunner`: each step is committed before the next.
- **Execution model: background**, the default: a one-off script.
- **The trace** is opt-in (`trace=True`): one timeline step per event, written in the same
  commit as the step.

## Run it

```text
uv run python -m examples.monitor_demo.seed /tmp/harel-demo.db
HAREL_STORE_BACKEND=sqlite HAREL_STORE_DB=/tmp/harel-demo.db \
    uv run harel monitor --definitions-dir examples/monitor_demo/machines
```
