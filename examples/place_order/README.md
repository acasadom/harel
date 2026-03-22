# Place order — example

The canonical e-commerce order lifecycle (the DDD "place order" example) modelled
as an `harel` statechart. It's a runnable demo, not a test.

```bash
uv run python -m examples.place_order.run
```

What it shows:

- **Declarative definition** (`order.stm`) compiled by `definition_from_dsl_file`.
- **Lifecycle**: `Cart → AwaitingPayment → Paid → Fulfilling → Shipped → Delivered`,
  plus `Cancelled`.
- **A selector** (`payment_retry`) branching on `PaymentDeclined`: retry (via a
  transient `Retrying` state that re-enters `AwaitingPayment`) or give up
  (`Cancelled`), counting attempts in the order's context.
- **A composite** state `Fulfilling` with `Picking → Packing → ReadyToShip`.
- **Automatic vs event transitions**, **sinks** (`Delivered`/`Cancelled` finish
  the order), and **context** (attempts, a human-readable `history`).
- **PlantUML** rendering of the whole machine.

Files:

- `order.stm` — the machine (DSL).
- `actions.py` — the `(stm, event, **inputs)` action functions (here they just
  record steps; a real app would charge a card, reserve stock, call a carrier).
- `run.py` — loads the machine, prints the diagram, and drives a few scenarios
  through the headless `DurableRunner` over an in-memory `DictStore`.

Gotcha worth noting: the cancel domain event is `CancelOrder`, not `Cancel` —
`Cancel` (with `Start`/`Reset`/`SetState`) is a reserved engine control event.
