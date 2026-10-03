# Place order — example

The canonical e-commerce order lifecycle (the DDD "place order" example) modelled
as an `harel` statechart. It's a runnable demo; `test/unit/examples/` checks its
scenarios.

```bash
uv run python -m examples.place_order.run
```

What it shows:

- **Declarative definition** (`order.stm`) compiled by `definition_from_dsl_file`.
- **Lifecycle**: `Cart → AwaitingPayment → Paid → Fulfilling → Shipped → Delivered`,
  plus `Cancelled`.
- **A typed context** (`context { ... }`): the order's own data, checked by
  `harel validate` wherever the model reads or writes it, and by `create()`.
- **A `choose` with `set`** on `PaymentDeclined`: the model counts the decline
  (`set context.declines = context.declines + 1`) and retries while
  `context.declines < context.max_retries` — through a transient `Retrying` state
  that re-enters `AwaitingPayment` — else cancels. No Python needed for a decision
  over the order's own data.
- **A selector** (`choose_carrier`) inside `Fulfilling`: after packing, a Python
  function picks the carrier (`express` / `standard`) — the kind of decision that
  belongs in code (rates, destination, weight; in a real app, a shipping API).
  `returns {"express", "standard"}` lets `harel validate` check every branch.
- **A composite** state `Fulfilling` with `Picking → Packing → Express | Standard`.
- **`on error`**: an unexpected exception in `capture_payment` or in `Fulfilling`
  goes to a failed terminal instead of dead-lettering the order.
- **Automatic vs event transitions**, **terminals** (`Delivered`/`Cancelled` finish
  the order), and **PlantUML** rendering of the whole machine.

Files:

- `order.stm` — the machine (DSL).
- `actions.py` — the `(stm, event, **inputs)` action functions (here they record
  steps in a `history`) and the `choose_carrier` selector.
- `run.py` — loads the machine, prints the diagram, and drives the scenarios
  through the headless `DurableRunner` over an in-memory `DictStore`.

The cancel domain event is `CancelOrder`, not `Cancel`: `Cancel` is reserved for the
control plane (`runner.cancel()` tears an execution down in one step), and so are
`Start`, `Reset`, `SetState`, `Timeout`, `Finished`, `Returned`, `Expired` and `error`.
Cancelling an order is business, with its own event and transitions.
