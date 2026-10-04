# place_order — an order lifecycle

## What it does

The canonical e-commerce order, from cart to delivery: a payment that may be declined and
retried, fulfilment (pick, pack, choose a carrier), shipping and delivery — plus cancelling
while the payment is pending, and the two unexpected failures a real order meets: the payment
gateway or the warehouse raising. `run.py` replays seven scenarios, each on a fresh order, and
prints every step:

```text
=== payment retried, then paid ===
  (start)              -> Cart
  PlaceOrder           -> AwaitingPayment
  PaymentDeclined      -> AwaitingPayment
  PaymentAuthorized    -> Fulfilling.Picking
  Picked               -> Fulfilling.Packing
  Packed               -> Fulfilling.Express
  Dispatched           -> Shipped
  Delivered            -> Delivered
  status=DONE  outcome=success
```

## The machine

<!-- machine: examples/place_order/order.stm order -->
```{mermaid}
stateDiagram-v2
[*] --> Cart
Cart : on enter: on_cart
AwaitingPayment : on enter: request_payment
Retrying : on enter: on_retry
Paid : on enter: capture_payment
state "Fulfilling<br/>on enter: start_fulfilment" as Fulfilling {
  [*] --> Fulfilling_Picking
  state "Picking" as Fulfilling_Picking
  Fulfilling_Picking : on enter: pick
  state "Packing" as Fulfilling_Packing
  Fulfilling_Packing : on enter: pack
  state "Express" as Fulfilling_Express
  Fulfilling_Express : on enter: book_express
  state "Standard" as Fulfilling_Standard
  Fulfilling_Standard : on enter: book_standard
  Fulfilling_Picking --> Fulfilling_Packing : Picked
  state Fulfilling_Packing__choose_carrier <<choice>>
  Fulfilling_Packing --> Fulfilling_Packing__choose_carrier : Packed
  Fulfilling_Packing__choose_carrier --> Fulfilling_Express : choose_carrier=express
  Fulfilling_Packing__choose_carrier --> Fulfilling_Standard : choose_carrier=standard
  Fulfilling_Express --> Shipped : Dispatched
  Fulfilling_Standard --> Shipped : Dispatched
}
Shipped : on enter: ship
Delivered : on enter: deliver
Delivered : outcome: success
Cancelled : on enter: cancel_order
Cancelled : outcome: cancelled
PaymentError : on enter: on_payment_error
PaymentError : outcome: failed
FulfilmentError : on enter: on_fulfilment_error
FulfilmentError : outcome: failed
Cart --> AwaitingPayment : PlaceOrder
AwaitingPayment --> Paid : PaymentAuthorized
state AwaitingPayment__choose <<choice>>
AwaitingPayment --> AwaitingPayment__choose : PaymentDeclined<br/>/ context.declines = context.declines + 1, context.last_decline = event.reason
AwaitingPayment__choose --> Retrying : [context.declines < context.max_retries]
AwaitingPayment__choose --> Cancelled : else
AwaitingPayment --> Cancelled : CancelOrder
Retrying --> AwaitingPayment
Paid --> Fulfilling
Paid --> PaymentError : error
Fulfilling --> FulfilmentError : error
Shipped --> Delivered : Delivered
Delivered --> [*]
Cancelled --> [*]
PaymentError --> [*]
FulfilmentError --> [*]
```

What it shows:

- **A typed context** (`context { declines: int = 0  max_retries: int = 1 ... }`): the order's
  own data, checked by `harel validate` wherever the model reads or writes it, and by
  `create()`.
- **A `choose` with `set`** on `PaymentDeclined`: the model counts the decline and retries while
  `context.declines < context.max_retries`, through a transient `Retrying` state, else cancels
  — a decision over the order's own data, made in the model.
- **A selector**, `choose_carrier`, inside the `Fulfilling` composite: the carrier is a decision
  that belongs in code (rates, destination, weight); `returns {"express", "standard"}` lets
  `harel validate` check both branches are handled.
- **`on error`**: an unexpected exception in `capture_payment`, or anywhere inside
  `Fulfilling`, goes to a failed terminal (`PaymentError`, `FulfilmentError`) instead of
  dead-lettering the order. A declined payment is not an error: it is a modelled event.
- **`CancelOrder`, not `Cancel`**: `Cancel` belongs to the control plane (`runner.cancel()`);
  cancelling an order is business, with its own event.

## How it is built

- `order.stm` — the machine.
- `actions.py` — the `(stm, event, **inputs)` actions, which record each step in the context's
  `history` (and raise when a scenario sets `simulate_payment_error` or
  `simulate_fulfilment_error`), and the `choose_carrier` selector.
- `run.py` — compiles the machine, prints its PlantUML, and drives each scenario with a
  `DurableRunner` over a `DictStore`.

An action raising, routed by `on error`:

```{mermaid}
sequenceDiagram
  participant S as run.py
  participant R as DurableRunner
  participant E as engine
  participant A as capture_payment
  participant D as DictStore
  S->>R: process(id, PaymentAuthorized)
  R->>E: process(exe, PaymentAuthorized)
  E->>A: on enter Paid
  A--xR: raises (gateway timeout)
  R->>E: an error event (an on error is in scope)
  E->>A: Paid to PaymentError: on enter on_payment_error
  R->>D: commit(exe: PaymentError, DONE, outcome failed)
  R-->>S: exe (PaymentError)
```

The step that raised is dropped: the order goes from `AwaitingPayment` straight to
`PaymentError`, with the exception in `context._error`.

## How it runs

- **Semantics: synchronous** — `DurableRunner`, since each scenario reads the state after every
  event.
- **Execution model: background**, the default: a script, nothing to choose.
- **Action errors**: an action error that nothing models would dead-letter the order (the
  runner's `on_action_error="fail"`, its default); here the model handles both with
  `on error`, so it never comes to that.

## Run it

```text
uv run python -m examples.place_order.run
harel validate examples/place_order/order.stm order
```
