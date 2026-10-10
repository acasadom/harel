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
state "Cart" as Cart
Cart : on enter#58; on_cart
state "AwaitingPayment" as AwaitingPayment
AwaitingPayment : on enter#58; request_payment
state "Retrying" as Retrying
Retrying : on enter#58; on_retry
state "Paid" as Paid
Paid : on enter#58; capture_payment
state "Fulfilling<br/>on enter: start_fulfilment" as Fulfilling {
  [*] --> Fulfilling_Picking
  state "Picking" as Fulfilling_Picking
  Fulfilling_Picking : on enter#58; pick
  state "Packing" as Fulfilling_Packing
  Fulfilling_Packing : on enter#58; pack
  state "Express" as Fulfilling_Express
  Fulfilling_Express : on enter#58; book_express
  state "Standard" as Fulfilling_Standard
  Fulfilling_Standard : on enter#58; book_standard
  Fulfilling_Picking --> Fulfilling_Packing : Picked
  state Fulfilling_Packing__choose_carrier <<choice>>
  Fulfilling_Packing --> Fulfilling_Packing__choose_carrier : Packed
  Fulfilling_Packing__choose_carrier --> Fulfilling_Express : choose_carrier=express
  Fulfilling_Packing__choose_carrier --> Fulfilling_Standard : choose_carrier=standard
}
state "Shipped" as Shipped
Shipped : on enter#58; ship
state "Delivered" as Delivered
Delivered : on enter#58; deliver
Delivered : outcome#58; success
state "Cancelled" as Cancelled
Cancelled : on enter#58; cancel_order
Cancelled : outcome#58; cancelled
state "PaymentError" as PaymentError
PaymentError : on enter#58; on_payment_error
PaymentError : outcome#58; failed
state "FulfilmentError" as FulfilmentError
FulfilmentError : on enter#58; on_fulfilment_error
FulfilmentError : outcome#58; failed
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
Fulfilling_Express --> Shipped : Dispatched
Fulfilling_Standard --> Shipped : Dispatched
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
