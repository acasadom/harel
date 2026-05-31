# 4. Guarding transitions

So far every transition fires the moment its event arrives. Real machines need conditions: a
payment moves the order forward only if it was *authorized*; a declined payment goes the other
way. That condition is a **guard** — a predicate on the `where` clause.

## `where` and the event's data

An event carries data (`Event(kind="PaymentResult", data={...})`), and a guard tests it. Both
edges below react to the *same* event, `PaymentResult`, but branch on its `status`:

```text
event PaymentResult { status: string  amount: int }

guard authorized = status == "authorized"

machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid       on PaymentResult where authorized and amount <= 100
  from AwaitingPayment to Cancelled  on PaymentResult where status == "declined"
  from Paid to Delivered on Deliver
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Cart
    Cart --> AwaitingPayment : PlaceOrder
    AwaitingPayment --> Paid : PaymentResult<br/>[authorized and amount ≤ 100]
    AwaitingPayment --> Cancelled : PaymentResult<br/>[status == declined]
    Paid --> Delivered : Deliver
    Delivered --> [*]
    Cancelled --> [*]
```

Two things are new here.

**Named guards.** `guard authorized = status == "authorized"` names a reusable predicate. A
guard reference is a predicate *atom*: you can use it alone (`where authorized`) or compose it
with more conditions (`where authorized and amount <= 100`). Comparisons use
`==  !=  <  <=  >  >=  in`, and you combine them with `and` / `or` / `not`.

**`event` declarations.** `event PaymentResult { status: string  amount: int }` declares the
event's shape. It is optional, but once you declare events, [validation](14-validation) can
check that your guards reference fields that actually exist.

## Run the branches

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event PaymentResult { status: string  amount: int }

guard authorized = status == "authorized"

machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid       on PaymentResult where authorized and amount <= 100
  from AwaitingPayment to Cancelled  on PaymentResult where status == "declined"
  from Paid to Delivered on Deliver
}
"""

defn = definition_from_dsl(SOURCE, "order")
runner = DurableRunner(DictStore(), {defn.id: defn})


def after_payment(data):
    exe = runner.create(defn.id)
    exe = runner.process(exe.id, Event(kind="PlaceOrder"))
    exe = runner.process(exe.id, Event(kind="PaymentResult", data=data))
    return exe.active_path


print("authorized, 50  ->", after_payment({"status": "authorized", "amount": 50}))
print("declined        ->", after_payment({"status": "declined"}))
print("authorized, 500 ->", after_payment({"status": "authorized", "amount": 500}))
```

```text
authorized, 50  -> Paid
declined        -> Cancelled
authorized, 500 -> AwaitingPayment
```

The third case is the important one. The payment was authorized, but `amount <= 100` is false,
so the guard fails and **no edge fires** — the order stays in `AwaitingPayment`. The event was
delivered, found no enabled transition, and was simply not acted on.

```{note}
A predicate on a field the event **does not carry** evaluates to **false**, not an error. So
`where amount <= 100` against an event with no `amount` fails the guard — the transition won't
fire. Keep that in mind when an event "mysteriously" doesn't transition: a typo'd or missing
field silently fails the guard rather than blowing up.
```

## Binding guards from the outside

Just as actions can be bound programmatically with `actions=`, guards can be supplied with
`guards={…}` — a map from guard name to a predicate dict (e.g. `{"status__eq": "authorized"}`
or `{"all": [...]}`). It overrides the in-DSL `guard` declaration, giving tests and callers the
same swap-the-implementation seam they have for actions.

A guard answers yes/no. When you need to route *more than two ways* on a computed value, you
reach for a **selector** — [the next step](05-selectors).
