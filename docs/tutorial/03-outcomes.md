# 3. Reaching a conclusion: terminals and outcomes

In step 2 the order reached `DONE`, but `DONE` alone doesn't say *how* it ended — delivered or
cancelled. That distinction is the **outcome**: the domain verdict the surrounding system
routes on.

## Three different things called "the result"

It helps to keep three axes separate, because they answer different questions:

| Axis | Where | Answers |
| ---- | ----- | ------- |
| **status** | `exe.status` | the lifecycle state the engine manages — `RUNNING`, `DONE`, `FAILED`, `CANCELLED`, … |
| **outcome** | `exe.outcome` | the *domain verdict* — `success`, `cancelled`, `rejected`, … (a free label you choose) |
| **result** | `exe.context` | the data the run produced |

`success` is the one privileged label: "did it succeed?" means `outcome == "success"`. Every
other label is just a named variant of *not success* (UML's named final states). The engine
imposes **no default** — a normal completion is not silently "success", and an error is not
silently "failed".

## Declaring the verdict

A terminal state declares its verdict with the `final` sugar: `final <Name> <outcome>`. Our
order now ends two ways — `Delivered` (success) or `Cancelled` (cancelled):

```text
machine order {
  initial Cart
  state Cart            { on enter on_cart }
  state AwaitingPayment { on enter request_payment }
  state Paid            { on enter capture_payment }
  final Delivered success   { on enter deliver }
  final Cancelled cancelled { on enter cancel_order }

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on CancelOrder
  from Paid to Delivered on Deliver
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Cart
    Cart --> AwaitingPayment : PlaceOrder
    AwaitingPayment --> Paid : PaymentAuthorized
    AwaitingPayment --> Cancelled : CancelOrder
    Paid --> Delivered : Deliver
    Delivered --> [*]
    Cancelled --> [*]
```

`final Delivered success { … }` is sugar for a leaf state that, when sunk through, ends the
execution with `outcome = "success"`. It can still carry hooks (`on enter deliver`), which fire
as the terminal is entered.

## Run both endings

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine order {
  initial Cart
  state Cart            { on enter on_cart }
  state AwaitingPayment { on enter request_payment }
  state Paid            { on enter capture_payment }
  final Delivered success   { on enter deliver }
  final Cancelled cancelled { on enter cancel_order }

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on CancelOrder
  from Paid to Delivered on Deliver
}
"""


def _record(stm, message):
    stm.execution_ctx.setdefault("history", []).append(message)


actions = {
    name: (lambda stm, event, _m=name, **inputs: _record(stm, _m))
    for name in ["on_cart", "request_payment", "capture_payment", "deliver", "cancel_order"]
}

defn = definition_from_dsl(SOURCE, "order", actions=actions)
runner = DurableRunner(DictStore(), {defn.id: defn})


def run(events):
    exe = runner.create(defn.id)
    for kind in events:
        exe = runner.process(exe.id, Event(kind=kind))
    return exe


happy = run(["PlaceOrder", "PaymentAuthorized", "Deliver"])
cancelled = run(["PlaceOrder", "CancelOrder"])

print(f"happy     -> {happy.active_path:10} status={happy.status.name} outcome={happy.outcome}")
print(f"cancelled -> {cancelled.active_path:10} status={cancelled.status.name} outcome={cancelled.outcome}")
```

```text
happy     -> Delivered  status=DONE outcome=success
cancelled -> Cancelled  status=DONE outcome=cancelled
```

Both runs reach `DONE`, but now the **outcome** tells them apart — and that is the value the
rest of your system branches on, stores, or reports.

## Who makes sure you declared it?

Because there is no engine default, it would be easy to forget the verdict on a terminal and
silently get `outcome = None`. harel catches that with **static validation**: a rule
*requires* every execution-ending terminal to declare an outcome, and flags the ones that
don't — before you ever run the machine. We turn that on in [step 14](14-validation).

Next: not every transition should fire unconditionally. In [step 4](04-guards) we gate
transitions on **guards**.
