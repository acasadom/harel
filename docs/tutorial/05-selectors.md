# 5. Computed branching with selectors

A guard is binary: the edge fires or it doesn't. But sometimes the next state depends on a
*computed* value with several outcomes — "retry, or give up?", "route to A, B, or C?". That is
a **selector**: the imperative sibling of a guard.

## `select` routes by a function's result

When a payment is declined, our order should retry a couple of times, then give up. A selector
runs a function and routes on the string it returns:

```text
event PlaceOrder {}
event PaymentAuthorized {}
event PaymentDeclined {}
event Deliver {}

machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized

  from AwaitingPayment select payment_retry on PaymentDeclined {
    "retry"  to AwaitingPayment
    "giveup" to Cancelled
  }

  from Paid to Delivered on Deliver
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Cart
    Cart --> AwaitingPayment : PlaceOrder
    AwaitingPayment --> Paid : PaymentAuthorized
    state payment_retry <<choice>>
    AwaitingPayment --> payment_retry : PaymentDeclined
    payment_retry --> AwaitingPayment : retry
    payment_retry --> Cancelled : giveup
    Paid --> Delivered : Deliver
    Delivered --> [*]
    Cancelled --> [*]
```

When a `PaymentDeclined` arrives in `AwaitingPayment`, the engine runs `payment_retry`, takes
the string it returns, and follows the matching branch. A selector function has the same
`(stm, event, **inputs)` contract as any action; it just *returns* a branch key.

## Run it

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event PlaceOrder {}
event PaymentAuthorized {}
event PaymentDeclined {}
event Deliver {}

machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized

  from AwaitingPayment select payment_retry on PaymentDeclined {
    "retry"  to AwaitingPayment
    "giveup" to Cancelled
  }

  from Paid to Delivered on Deliver
}
"""


def payment_retry(stm, event, **inputs):
    attempts = stm.execution_ctx.get("attempts", 0) + 1
    stm.execution_ctx["attempts"] = attempts
    return "retry" if attempts < 2 else "giveup"


defn = definition_from_dsl(SOURCE, "order", actions={"payment_retry": payment_retry})
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
exe = runner.process(exe.id, Event(kind="PlaceOrder"))

exe = runner.process(exe.id, Event(kind="PaymentDeclined"))
print("decline #1 ->", exe.active_path)            # retry: back to AwaitingPayment

exe = runner.process(exe.id, Event(kind="PaymentDeclined"))
print("decline #2 ->", exe.active_path, "/", exe.outcome)   # giveup: Cancelled
```

```text
decline #1 -> AwaitingPayment
decline #2 -> Cancelled / cancelled
```

The first decline returns `"retry"` and loops back; the second returns `"giveup"` and routes
to `Cancelled`. The retry policy — *how many times, with what backoff* — lives in ordinary
code (the function) and in the model (the states), not in the engine.

## `else` and declared branches

Two optional refinements:

- **`else`** catches any returned value with no explicit branch:

  ```text
  from Triage select classify on Ticket {
    "bug"     to Engineering
    "billing" to Finance
    else      to GeneralQueue
  }
  ```

- **`returns { … }`** declares the set of values the function can return, so
  [validation](14-validation) can flag a branch that can never match (a typo) or a value with
  no branch and no `else` (a hole):

  ```text
  from Triage select classify returns {"bug", "billing"} on Ticket {
    "bug"     to Engineering
    "billing" to Finance
  }
  ```

So far the order has been a flat list of states. Real machines group related states into
**composites** — that hierarchy is [the next step](06-hierarchy).
