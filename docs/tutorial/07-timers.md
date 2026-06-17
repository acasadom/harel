# 7. Time: durable timers

Some transitions are driven by the clock, not by an event. An unpaid order shouldn't sit
forever — give it a payment window, and cancel it when the window closes. harel models that
with a **durable timer**.

## `timeout` arms a timer

Add `timeout <seconds>` to a state. Entering the state arms a timer; leaving it cancels the
timer. When the timer is due, the engine delivers a reserved **`Timeout`** event — and your
model decides what that means by handling it like any other event:

```text
event PaymentAuthorized {}
event Deliver {}

machine order {
  initial AwaitingPayment
  state AwaitingPayment { timeout 900 }
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on Timeout
  from Paid to Delivered on Deliver
}
```

```{mermaid}
stateDiagram-v2
    [*] --> AwaitingPayment
    AwaitingPayment --> Paid : PaymentAuthorized
    AwaitingPayment --> Cancelled : Timeout (900s)
    Paid --> Delivered : Deliver
    Delivered --> [*]
    Cancelled --> [*]
```

This is the key design choice: **the engine schedules, the model decides.** The engine arms,
fires, and cancels the timer durably; *what* a timeout does — cancel, retry, escalate — is an
ordinary transition you write. There is no special "timeout handler" concept.

## Firing it deterministically

Timers fire against a clock, and the clock is injectable — so examples (and tests) are
deterministic, with no real sleeping. We pass a clock we control, then advance it and ask the
runner to sweep for due timers:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event PaymentAuthorized {}
event Deliver {}

machine order {
  initial AwaitingPayment
  state AwaitingPayment { timeout 900 }
  state Paid {}
  final Delivered success {}
  final Cancelled cancelled {}

  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on Timeout
  from Paid to Delivered on Deliver
}
"""

defn = definition_from_dsl(SOURCE, "order")

clock = [1000.0]  # a mutable clock we advance by hand
runner = DurableRunner(DictStore(), {defn.id: defn}, clock=lambda: clock[0])

exe = runner.create(defn.id)               # entering AwaitingPayment arms a timer for t=1900
print("start       ->", exe.active_path)
print("sweep @1000 -> fired", runner.fire_due_timers(), "(window still open)")

clock[0] = 1900.0                          # the window closes
print("sweep @1900 -> fired", runner.fire_due_timers())
exe = runner.store.load(exe.id)
print("result      ->", exe.active_path, "/", exe.outcome)
```

```text
start       -> AwaitingPayment
sweep @1000 -> fired 0 (window still open)
sweep @1900 -> fired 1
result      -> Cancelled / cancelled
```

`fire_due_timers()` delivers every timer due at the current clock and returns how many fired.
In production you don't call it by hand — a worker's idle loop sweeps automatically (see
[distribution](../guide/distribution)). And because the timer is persisted in the store, it
survives a crash or restart: a timer armed before the process died still fires when a worker
comes back and sweeps. If the payment *does* arrive first, leaving `AwaitingPayment` cancels
the timer, so a later sweep finds nothing due.

```{note}
The `Timeout` is anchored to the state that armed it and **bubbles up**: it fires that state's
own `on Timeout`, or — if it has none — an enclosing ancestor's. A timeout for a state that is
no longer active is silently ignored (a staleness guard), so a stale sweep can never derail a
machine that already moved on.
```

## Retry and backoff are *modelled*, not built in

Because the model decides what a timeout does, retry-with-backoff isn't an engine feature —
it's a small composite you assemble: a `Waiting` state whose delay is read from context
(`timeout {context: backoff}`), a selector that branches *succeeded / retry again*, and the
composite's own `timeout` as the overall budget. harel ships composable backoff actions
(`harel.lib.exponential_backoff`, `linear_backoff`, `reset_backoff`) to compute the next
delay. The full pattern is laid out in [durability](../guide/durability); for now the takeaway
is that *policy lives in the model*, and the engine just keeps time.

Next we leave the single-thread-of-control world entirely: [orthogonal regions](08-orthogonal)
let a machine be in several states **at once**.
