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

## An inactivity budget: `ttl`

A `timeout` bounds how long a machine may stay in *one state*. Some machines never finish on
purpose — a session or a listener that loops on every event it receives — and the question is
different: how long may it go **without hearing from anyone**? Declare that once, at the
machine level, with `ttl <seconds>`. Every domain event the execution receives restarts the
budget; when it runs out, the engine delivers the reserved **`Expired`** event:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SESSION = """
event Ping {}

machine session {
  ttl 1800
  initial Active
  state Active {}
  final Closed expired {}

  from Active to Active on Ping
  from Active to Closed on Expired
}
"""

clock = [0.0]
defn = definition_from_dsl(SESSION, "session", validate=True)
store = DictStore()
runner = DurableRunner(store, {defn.id: defn}, clock=lambda: clock[0])

exe = runner.create(defn.id)
clock[0] = 1500.0
runner.process(exe.id, Event(kind="Ping"))   # activity: the budget restarts from here
clock[0] = 2000.0
runner.fire_due_timers()
print("at 2000:", store.load(exe.id).status.name)
clock[0] = 3301.0
runner.fire_due_timers()
exe = store.load(exe.id)
print("at 3301:", exe.status.name, exe.active_path, exe.outcome)
```

```text
at 2000: RUNNING
at 3301: DONE Closed expired
```

- **Only domain events count** — what you `send`. The machine's own `Timeout`s are not activity,
  so a machine that only polls on a timer still expires if nobody talks to it.
- **`on Expired` must go straight to a terminal**, like `on Cancel`: the execution is being
  ended, not asked to do more work (`harel validate` checks it, and the engine refuses an unsafe
  one at runtime). Without one, the execution is ended forcefully — `CANCELLED` with outcome
  `expired`, any live region or invoked child cancelled.
- **Roots only.** A region or an `invoke` child lives as long as its parent; if the invoked
  machine declares a `ttl`, it applies only when that machine runs on its own. An event
  broadcast to a machine's regions still counts as activity for the machine itself.
- A suspended execution's budget keeps running: if it ran out meanwhile, the execution expires
  once resumed. A `FAILED` dead letter never expires (abandoning one is a deliberate
  `terminate`); a `redrive` restarts its budget.

An expired execution is finished like any other, so a [purge](../guide/control-plane.md#purge)
retention job removes it later.

Next we leave the single-thread-of-control world entirely: [orthogonal regions](08-orthogonal)
let a machine be in several states **at once**.
