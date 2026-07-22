# 15. Out-of-order events: `defer`

External systems don't always send events in the order your machine expects them. A payment
gateway may fire a webhook before your own backend has finished updating state. A mobile client
may push a confirmation before the server-side handshake completes. `defer` lets the machine
**hold** an event it isn't ready to handle yet and re-deliver it automatically once it reaches
a state that can.

## The problem: an early webhook

Consider a payment flow. After calling the gateway the machine waits for its own internal
acknowledgement (`GatewayAck`), then moves to `WaitingWebhook` where the confirmation
(`PaymentConfirmed`) is expected. In production, `PaymentConfirmed` occasionally arrives
*before* `GatewayAck` — network paths differ. Without `defer`, the early confirmation is
silently dropped and the machine gets stuck waiting forever.

```text
event GatewayAck {}
event PaymentConfirmed {}

machine payment {
  initial Charging
  state Charging {}
  state WaitingWebhook {}
  final Done success {}

  from Charging     to WaitingWebhook on GatewayAck
  from WaitingWebhook to Done         on PaymentConfirmed
}
```

## Fixing it with `defer`

Add `defer PaymentConfirmed` at the machine level. That tells the engine: *while no state in
the current configuration has a transition for this event, hold it instead of dropping it.*

```text
event GatewayAck {}
event PaymentConfirmed {}

machine payment {
  defer PaymentConfirmed

  initial Charging
  state Charging {}
  state WaitingWebhook {}
  final Done success {}

  from Charging       to WaitingWebhook on GatewayAck
  from WaitingWebhook to Done           on PaymentConfirmed
}
```

Declared at the machine level `defer` applies everywhere. Declared inside a `state` it applies
only in that state and its substates.

## Running it

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event GatewayAck {}
event PaymentConfirmed {}

machine payment {
  defer PaymentConfirmed

  initial Charging
  state Charging {}
  state WaitingWebhook {}
  final Done success {}

  from Charging       to WaitingWebhook on GatewayAck
  from WaitingWebhook to Done           on PaymentConfirmed
}
"""

defn   = definition_from_dsl(SOURCE, "payment")
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
print("start           ->", exe.active_path)  # Charging

# PaymentConfirmed arrives early — Charging has no transition for it
exe = runner.process(exe.id, Event(kind="PaymentConfirmed"))
print("after early hook ->", exe.active_path, "| deferred:", [e.kind for e in exe.deferred])

# GatewayAck arrives — machine moves to WaitingWebhook, engine re-delivers the held event
exe = runner.process(exe.id, Event(kind="GatewayAck"))
print("after ack        ->", exe.active_path, "| outcome:", exe.outcome)
```

```text
start            -> Charging
after early hook -> Charging | deferred: ['PaymentConfirmed']
after ack        -> Done     | outcome: success
```

`GatewayAck` moves the machine to `WaitingWebhook`. The engine then drains the deferred queue:
`PaymentConfirmed` now has a matching transition, so it fires immediately — all within the same
`process()` call, without you sending the event again.

## State-level `defer`

`defer` on a state only applies there and in its substates. Events that arrive outside that
scope are still dropped (or handled by on_activity if present). Use this when you only want to
hold an event for a specific phase of the machine's life.

```text
event Go {}
event EarlyResult {}

machine pipeline {
  initial Preparing

  state Preparing {
    defer EarlyResult   # hold EarlyResult only while here
  }

  state Processing {}
  final Done success {}

  from Preparing  to Processing on Go
  from Processing to Done       on EarlyResult
}
```

An `EarlyResult` that arrives during `Preparing` is held and delivered as soon as `Processing`
becomes active (triggered by `Go`). An `EarlyResult` during any other state would be dropped
normally.

## What `defer` does not cover

`defer` applies to **domain events** only. The engine's own system events — `Timeout`, `Cancel`,
`Finished`, `Reset`, `Start`, `SetState` — are routed before the defer check and cannot be held.
Writing `defer Timeout` compiles, but has no effect at runtime.

Deferred events are held in a **FIFO queue** on the `Execution` and persisted with it, so they
survive crashes and worker restarts. If the machine terminates (reaches a `final` state) while
events are still deferred, those events are discarded.

Next: [catching mistakes early with static validation](14-validation).
