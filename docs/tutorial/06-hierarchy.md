# 6. Hierarchy: composite states

Flat machines get unwieldy fast. A statechart's defining feature is **hierarchy**: a state can
*contain* a whole sub-machine. Once an order is paid, it enters **fulfilment**, which is itself
a little machine — pick the items, pack them, get them ready to ship.

## A state with states inside it

A `state` block that declares its own `initial` and child states is a **composite**:

```text
machine order {
  initial Paid
  state Paid { on enter capture_payment }

  state Fulfilling {
    on enter start_fulfilment
    on exit  fulfilment_done
    initial Picking
    state Picking     { on enter pick }
    state Packing     { on enter pack }
    state ReadyToShip { on enter ready }

    from Picking to Packing on Picked
    from Packing to ReadyToShip on Packed
    from ReadyToShip to Delivered on Dispatched
  }

  final Delivered success { on enter deliver }

  from Paid to Fulfilling on Capture
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Paid
    Paid --> Fulfilling : Capture
    state Fulfilling {
        [*] --> Picking
        Picking --> Packing : Picked
        Packing --> ReadyToShip : Packed
    }
    Fulfilling --> Delivered : Dispatched
    Delivered --> [*]
```

Entering `Fulfilling` descends into its `initial` child, `Picking`. The current position is a
**path**: `Fulfilling.Picking`, then `Fulfilling.Packing`, and so on.

## Scope: where a transition can live

Notice `from ReadyToShip to Delivered on Dispatched` is written **inside** `Fulfilling`. That
matters: a transition can only name states in its own scope. `ReadyToShip` is internal to
`Fulfilling`, so an edge leaving it must be declared there — but its target (`Delivered`)
resolves by looking *outward* through enclosing scopes. (A transition declared on the composite
itself, like `from Fulfilling to Cancelled on CancelOrder`, applies whatever inner state is
active — handy for "no matter where we are in fulfilment, a cancel aborts it".)

## Hooks fire per level

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine order {
  initial Paid
  state Paid { on enter capture_payment }

  state Fulfilling {
    on enter start_fulfilment
    on exit  fulfilment_done
    initial Picking
    state Picking     { on enter pick }
    state Packing     { on enter pack }
    state ReadyToShip { on enter ready }

    from Picking to Packing on Picked
    from Packing to ReadyToShip on Packed
    from ReadyToShip to Delivered on Dispatched
  }

  final Delivered success { on enter deliver }

  from Paid to Fulfilling on Capture
}
"""


def _record(stm, message):
    stm.execution_ctx.setdefault("trace", []).append(message)


names = ["capture_payment", "start_fulfilment", "fulfilment_done", "pick", "pack", "ready", "deliver"]
actions = {n: (lambda stm, event, _m=n, **inputs: _record(stm, _m)) for n in names}

defn = definition_from_dsl(SOURCE, "order", actions=actions)
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
for kind in ["Capture", "Picked", "Packed", "Dispatched"]:
    exe = runner.process(exe.id, Event(kind=kind))
    print(f"{kind:12} -> {exe.active_path}")

print("trace:", exe.context["trace"])
```

```text
Capture      -> Fulfilling.Picking
Picked       -> Fulfilling.Packing
Packed       -> Fulfilling.ReadyToShip
Dispatched   -> Delivered
trace: ['capture_payment', 'start_fulfilment', 'pick', 'pack', 'ready', 'fulfilment_done', 'deliver']
```

Read the trace against UML's rules, which harel follows exactly:

- **Each level runs its own hook** — there is no override-by-depth. Entering `Fulfilling` runs
  `start_fulfilment` *and then* `pick` (the child's `on enter`).
- **Enter is outermost-first, exit innermost-first.** Leaving on `Dispatched`, the inner state
  exits before `fulfilment_done` (the composite's `on exit`), then `Delivered` is entered
  (`deliver`).
- A state with no hook fires nothing; a self-transition into the state you're already in fires
  nothing.

Composites are also where **time** starts to matter — a whole sub-machine might have a budget.
[The next step](07-timers) arms durable timers.
