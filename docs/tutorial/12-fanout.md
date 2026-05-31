# 12. Data-parallel work: fan-out

[Orthogonal regions](08-orthogonal) run a *fixed set of different* activities at once.
**Fan-out** is the other kind of parallelism: run the *same* machine once **per item** in a
collection — ship every parcel in the order, deploy to every region, process every line item —
and join when they're all done.

## `invoke … for item in collection`

A fan-out is an `invoke` with a `for` clause. It forks one addressed child per entry of a
context list, seeding each from its slice, and joins on completion:

```text
# ship.stm — the per-parcel machine
machine ship {
  initial Shipping
  state Shipping {}
  final Shipped success {}
  from Shipping to Shipped
}
```

```text
# order.stm
import "ship.stm" as s

machine order {
  initial Dispatching
  state Dispatching {
    invoke s.ship for parcel in parcels   # one `ship` per entry of `parcels`
    with { parcel: parcel }               # each child gets its own slice
  }
  final Delivered success
  final Stuck failed

  from Dispatching join all to Delivered else to Stuck
}
```

The `join all` is the exit: reach `Delivered` only if **every** child succeeded; otherwise
`Stuck`. (`join any` reaches the first target if **at least one** succeeded.)

## Run it

```python
import tempfile
from pathlib import Path

from harel import definition_from_dsl_file, DurableRunner, DictStore

project = Path(tempfile.mkdtemp())
(project / "ship.stm").write_text("""
machine ship {
  initial Shipping
  state Shipping {}
  final Shipped success {}
  from Shipping to Shipped
}
""")
(project / "order.stm").write_text("""
import "ship.stm" as s

machine order {
  initial Dispatching
  state Dispatching {
    invoke s.ship for parcel in parcels
    with { parcel: parcel }
  }
  final Delivered success
  final Stuck failed

  from Dispatching join all to Delivered else to Stuck
}
""")

defn = definition_from_dsl_file(project / "order.stm", "order")
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id, context={"parcels": ["A", "B", "C"]})
print(f"3 parcels -> {exe.active_path} / {exe.outcome} ({exe.status.name})")
```

```text
3 parcels -> Delivered / success (DONE)
```

Three parcels in the `parcels` list fork three independent `ship` children, each addressed by
its index and seeded with its own `parcel`. When all three reach `Shipped`, the `join all` gate
routes the order to `Delivered`.

## Fan-out vs. orthogonal — don't mix them up

```{important}
| | Orthogonal region | Fan-out invoke |
| --- | --- | --- |
| What runs | a *fixed set of different* sub-machines | the *same* machine, once per item |
| How many | known at authoring time | depends on a runtime list |
| Events | broadcast to all regions | each child is an isolated black box |
| Use for | "fraud check **and** stock reservation" | "ship **each** parcel" |

Reaching for an orthogonal state when you mean "N of the same" (or vice-versa) is the most
common modelling mistake here. If the count comes from data, it's a fan-out.
```

Both single `invoke` and fan-out hand data back through the `Returned`/join machinery. The next
step looks at exactly **what** crosses those boundaries: [payloads](13-payloads).
