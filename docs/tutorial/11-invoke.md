# 11. Composing machines: submachine invoke

Sometimes a step is itself a whole machine you'd rather keep separate — payment authorization,
a KYC check, a credit decision. `invoke` runs **another** machine as a child, treats it as a
**black box**, and routes on the verdict it returns.

## A machine that authorizes payment

The payment machine decides on its projected input and ends with a verdict. It self-drives —
an automatic selector routes it the moment it starts (we'll see why that matters below):

```text
# payment.stm
machine payment {
  carry authcode
  initial Authorizing
  state Authorizing {}
  final Authorized success {}
  final Declined failed {}

  from Authorizing select paylib.decide {
    "ok" to Authorized
    "no" to Declined
  }
}
```

The order invokes it, projects input in with `with { … }`, and routes on the reserved
**`Returned`** event:

```text
# order.stm
import "payment.stm" as pay

machine order {
  initial Charge
  state Charge {
    invoke pay.payment
    with { amount: amount }      # child's context <- parent's context
  }
  final Paid success
  final Cancelled cancelled

  from Charge to Paid      on Returned where outcome == "success"
  from Charge to Cancelled on Returned where outcome == "failed"
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Charge
    state Charge {
        [*] --> payment
        note right of payment : runs payment.stm\nas a black box
    }
    Charge --> Paid : Returned [success]
    Charge --> Cancelled : Returned [failed]
    Paid --> [*]
    Cancelled --> [*]
```

## Run it

```python
import sys, tempfile
from pathlib import Path

from harel import definition_from_dsl_file, DurableRunner, DictStore

project = Path(tempfile.mkdtemp())
sys.path.insert(0, str(project))

# the submachine's decision logic, referenced as a literal dotted path
(project / "paylib.py").write_text(
    "def decide(stm, event, **inputs):\n"
    "    return 'ok' if stm.execution_ctx.get('amount', 0) <= 100 else 'no'\n"
)
(project / "payment.stm").write_text("""
machine payment {
  carry authcode
  initial Authorizing
  state Authorizing {}
  final Authorized success {}
  final Declined failed {}

  from Authorizing select paylib.decide {
    "ok" to Authorized
    "no" to Declined
  }
}
""")
(project / "order.stm").write_text("""
import "payment.stm" as pay

machine order {
  initial Charge
  state Charge {
    invoke pay.payment
    with { amount: amount }
  }
  final Paid success
  final Cancelled cancelled

  from Charge to Paid      on Returned where outcome == "success"
  from Charge to Cancelled on Returned where outcome == "failed"
}
""")

defn = definition_from_dsl_file(project / "order.stm", "order")
runner = DurableRunner(DictStore(), {defn.id: defn})

for amount in (50, 500):
    exe = runner.create(defn.id, context={"amount": amount})
    print(f"amount={amount:<4} -> {exe.active_path} / {exe.outcome}")
```

```text
amount=50   -> Paid / success
amount=500  -> Cancelled / cancelled
```

Entering `Charge` forks the `payment` machine as a child execution seeded with `amount`. The
child runs to its own terminal, and on finishing emits a `Returned` event carrying its
`outcome` (and anything it declared with `carry`). The order routes on that event: a small
amount is `Authorized` → `Paid`; a large one is `Declined` → `Cancelled`.

## Black box, in and out

```{important}
A submachine is a **black box**. The parent's domain events are **not** delivered into it — so
a submachine must drive itself to completion (automatic transitions and selectors), which is
why `payment` uses an automatic `select`. Communication is narrow and explicit:

- **in** — `with { child_key: parent_key }` projects parent context into the child;
- **out** — the child's terminal `outcome`, plus any context keys it lists with `carry`,
  arrive on the `Returned` event.

A single `invoke` state must be a **leaf with no automatic exit** — it leaves only by routing
on `Returned`. (Lifecycle `Cancel` still reaches a running submachine; that's the control
plane, in [its own guide](../guide/control-plane).)
```

Invoking *one* submachine is composition. Invoking the *same* machine once **per item in a
list** — ship N parcels, each its own run — is the data-parallel sibling:
[fan-out](12-fanout).
