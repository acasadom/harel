# 2. Making states do something

The turnstile's states were inert — they only marked *where* we were. Real machines run
**behaviour** as they move: charge a card on entry, release a lock on exit, log progress.

From here on we follow one running example that grows across the tutorial: an **online
order**. We start with four states and attach an action to each.

## Hooks: `on enter`, `on exit`, `on activity`

A state can run an action at three moments:

- **`on enter`** — when the state becomes active.
- **`on exit`** — when the state is left.
- **`on activity`** — when an event arrives that has *no* transition out of the current state
  (a "nothing happened, but react anyway" hook).

```text
machine order {
  initial Cart
  state Cart            { on enter on_cart }
  state AwaitingPayment { on enter request_payment }
  state Paid            { on enter capture_payment }
  state Delivered       { on enter deliver }

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from Paid to Delivered on Deliver
}
```

```{mermaid}
stateDiagram-v2
    [*] --> Cart
    Cart --> AwaitingPayment : PlaceOrder
    AwaitingPayment --> Paid : PaymentAuthorized
    Paid --> Delivered : Deliver
    Delivered --> [*]
```

The hook semantics are standard UML: each level runs its **own** enter/exit hook (there is no
"inherited" hook from an ancestor), enter fires outermost-first and exit innermost-first, and
a self-transition back into the state you are already in fires nothing. That only matters once
states are nested — see [hierarchy](06-hierarchy).

## Actions are just functions

An action has the engine's contract `(stm, event, **inputs)`. It can read and mutate the
execution's context through `stm.execution_ctx` — a plain dict that travels with the
execution and is persisted with it. Here each action records a human-readable step:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine order {
  initial Cart
  state Cart            { on enter on_cart }
  state AwaitingPayment { on enter request_payment }
  state Paid            { on enter capture_payment }
  state Delivered       { on enter deliver }

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from Paid to Delivered on Deliver
}
"""


def _record(stm, message):
    stm.execution_ctx.setdefault("history", []).append(message)


def on_cart(stm, event, **inputs):
    _record(stm, "order created")


def request_payment(stm, event, **inputs):
    _record(stm, "payment requested")


def capture_payment(stm, event, **inputs):
    _record(stm, "payment captured")


def deliver(stm, event, **inputs):
    _record(stm, "delivered")


defn = definition_from_dsl(
    SOURCE,
    "order",
    actions={
        "on_cart": on_cart,
        "request_payment": request_payment,
        "capture_payment": capture_payment,
        "deliver": deliver,
    },
)
runner = DurableRunner(DictStore(), {defn.id: defn})
print("compiled:", defn.id)
```

## Handlers vs. literals, and how they get bound

Look at the DSL: `on enter on_cart` references `on_cart` by a **bare name**. That is a
**handler** — an abstract slot the machine declares but does not implement. You bind handlers
to real functions in one of two ways:

- **In the DSL**, with a `bind` block — handy when the implementations live in a known module:

  ```text
  bind {
    on_cart         = myapp.orders.on_cart
    request_payment = myapp.orders.request_payment
  }
  ```

- **Programmatically**, with `actions={…}` passed to `definition_from_dsl` — which is what we
  do above. Its values may be dotted import paths *or* live callables (as here). `actions=`
  wins over an in-DSL `bind`, which makes it the seam for swapping implementations in tests.

A reference with **two or more dotted segments**, like `myapp.orders.on_cart`, is a
**literal**: a fully-qualified path the engine imports directly, no binding needed. So
`on enter on_cart` is a handler (bind it), while `on enter myapp.orders.on_cart` is a literal
(resolved on its own). Leaving a handler unbound is a hard error at build time — the machine
won't compile with a dangling action.

## Drive it

With the actions bound, create the execution and feed it the happy path:

```python
exe = runner.create(defn.id)
print("start ->", exe.active_path, "| history:", exe.context.get("history"))

for kind in ["PlaceOrder", "PaymentAuthorized", "Deliver"]:
    exe = runner.process(exe.id, Event(kind=kind))
    print(f"{kind:<16} -> {exe.active_path}")

print("status:", exe.status.name)
print("history:", exe.context["history"])
```

```text
start -> Cart | history: ['order created']
PlaceOrder       -> AwaitingPayment
PaymentAuthorized -> Paid
Deliver          -> Delivered
status: DONE
history: ['order created', 'payment requested', 'payment captured', 'delivered']
```

Two things to notice:

- `on_cart` already ran at `create` time — entering the `initial` state fires its `on enter`,
  so `history` is non-empty before we send a single event.
- The execution reached `DONE`. `Delivered` is a leaf with no outgoing transition — a **sink**.
  Sinking ends the execution. But `DONE` with no *verdict* doesn't tell us whether the order
  succeeded or was cancelled. Declaring that verdict is [the next step](03-outcomes).
