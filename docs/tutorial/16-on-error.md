# 16. Catching action errors: `on error`

Actions can raise. By default an unhandled exception fails the execution terminally — the
runner sets `status=FAILED` and dead-letters it. `on error` lets the machine **route** an
exception to a recovery state instead, keeping the execution alive and giving the model a
chance to handle it explicitly.

## The problem: an exception kills the execution

Consider a payment step that calls a card processor. If the processor client raises, the
whole execution is marked `FAILED` — even if the failure was expected and recoverable.

```text
machine payment {
  initial Charging
  state Charging { on enter charge_card }
  final Done    success {}
  final Aborted failed  {}

  from Charging to Done on Charged
}
```

A `ValueError` inside `charge_card` ends the execution with no verdict and no way to
distinguish a card-decline from a bug.

## Fixing it with `on error`

Add a transition `from Charging to Declined on error`. The engine synthesises an `error`
event the moment the action raises and routes it immediately — the same way any other event
triggers a transition.

```text
machine payment {
  initial Charging
  state Charging { on enter charge_card }
  state Declined { on enter notify_declined }
  final Done    success {}
  final Aborted failed  {}

  from Charging to Declined on error
  from Charging to Done     on Charged
  from Declined to Aborted
}
```

If `charge_card` raises, the machine moves to `Declined` where `notify_declined` runs, then
settles in `Aborted`. The execution reaches a clean terminal state with a verdict.

## Running it

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine payment {
  initial Charging
  state Charging { on enter charge_card }
  state Declined { on enter notify_declined }
  final Done    success {}
  final Aborted failed  {}

  from Charging to Declined on error
  from Charging to Done     on Charged
  from Declined to Aborted
}
"""


def charge_card(stm, event, **inputs):
    raise ValueError("card declined by processor")


def notify_declined(stm, event, **inputs):
    stm.execution_ctx["decline_reason"] = stm.execution_ctx["_error"]["message"]


defn = definition_from_dsl(
    SOURCE,
    "payment",
    actions={"charge_card": charge_card, "notify_declined": notify_declined},
)
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
print("path:   ", exe.active_path)       # Aborted
print("outcome:", exe.outcome)            # failed
print("reason: ", exe.context["decline_reason"])
print("_error: ", exe.context["_error"])
```

```text
path:    Aborted
outcome: failed
reason:  card declined by processor
_error:  {'type': 'ValueError', 'message': 'card declined by processor'}
```

Two things the engine does automatically:

- `context["_error"]` is set to `{"type": "...", "message": "..."}` before the error
  transition fires — the recovery state's action can read it.
- Execution status stays `DONE` (with a `failed` outcome), not `FAILED`. The execution
  reached a modelled terminal state; `status=FAILED` means *the engine itself* gave up.

## Routing by exception type

`on error` can be guarded just like any event transition. The synthetic `error` event carries
`type` (the exception class name) and `message`, both guardable with `where`.

```text
machine payment {
  initial Charging
  state Charging { on enter charge_card }
  state Declined {}
  final Done    success {}
  final Aborted failed  {}
  final Crashed failed  {}

  from Charging to Declined on error where type == "ValueError"
  from Charging to Crashed  on error
  from Charging to Done     on Charged
  from Declined to Aborted
}
```

A `ValueError` routes to `Declined`; any other exception falls through to the catch-all
`on error` (no guard). Guards are evaluated top-to-bottom in declaration order, and the first
match wins — the same rules as any other guarded transition.

If **no** `on error` guard matches (including the case where every handler has a `where`
clause that doesn't match the raised exception), the runner's default policy applies: the
execution is failed terminally (`status=FAILED`), exactly as if no `on error` were declared
at all.

## Scope and inheritance

`on error` is a transition, not a state-level declaration. It lives wherever any transition
lives — at the machine root or inside a composite state's block — and the normal scope rules
apply: a transition declared inside `state Outer {}` is only in scope while `Outer` (or one
of its substates) is active.

To catch errors from a whole group of states in one place, declare the handler in a
wrapping composite:

```text
state Checkout {
  initial Charging
  state Charging { on enter charge_card }
  state Capturing { on enter capture_card }
  state CheckoutError {}

  from Charging  to Capturing    on Authorized
  from Checkout  to CheckoutError on error   # catches any action error inside Checkout
  from CheckoutError to …
}
```

## What `on error` does not cover

- **Non-action code**: only exceptions raised *inside* an action function are intercepted.
  Engine-internal errors (bugs in harel itself) are not routed.
- **Error handler raises too**: if the action in the error-handler state also raises, the
  runner's default policy applies — there is no second-level routing to avoid loops.
- **Guards and selectors**: errors in guard functions or selector functions are not
  intercepted by `on error`.
- **`on exit`**: an exception raised while *leaving* a state is never routed, regardless of
  scope. Leaving a state runs real, un-undoable side effects (releasing a lock, cancelling
  orthogonal regions, disarming a timer); recovering away from it would have to re-run that
  very `on exit` to reach anywhere outside the state's own subtree, re-triggering the same
  failure. So `on exit` is expected to always succeed — a raise there always falls straight
  to the runner's default policy (fail the execution), the same as an unmatched `on error`.

Model *expected* failures as action results routed by a [selector](05-selectors); use
`on error` for genuine exceptions you want to recover from in the model.

See also: [guards](04-guards), [selectors](05-selectors), [DSL reference](../guide/dsl-reference).
