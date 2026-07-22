# DSL reference

A compact reference to the `.stm` language. Each form links back to the tutorial step that
introduces it.

## Top-level declarations

```text
import "lib.stm"            # bring in another file (events/guards/bindings: bare)
import "lib.stm" as ns      # machines/fragments namespaced as ns.Name

event Name { field: type  other: type? }   # type ∈ string|int|float|bool|any ; ? = optional

guard name = <predicate>    # a reusable, composable predicate atom

bind { handler = pkg.mod.fn }   # map handler names to implementations

machine Name { … }          # a machine
fragment Name(params) { … } # a parametrized, reusable piece
```

## Inside a machine or state

```text
initial StateName           # where this (sub)machine starts
state Name { … }            # a state; nest states inside for a composite
orthogonal Name { … }       # an AND-state; each child state is a concurrent region
final Name <outcome> { … }  # sugar: a terminal sink declaring its verdict

on enter <action>           # hook: on entering
on exit <action>            # hook: on leaving
on activity <action>        # hook: event arrived with no transition
timeout 900                 # arm a durable timer (seconds)
timeout context key         # …with the delay read from context[key]
outcome <label>             # this terminal's verdict (final is sugar over this)
carry k1, k2                # context keys a region propagates on Finished
defer EventA, EventB        # hold these events while unhandled here; re-deliver on a state that handles them
no history                  # don't restore history on re-entry
```

`defer` makes an event that has no transition in the current state **wait** instead of being
dropped: it goes onto a per-execution FIFO (persisted on the `Execution`) and is re-delivered as
soon as the machine enters a state that has a matching transition. Declared on a state it applies
there and in its substates; declared at the machine level it applies everywhere a state doesn't
handle the event. Use it for out-of-order async events — a webhook or callback that can arrive
before the machine reaches the state that consumes it.

`defer` applies only to **domain events**. System events (`Timeout`, `Cancel`, `Finished`, `Reset`,
`Start`, `SetState`) are routed by the engine before the defer check and cannot be held this way.

Declared on a parent composite or at the machine root, `defer` is visible to orthogonal region
children that share the same Definition — they inherit the full ancestor chain of defer sets.

See: [deferred events](../tutorial/15-deferred-events), [actions](../tutorial/02-actions),
[outcomes](../tutorial/03-outcomes), [hierarchy](../tutorial/06-hierarchy),
[timers](../tutorial/07-timers), [orthogonal](../tutorial/08-orthogonal),
[payloads](../tutorial/13-payloads).

## Transitions

```text
from A to B                       # automatic (no trigger)
from A to B on Event              # event-triggered
from A to B on E1 | E2            # multiple event kinds
from A to B on Event where <pred> # guarded (where only after `on`)

from A select fn on Event {       # computed branch (a selector)
  "x" to B
  "y" to C
  else to D
}
from A select fn returns {"x","y"} { … }   # declared branches (validatable)

from Fork join all to X else to Y          # orthogonal-join sugar (all regions success)
from Fork join any to X else to Y          # …at least one
```

See: [guards](../tutorial/04-guards), [selectors](../tutorial/05-selectors),
[payloads](../tutorial/13-payloads).

## Predicates

```text
field == value     field != value
field <  value     field <= value
field >  value     field >= value
field in [a, b]
guardName                          # a named guard, as an atom
P and Q     P or Q     not P       # composable; parenthesize as needed
```

A predicate on a field the event **does not carry** is **false** (not an error).

## Actions

```text
on enter handler             # a handler — a bare name, bound via bind{} / actions=
on enter pkg.mod.fn          # a literal — a dotted path, imported directly
on enter act(base: 10, k: v) # with inputs (literals or value-params inside a fragment)
```

Signature: `def fn(stm, event, **inputs)`, reading/writing `stm.execution_ctx`.

## Fragments & invoke

```text
fragment F(work: action, ok: guard, target: state, budget: value, ev: event) { … }
use F(work=charge, ok=(status=="ok"), budget=30) as Local   # splice as a child composite

fragment Outer(work: action, budget: value) {     # a fragment may `use` another fragment
  initial X  state X {}
  use F(work=work, ok=(status=="ok"), budget=budget) as Inner   # …and forward its own params
}

invoke pkg.machine                   # run another machine as a black box (a leaf)
  with { child_key: parent_key }     # project input in
invoke pkg.machine for item in coll  # fan-out: one child per entry of coll
  with { slice: item }
invoke { … machine body … }          # inline target (QML-style)
```

A single `invoke` leaves only via `on Returned where …`; a fan-out leaves via a `join`.
Fragments **nest**: a fragment body may `use` another fragment and **forward its own
parameters** (action, guard, value, state and event) as the nested use's args — resolved
against the enclosing fragment's scope. Forwarding a name the enclosing fragment doesn't declare
is a `DslError`.

See: [fragments](../tutorial/09-fragments), [imports](../tutorial/10-imports),
[invoke](../tutorial/11-invoke), [fan-out](../tutorial/12-fanout).

## Reserved events

`Start`, `Finished`, `Timeout`, `Cancel`, `Reset`, `SetState`, `Returned` are the engine's own
events — don't use these names for your domain events. (`CancelOrder`, not `Cancel`, for a
domain "cancel".)
