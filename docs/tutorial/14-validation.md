# 14. Catching mistakes early: static validation

A statechart is a *program*, and like any program it can be wrong in structural ways — a
terminal with no verdict, a state nothing reaches, a selector branch that can never match.
`validate` finds those **before you run the machine**, surface-independent and pure.

## `validate` returns findings

```python
from harel import definition_from_dsl, validate

BROKEN = """
machine order {
  initial Cart
  state Cart {}
  state Shipped {}
  state Orphan {}

  from Cart to Shipped on Deliver
  from Orphan to Shipped on Rescue
}
"""

defn = definition_from_dsl(BROKEN, "order")
for issue in validate(defn):
    print(issue)
```

```text
[error] unknown_event at Cart: transition references undeclared event 'Deliver'
[error] unknown_event at Orphan: transition references undeclared event 'Rescue'
[warning] unreachable at Orphan: state is not reachable from the root
[error] terminal_missing_outcome at Shipped: terminal of the machine/region must declare an `outcome` (e.g. success / failed) — the verdict the model routes on
```

Distinct problems, each a structured `Issue` (`severity`, `code`, `path`, `message`):

- **`Deliver` / `Rescue`** are referenced by transitions but never **declared** with an
  `event` line — an **error**. Declaring your events up front turns them into a checked
  vocabulary, so a typo (`Delvier`) can't slip through as a silent event that never fires.
- **`Shipped`** is a leaf sink that *ends the execution* but declares no `outcome` — an
  **error**. This is the rule that backs [step 3](03-outcomes): the engine won't guess a
  verdict, so it makes you state one where it's consumed.
- **`Orphan`** can't be reached from the initial state — a **warning** (dead model, probably a
  leftover or a missing edge).

## What it checks

`validate` covers the structural traps, including:

- every execution-ending terminal declares an `outcome`;
- composites have a resolvable `initial`;
- no non-deterministic automatic transitions;
- selector branch targets resolve, and declared `returns {…}` branches are exhaustive (no
  phantom or missing branch);
- **every referenced event is declared** (and, for events with fields, the field references
  exist and the operators match) — an undeclared event is an error;
- `timeout` shape, and a warning for an unhandled `Timeout`;
- unreachable states.

## Fix it, and fail the build on errors

The fixed machine declares the verdict and drops the dead state — `validate` returns no
findings. Pass `validate=True` to fail the *build* on any error, so a broken machine never even
compiles:

```python
from harel import definition_from_dsl, validate

FIXED = """
event Deliver {}

machine order {
  initial Cart
  state Cart {}
  final Shipped success {}
  from Cart to Shipped on Deliver
}
"""

print("findings:", validate(definition_from_dsl(FIXED, "order")))

defn = definition_from_dsl(FIXED, "order", validate=True)   # raises ValidationError on any error
print("built:", defn.id)
```

```text
findings: []
built: order
```

`validate=True` raises `ValidationError` if any error-severity issue is present; warnings don't
block. Wiring `validate` into your machine-loading path (as the bundled worker does) closes the
gap between "it parsed" and "it's a sound machine".

## That's the model

You now have the whole authoring surface: states and transitions, actions, outcomes, guards,
selectors, hierarchy, timers, orthogonal regions, fragments, imports, submachine `invoke`,
fan-out, payloads, and validation.

The rest of the documentation is the **operations** side — how these machines run for real:

- [Visualization & tooling](../guide/visualization) — diagrams, the language server, the live preview.
- [Durability](../guide/durability) — persisting executions across crashes; the store backends.
- [Distribution](../guide/distribution) — running machines across many workers.
- [Control plane](../guide/control-plane) — cancel, suspend, resume, terminate.

…plus a [DSL reference](../guide/dsl-reference) and the [public API](../guide/api-reference).
