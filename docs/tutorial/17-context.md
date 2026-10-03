# 17. The context in the model: guards, `set` and `choose`

So far the execution context has been something only actions touch: an action reads and writes
`stm.execution_ctx`, and the model routes on events. This chapter lets the model itself use its
own extended state — a guard can read it, a transition can update it, a `choose` can route on it,
and a `context { ... }` block can declare its types — so a counter or a flag doesn't need a Python
function around it.

## Guards read the context

A comparison reads a field of the triggering event (the bare form, as before, or `event.x`) or
of the execution context (`context.x`):

```text
from Working to GaveUp on Fail where context.attempts >= 3
from Working to Retry  on Fail where context.attempts < 3 and event.code == 503
guard exhausted = context.attempts >= 3          # named guards may read it too
```

As with an event field, a guard on a context field that isn't there — or whose value can't be
compared (`"abc" < 3`) — simply doesn't hold.

The right side of a comparison is a literal or another reference, written with its namespace
(a bare name on the right is not a reference):

```text
from Working to GaveUp on Fail where context.attempts >= context.max_attempts
from Working to Retry  on Fail where event.code != context.last_code
```

If either side is missing, or the two values can't be compared, the guard doesn't hold. When
both sides have a declared type (the context schema, the event's fields) and the types differ —
an `int` with a `string` — `harel validate` warns (`compare_type_mismatch`): such values are never
equal or ordered.

## A typed context

A machine may declare its context's fields, with the same field syntax as an event — and,
unlike an event's, a field may give a **default**, the value an execution starts with when the
caller doesn't pass one:

```text
machine job {
  context {
    attempts: int = 0      # starts at 0 unless create() passes it
    max_attempts: int = 3
    last_code: int?        # optional: may be absent
  }
  ...
}
```

A default is a literal of the field's type (`harel validate` checks it:
`default_type_mismatch`); a field with one can't also be `?` — an execution always has it. A list
default is copied for each execution. Defaults fill an execution created with `create()`, and the
child of an `invoke` gets its own machine's defaults; an orthogonal region starts with only what
its fork passes down (`with`), as below.

The declaration is checked at three points:

- **when an execution is created** (after the defaults are filled in) — a required field
  missing, or a declared field of the wrong type, raises `ContextError` (with `DistributedRunner.create(..., start_on_create=False)`, the
  required fields may still come with `start(data=...)`, and are checked there);
- **by `harel validate`** — a guard or a `set` naming an undeclared field, or a `set` whose value
  can't be of the declared type, is an error;
- **after each `set`** — see below.

Undeclared keys are not rejected at runtime: actions may keep their own keys next to the declared
ones. The schema covers every execution of the machine — the keys its orthogonal regions use
included — and a `with` may only read declared fields.

## `set` updates it

A transition may carry assignments, written after its trigger:

```text
from Working to Cooling on Fail set context.attempts = context.attempts + 1, context.last_code = event.code
```

- The left side is always `context.<field>`. A right side is a reference (`event.x`,
  `context.x`), a literal (`1`, `"text"`, `true`, `[1, 2]`), or **one** arithmetic operation
  (`+ - * /`) over two of those, on numbers. Anything more belongs in an action.
- Every right side is evaluated against the context and event **as the transition starts**, before
  any of them is applied; then the values are written between leaving the source and entering the
  target — an `on exit` sees the old context, an `on enter` sees the new one.
- An expression that can't be evaluated — a field that isn't set, arithmetic on a non-number,
  a division by zero, a value that breaks the declared type — is an `ExpressionError`, handled
  exactly like an action raising: an `on error` in scope takes it (`where type ==
  "ExpressionError"`), and without one the execution is dead-lettered. It fails before the
  transition leaves any state, so the recovery starts from where the execution was.

The execution trace records what each `set` wrote, as that step's `assigned`.

## `choose` routes on it

A `choose` is a transition whose destination is picked by guards, tried in order — the first that
holds wins, else the `else` branch:

```text
from Working choose on Fail set context.attempts = context.attempts + 1 {
  when context.attempts >= 3 to GaveUp
  else to Cooling
}
```

Without an `else`, a `choose` where no branch holds doesn't fire at all — as if its guard were
false. Without a trigger it is an automatic transition, routing as soon as its source is reached —
and re-evaluated after every later step taken in that state, so "wait until `context.ready`" works
as long as some event transition there can change the context. An automatic `choose` without
`else` in a state nothing can ever re-run (no event transition, no `on activity`) would wait
forever if no branch holds: `harel validate` reports it (`choose_can_hang`).
Its guards are evaluated when the transition is picked, so they see the context **before** the
transition's own `set`, as in UML — above, the fourth failure is the one that gives up.

A full example:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

JOB = """
event Fail { code: int }
event Recovered {}

machine job {
  context {
    attempts: int
    last_code: int?
  }

  initial Working
  state Working {}
  state Cooling {}
  final GaveUp failed {}

  from Working choose on Fail set context.attempts = context.attempts + 1, context.last_code = event.code {
    when context.attempts >= 3 to GaveUp
    else to Cooling
  }
  from Cooling to Working on Recovered
}
"""

defn = definition_from_dsl(JOB, "job", validate=True)
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id, context={"attempts": 0})
for code in (500, 502, 503, 504):
    exe = runner.process(exe.id, Event(kind="Fail", data={"code": code}))
    print(code, "->", exe.active_path, exe.context["attempts"], exe.context["last_code"])
    if exe.active_path == "Cooling":
        exe = runner.process(exe.id, Event(kind="Recovered"))
print(exe.status.name, exe.outcome)
```

```text
500 -> Cooling 1 500
502 -> Cooling 2 502
503 -> Cooling 3 503
504 -> GaveUp 4 504
DONE failed
```

## Orthogonal regions

Each region of an orthogonal state is its own execution, with its own context — which starts with
only what the orthogonal node passes down, exactly like an `invoke`'s `with`:

```text
orthogonal Checks {
  with { limit: max_amount }       # each region starts with context.limit = the parent's max_amount
  state Fraud { ... from Scoring to Flagged on Scored where context.limit < event.amount ... }
  state Stock { ... }
}
```

Nothing else is copied, so a region's guards see exactly what it was given, and a key it reports
back with `carry` is one it produced (or was given). See [orthogonal regions](08-orthogonal).

Next: [validation](14-validation) covers every rule above in one place.
