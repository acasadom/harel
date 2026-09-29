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

## A typed context

A machine may declare its context's fields, with the same field syntax as an event:

```text
machine job {
  context {
    attempts: int
    last_code: int?        # optional
  }
  ...
}
```

The declaration is checked at three points:

- **when an execution is created** — a required field missing, or a declared field of the wrong
  type, raises `ContextError` (with `DistributedRunner.create(..., start_on_create=False)`, the
  required fields may still come with `start(data=...)`, and are checked there);
- **by `harel validate`** — a guard or a `set` naming an undeclared field, or a `set` whose value
  can't be of the declared type, is an error;
- **after each `set`** — see below.

Undeclared keys are not rejected at runtime: actions may keep their own keys next to the declared
ones.

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
false. Without a trigger it is an automatic transition, routing as soon as its source is reached.
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

Each region of an orthogonal state is its own execution. It starts from a **copy** of the parent's
context as it is at the fork — so a region's guards see what the parent knew — and from then on the
two evolve apart: a region reports back through `carry` (see [orthogonal regions](08-orthogonal)).
An `invoke`d machine still receives only what its `with` passes.

Next: [validation](14-validation) covers every rule above in one place.
