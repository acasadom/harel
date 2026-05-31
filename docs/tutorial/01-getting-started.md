# 1. Your first machine

We start with the smallest interesting state machine there is: a **turnstile**. It has two
states — `Locked` and `Unlocked` — and reacts to two events: inserting a `Coin` unlocks it;
a `Push` (someone walking through) locks it again.

This is the *hello world* of state machines. By the end of this page you will have authored a
machine in the DSL, run it, and driven it with events.

## Install

```bash
pip install harel
```

`harel` needs Python 3.11+. The core install has everything in this tutorial; optional
backends (Redis, Postgres, …) and the editor tooling are extras you add later.

## Author the machine

A machine is written in the `.stm` DSL. The transitions live **inside** the machine, each one
reading `from <state> to <state> on <Event>`:

```text
machine turnstile {
  initial Locked
  state Locked {}
  state Unlocked {}

  from Locked to Unlocked on Coin
  from Unlocked to Locked on Push
}
```

That is the whole program. Read top to bottom:

- `machine turnstile { … }` — declares a machine named `turnstile`.
- `initial Locked` — where execution starts.
- `state Locked {}` / `state Unlocked {}` — the two states. The empty braces `{}` mean "no
  behaviour attached yet" — we add that in [the next step](02-actions).
- `from Locked to Unlocked on Coin` — *when in `Locked`, a `Coin` event moves to `Unlocked`*.
- `from Unlocked to Locked on Push` — and back.

```{mermaid}
stateDiagram-v2
    [*] --> Locked
    Locked --> Unlocked : Coin
    Unlocked --> Locked : Push
```

## Run it

Here is the canonical way to run any machine in this tutorial: compile the DSL into a
`Definition`, hand it to a `DurableRunner` backed by a store, `create` an execution, then feed
it events with `process`. We use the in-memory `DictStore` here; swapping in SQLite, Redis, or
Postgres is a one-line change covered under [durability](../guide/durability).

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine turnstile {
  initial Locked
  state Locked {}
  state Unlocked {}

  from Locked to Unlocked on Coin
  from Unlocked to Locked on Push
}
"""

defn = definition_from_dsl(SOURCE, "turnstile")   # compile the DSL
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)                       # a fresh execution
print("start    ->", exe.active_path)

for kind in ["Coin", "Push", "Coin"]:
    exe = runner.process(exe.id, Event(kind=kind))
    print(f"{kind:<8} -> {exe.active_path}  ({exe.status.name})")
```

Running it prints:

```text
start    -> Locked
Coin     -> Unlocked  (RUNNING)
Push     -> Locked  (RUNNING)
Coin     -> Unlocked  (RUNNING)
```

## What just happened

- **`definition_from_dsl(SOURCE, "turnstile")`** parses and compiles the text into an
  immutable `Definition`. The `name` argument selects the machine; it is only required when a
  file declares more than one.
- **`DurableRunner(store, {defn.id: defn})`** is the headless runner. It takes a store (where
  executions are persisted) and a registry mapping each definition's `id` to its `Definition`.
- **`runner.create(defn.id)`** starts a new `Execution` and returns it. `exe.active_path` is
  the state it is currently in — `Locked`, the `initial` state.
- **`runner.process(exe.id, Event(kind=...))`** delivers one event and returns the updated
  execution. An event is just a `kind` (its name) plus optional `data` — more on data in
  [payloads](../guide/durability).
- **`exe.status`** is the lifecycle status. It is `RUNNING` the whole time here, because the
  turnstile never *finishes* — it just toggles forever.

A machine that never ends is fine, but most useful machines reach a conclusion: an order is
*delivered* or *cancelled*, a review is *approved* or *rejected*. That conclusion — a terminal
state and the **verdict** it carries — is the subject of [step 3](03-outcomes). First, in
[step 2](02-actions), we make the states actually *do* something.
