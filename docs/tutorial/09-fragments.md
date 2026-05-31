# 9. Reuse: parametrized fragments

The retry idea from [step 5](05-selectors) is generic: *attempt something, check the result,
loop on failure*. You don't want to hand-wire that into every state that needs it. A
**fragment** is a parametrized piece of machine you define once and splice in wherever you need
it, filling its blanks at the use site.

## Define once, parametrize the blanks

```text
fragment AttemptWithRetry(work: action, check: action) {
  initial Attempt
  state Attempt { on enter work }
  state Waiting {}
  state Done {}

  from Attempt select check on Result {
    "ok"   to Done
    "fail" to Waiting
  }
  from Waiting to Attempt
}
```

The parameters in `(…)` are the customizable surface. Each has a **kind**:

| Kind | Filled with | Used for |
| ---- | ----------- | -------- |
| `action` | a handler/literal | a hook or selector inside the fragment |
| `guard` | a predicate | a `where` condition |
| `state` | a state name | a transition target (jump out to the consumer's states) |
| `value` | a literal | a `timeout`, or an action input |
| `event` | an event name | the trigger of an `on <param>` transition |

Here `work` and `check` are `action` parameters: the consumer supplies *what to attempt* and
*how to judge the result*, and the loop structure is reused as-is.

## Use it, filling the parameters

`use <Fragment>(args) as <LocalName>` splices the fragment in as a child composite named
`<LocalName>`:

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
fragment AttemptWithRetry(work: action, check: action) {
  initial Attempt
  state Attempt { on enter work }
  state Waiting {}
  state Done {}

  from Attempt select check on Result {
    "ok"   to Done
    "fail" to Waiting
  }
  from Waiting to Attempt
}

machine order {
  initial Charging
  use AttemptWithRetry(work=charge, check=charge_result) as Charging
  final Paid success {}
  from Charging to Paid
}
"""

attempts = {"n": 0}


def charge(stm, event, **inputs):
    stm.execution_ctx.setdefault("charges", []).append("charge")


def charge_result(stm, event, **inputs):
    attempts["n"] += 1
    return "ok" if attempts["n"] >= 3 else "fail"   # succeed on the third try


defn = definition_from_dsl(SOURCE, "order", actions={"charge": charge, "charge_result": charge_result})
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
print("start ->", exe.active_path)
for i in (1, 2, 3):
    exe = runner.process(exe.id, Event(kind="Result"))
    print(f"Result #{i} ->", exe.active_path)

print("final:", exe.active_path, "/", exe.outcome, "| charge attempts:", len(exe.context["charges"]))
```

```text
start -> Charging.Attempt
Result #1 -> Charging.Attempt
Result #2 -> Charging.Attempt
Result #3 -> Paid
final: Paid / success | charge attempts: 3
```

Each failing `Result` routes to `Waiting`, which bounces straight back to `Attempt` — re-running
`charge`. On the third attempt `charge_result` returns `"ok"`, the fragment's `Done` sink
completes the `Charging` composite, and the consumer's `from Charging to Paid` fires.

Because the loop is parametrized, the *same* fragment can wrap any retryable step — charging a
card, calling a carrier, reserving stock — by using it again with different `work`/`check`
arguments. The repository's `retry.stm` is the full version of this pattern, adding a `value`
parameter for the backoff policy and a budget on the consumer.

So far everything has lived in one file. Real projects split machines across files —
[imports](10-imports) are next.
