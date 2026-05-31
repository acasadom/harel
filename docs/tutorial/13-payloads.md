# 13. Passing data across boundaries

A region or a submachine doesn't just reach a verdict — it usually produces *data* the caller
needs. A fraud screen yields a risk score; a payment yields an auth code. harel moves that
data across execution boundaries with **`carry`**, and exposes it to the joining parent as
**`region_results`**.

## `carry`: a region's return value

By default a finishing region reports only its `outcome`. List extra context keys with `carry`
and they ride along too. Here the `Fraud` region computes a `score` and carries it:

```text
machine checkout {
  initial Verifying
  orthogonal Verifying {
    state Fraud {
      carry score
      initial Screening
      state Screening { on enter set_score }
      final Cleared success {}
      from Screening to Cleared on FraudCleared
    }
    state Stock {
      initial Checking
      state Checking {}
      final Reserved success {}
      from Checking to Reserved on StockReserved
    }
  }
  final Approved success {}
  from Verifying to Approved
}
```

## `region_results` at the join

When the parent joins, it finds `context["region_results"]` — a map from each region's path to
its `outcome` plus whatever it carried:

```python
import json

from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine checkout {
  initial Verifying
  orthogonal Verifying {
    state Fraud {
      carry score
      initial Screening
      state Screening { on enter set_score }
      final Cleared success {}
      from Screening to Cleared on FraudCleared
    }
    state Stock {
      initial Checking
      state Checking {}
      final Reserved success {}
      from Checking to Reserved on StockReserved
    }
  }
  final Approved success {}
  from Verifying to Approved
}
"""


def set_score(stm, event, **inputs):
    stm.execution_ctx["score"] = 87


defn = definition_from_dsl(SOURCE, "checkout", actions={"set_score": set_score})
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
exe = runner.process(exe.id, Event(kind="FraudCleared"))
exe = runner.process(exe.id, Event(kind="StockReserved"))

print("final:", exe.active_path, "/", exe.outcome)
print("region_results:", json.dumps(exe.context["region_results"], sort_keys=True))
```

```text
final: Approved / success
region_results: {"Verifying.Fraud": {"outcome": "success", "score": 87}, "Verifying.Stock": {"outcome": "success"}}
```

The `Fraud` region carried its `score`; `Stock` carried nothing, so only its `outcome` shows.
A `select` on the join can read `region_results` to route — e.g. *all cleared with score ≥ 80 →
fast-track, otherwise → manual review*. The same `outcome`-plus-`carry` payload is what a single
[`invoke`](11-invoke) hands back on its `Returned` event.

```{note}
The engine **does not aggregate** a verdict for you. After a join the parent's outcome is
whatever terminal *it* routes to — there is no hidden "all regions succeeded ⇒ success" default.
Policy stays in the model (a `select`/`join` you write), which is why a [validation
rule](14-validation) makes you declare the verdict rather than the engine guessing.
```

## Data going *in*

Two ways to seed an execution's context:

- **At creation** — `runner.create(defn.id, context={...})`, which we used for the fan-out's
  `parcels` and the submachine's `amount`.
- **On a `Start` event with data** — `Event(kind="Start", data={...})` seeds the context as the
  machine starts.

And on the way out under cancellation, `cancel(reason={...})` attaches an opaque payload to the
cooperative `Cancel` event, readable by the machine's own `on Cancel` cleanup — that's the
[control plane](../guide/control-plane).

One capability remains before we leave modelling: making the compiler catch your mistakes.
[Static validation](14-validation) is the final step.
