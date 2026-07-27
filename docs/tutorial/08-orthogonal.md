# 8. Doing things at once: orthogonal regions

Until now the machine has been in exactly one state at a time. But some things genuinely happen
**in parallel**. When an order is placed, you might run a fraud screen *and* reserve stock at
the same time, and only proceed once both are done. That is an **orthogonal** state (an
AND-state): several regions, each its own sub-machine, running concurrently.

## An AND-state with two regions

```text
event FraudCleared {}
event StockReserved {}

machine checkout {
  initial Verifying
  orthogonal Verifying {
    state Fraud {
      initial Screening
      state Screening {}
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

```{mermaid}
stateDiagram-v2
    [*] --> Verifying
    state Verifying {
        [*] --> Screening
        Screening --> Cleared : FraudCleared
        --
        [*] --> Checking
        Checking --> Reserved : StockReserved
    }
    Verifying --> Approved
    Approved --> [*]
```

`orthogonal Verifying { … }` declares the AND-state. Each child — `Fraud`, `Stock` — is a
**region**: a full sub-machine with its own `initial` and its own terminal. The two run
independently.

## Fork, broadcast, join

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
event FraudCleared {}
event StockReserved {}

machine checkout {
  initial Verifying
  orthogonal Verifying {
    state Fraud {
      initial Screening
      state Screening {}
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

defn = definition_from_dsl(SOURCE, "checkout")
runner = DurableRunner(DictStore(), {defn.id: defn})

exe = runner.create(defn.id)
print("start         ->", exe.active_path, exe.status.name)

exe = runner.process(exe.id, Event(kind="FraudCleared"))
print("FraudCleared  ->", exe.active_path, exe.status.name)

exe = runner.process(exe.id, Event(kind="StockReserved"))
print("StockReserved ->", exe.active_path, exe.status.name, "/", exe.outcome)
```

```text
start         -> Verifying RUNNING
FraudCleared  -> Verifying RUNNING
StockReserved -> Approved DONE / success
```

Three things happen here:

- **Fork.** Entering `Verifying` starts both regions at once. Under the hood each region runs
  as its own child execution over the same definition — which is exactly what makes them
  durable and, later, distributable.
- **Broadcast.** Every event is delivered to *all* regions (Harel semantics). `FraudCleared`
  reaches both; `Fraud` has a transition for it and advances to `Cleared`, while `Stock`
  ignores it. The parent stays parked on `Verifying` the whole time.
- **Join.** The parent only leaves `Verifying` once **every** region has finished. After
  `StockReserved`, both regions have reached their terminal, so the automatic
  `from Verifying to Approved` fires and the order is `Approved`.

```{important}
Orthogonal regions are for *a fixed set of different concurrent activities* that all share the
event stream. They are **not** the tool for "run the same machine over N items" (ship N
parcels, deploy to N regions) — that is a data-parallel **fan-out invoke**, covered in
[step 12](12-fanout). Using an orthogonal state where you mean fan-out is a classic
mis-modelling.
```

## Region hooks and timeouts

A region composite (`Fraud`, `Stock`) is a first-class state and supports the same hooks as any
other state. Its `on enter` fires when the region starts (before descending to its initial child);
its `on exit` fires when the region finishes (before the parent's join resolves). A `timeout`
declared on the region composite arms a timer for the whole region:

```text
orthogonal Verifying {
  state Fraud {
    on enter start_fraud_check
    on exit  record_fraud_result
    timeout 30                       # the whole Fraud region must finish within 30 s
    initial Screening
    state Screening {}
    final Cleared success {}
    final TimedOut failed {}
    from Screening to Cleared  on FraudCleared
    from Screening to TimedOut on Timeout
  }
  …
}
```

The hook order follows UML: the region's own `on enter` runs first, then its initial child's.
On completion the region's own `on exit` runs after its terminal child's.

## Ending the block early

By default the orthogonal state is a **join**: it waits for *every* region to finish before it
leaves. Two declarations on the orthogonal node itself let it end before that:

- **`cancel_on_failure`** — as soon as *one* region finishes with a non-`success` outcome, the
  engine cancels the regions still running and resolves the join immediately. Use it when a single
  region's failure already decides the block (a fraud rejection makes the stock check moot):

  ```text
  orthogonal Verifying {
    cancel_on_failure                 # first failing region ends the block
    state Fraud { … }
    state Stock { … }
  }
  from Verifying join all to Approved else to Rejected
  ```

- **`timeout`** on the orthogonal node — a budget for the *whole* block. If it fires before the
  regions join, the still-running regions are cancelled and the node's own `on Timeout` transition
  fires:

  ```text
  orthogonal Verifying {
    timeout 60                        # the block as a whole must finish within 60 s
    state Fraud { … }
    state Stock { … }
    from Verifying to TimedOut on Timeout
  }
  ```

A cancelled region receives a `Cancel` event, so a region that models its own `on Cancel` cleanup
gets to run it before it stops. Without either declaration the block waits for all regions — the
plain join.

What each region *produced* — and how the join can route on it (all succeeded? any failed?) —
is the subject of [payloads](13-payloads). First, let's tackle reuse: the retry pattern from
[step 5](05-selectors) keeps reappearing. [Fragments](09-fragments) let us write it once.
