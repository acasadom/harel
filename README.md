# harel

*Durable, distributed statecharts for Python.*

[![CI](https://github.com/acasadom/harel/actions/workflows/ci.yml/badge.svg)](https://github.com/acasadom/harel/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)

**A hierarchical statechart engine for Python — with the durability and distribution
of a workflow engine, but the right model underneath.**

You author a machine in a small textual DSL (`.stm`), it compiles to an immutable
`Definition`, and a pure, effects-based engine runs it against a serializable
`Execution` that can be persisted, distributed across workers, and survive crashes.

---

## Why this exists

In ten years across different companies I kept hitting the **same** problem: a thing
that was really a *hierarchical state machine* — an order, a subscription, a device, a
claim, a deployment — got called a "pipeline", a "workflow", or just "a graph". So it
got built with the tool that matched the *name* instead of the *shape*: a DAG runner, a
task queue, a pile of `if status == ...` branches.

The result was always the same: states implied but never named, transitions scattered
across the codebase, illegal states reachable, and "creative" retry/cancel logic that
was hell to debug, follow, or extend.

A statechart is the correct model for that shape — hierarchy, orthogonal regions,
guarded transitions, explicit terminal verdicts — and it has been since Harel, 1987. What
was missing in Python was a statechart engine that is *also* **durable and distributed**:

- **sismic** models statecharts well but is in-memory only.
- **Temporal** / **DBOS** give you durable execution, but model work as *imperative code*,
  not as a declarative statechart.
- **XState** is the gold standard for statecharts — but it's JavaScript.

`harel` sits in that gap: **the statechart as the model, durability and distribution as
the runtime.** [See the comparison below.](#where-it-fits)

---

## Install

```bash
pip install harel        # core: DSL + engine + in-memory/sqlite durability
# optional backends, pick what you need:
pip install "harel[redis]"     # Redis store + transport
pip install "harel[postgres]"  # Postgres store + transport
pip install "harel[mongo]"     # MongoDB store + transport
pip install "harel[surrealdb]" # SurrealDB store + transport
pip install "harel[dynamodb]"  # DynamoDB store (pairs with sqs for an all-AWS stack)
pip install "harel[sqs]"       # AWS SQS FIFO transport
pip install "harel[lsp]"       # DSL language server (editor tooling)
```

Requires Python 3.11+.

## Quickstart

Scaffold a starter machine that validates and runs out of the box — zero to a working
state machine in one command:

```bash
harel new approval.stm
harel run approval.stm -e Submit -e Approve   # (start) -> Draft -> Review -> Approved
```

Or author one yourself — transitions live **inside** the state they leave, events are named,
terminals declare their verdict:

```
machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success
  final Cancelled cancelled

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on CancelOrder
  from Paid to Delivered on Delivered
}
```

Run it through the headless durable runner (here over an in-memory store):

```python
from harel import definition_from_dsl, DurableRunner, DictStore, Event

SOURCE = """
machine order {
  initial Cart
  state Cart {}
  state AwaitingPayment {}
  state Paid {}
  final Delivered success
  final Cancelled cancelled

  from Cart to AwaitingPayment on PlaceOrder
  from AwaitingPayment to Paid on PaymentAuthorized
  from AwaitingPayment to Cancelled on CancelOrder
  from Paid to Delivered on Delivered
}
"""

defn = definition_from_dsl(SOURCE, "order")          # compile the DSL
runner = DurableRunner(DictStore(), {defn.id: defn})  # swap DictStore for Sqlite/Redis/Postgres

exe = runner.create(defn.id)
for kind in ["PlaceOrder", "PaymentAuthorized", "Delivered"]:
    exe = runner.process(exe.id, Event(kind=kind))
    print(f"{kind} -> {exe.active_path}")

print(exe.status.name, exe.outcome)
```

```text
PlaceOrder -> AwaitingPayment
PaymentAuthorized -> Paid
Delivered -> Delivered
DONE success
```

A complete, runnable example (nested states, a selector-driven retry, actions) lives in
[`examples/place_order/`](examples/place_order/) — run it with
`uv run python -m examples.place_order.run`.

## Documentation

A step-by-step **tutorial** — 14 stages that grow one example from a turnstile to durable,
distributed submachines — plus an operations and reference guide live under
[`docs/`](docs/). Build the HTML locally with `make docs`. Every code example in the docs is
executed in CI, so it stays in sync with the engine.

---

## Where it fits

|                         | harel | sismic | transitions | XState | Temporal / DBOS |
| ----------------------- | :------: | :----: | :---------: | :----: | :-------------: |
| Hierarchical statechart |    ✅    |   ✅   |  partial    |   ✅   |       ❌        |
| Orthogonal regions      |    ✅    |   ✅   |     ❌      |   ✅   |       ❌        |
| Declarative model       |    ✅    |   ✅   |     ✅      |   ✅   |   ❌ (code)     |
| Durable / crash-safe    |    ✅    |   ❌   |     ❌      | partial|       ✅        |
| Distributed workers     |    ✅    |   ❌   |     ❌      |   ❌   |       ✅        |
| Language                |  Python  | Python |   Python    |  JS/TS |   Go/Java/…     |

Use a statechart when the domain **is** a machine of named states with hierarchy, guarded
transitions and explicit terminal verdicts. If your domain is genuinely "run these steps in
order with retries", a workflow engine is the better fit — this is not trying to replace one.

---

## What's in the box

- **A textual DSL** (`.stm`, parsed with [lark](https://github.com/lark-parser/lark)) that
  reads like a spec: nested composite states, orthogonal regions, guarded transitions,
  named guards, computed `select` branches, parametrized `fragment`s, `import`s, and
  black-box `invoke` of sub-machines (including data-parallel fan-out).
- **A pure, effects-based engine**: `start`/`process` are generators that *describe* effects
  (run an action, emit an event, spawn regions) and mutate a serializable `Execution`. No IO,
  no user code inside the engine — which makes runs deterministic and trivially testable.
- **Durable & distributed execution**: optimistic-concurrency (CAS) checkpointing, a
  transactional outbox, event dedupe, durable timers, and a control plane
  (`cancel`/`suspend`/`resume`/`terminate`). Stores: in-memory, SQLite, Redis, Postgres,
  rqlite, MongoDB, SurrealDB, DynamoDB. Transports: in-memory, SQLite, Redis, Postgres,
  rqlite, MongoDB, SurrealDB, SQS — mix freely.
- **Static validation** (`validate`): unreachable states, non-deterministic transitions,
  unresolved selector targets, missing terminal verdicts, timeout shape — surface-independent,
  run it before you execute.
- **Editor tooling**: a DSL language server (diagnostics, hover, go-to-definition,
  completion across imports) and a **VSCode extension** with a live Mermaid statechart preview.
- **Visualization**: render any machine to PlantUML or Mermaid.

## Design principle: it's a statechart, not a job engine

The engine schedules; the **model decides**. Retry/backoff is a composite with a `timeout`
and a selector — not an engine feature. Cancellation is a modelled `on Cancel` transition when
the machine wants to own its cleanup. There is no hidden "default to success/failed" — a
validation rule *forces* you to declare the terminal verdict where it's consumed, instead of
the engine guessing. Policy stays in the model; the engine stays small.

## Status

Beta. The core engine, DSL, durability/distribution layers and tooling are in place and
covered by an extensive test suite (run `uv run pytest`).

**Roadmap:** remote action execution — running a state's actions on FaaS (AWS Lambda, Spin,
…). The architecture already has the seam for it: actions are *effects* the runner resolves,
so a remote executor is a new runner/resolver, not an engine change.

## Development

```bash
uv sync                 # create/refresh the venv (Python 3.13)
uv run pytest           # run the suite
make lint               # ruff + .stm formatter check
make type-check         # mypy
```

## The name

Named after **David Harel**, who introduced statecharts in 1987 — the formalism this engine
makes durable and distributed.

## License

[Apache License 2.0](LICENSE) © Alberto Casado
