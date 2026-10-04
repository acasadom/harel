# Examples

Six runnable programs under [`examples/`](https://github.com/acasadom/harel/tree/main/examples),
each a small application built around one machine. Every one runs in CI (`test/unit/examples/`),
so they keep up with the engine. Each page says what the example does, shows its machine, walks
through how it is built, and draws its main path as a sequence diagram.

Each also says how it runs, along the two axes of [execution models](../guide/execution.md):
its **semantics** — does the caller wait for the result, or hand the event to a worker? — and
its **execution model** — the caller's own thread, a background event loop, or coroutines on
the caller's loop. Between them the examples cover each combination an application meets:

| example | what it is | semantics | execution model | why |
|---|---|---|---|---|
| [minimal](minimal) | the smallest machine, driven by a script | synchronous — `DurableRunner` | background (the default) | a script: nothing to choose |
| [place_order](place_order) | an order lifecycle: retries, a selector, `on error` | synchronous | background | scenarios replayed one after another |
| [webhook_payment](webhook_payment) | a payment driven by webhooks, with a durable timeout | **asynchronous** — `DistributedRunner` + a worker | background (FastAPI's `def` handlers run in a thread pool) | the webhook must answer at once; a worker advances the machine |
| [nicegui_wizard](nicegui_wizard) | a multi-step form that survives reloads and restarts | synchronous — `AsyncDurableRunner` | **coroutines** | NiceGUI calls its handlers on its event loop |
| [inline_transaction](inline_transaction) | an order whose state commits with the shop's own writes | synchronous | **inline** | one transaction for the shop's rows and the machine |
| [monitor_demo](monitor_demo) | seeds a store with executions in every state, for the monitor | synchronous, with the trace on | background | a one-off seeding script |

Run any of them from the repository root, for example:

```text
uv run python -m examples.minimal.run
```

```{toctree}
:maxdepth: 1

minimal
place_order
webhook_payment
nicegui_wizard
inline_transaction
monitor_demo
```
