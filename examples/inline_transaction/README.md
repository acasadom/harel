# Inline transaction — example

> How it works — the machine, how it is built, sequence diagrams, and why it runs the way it
> does: [the inline_transaction page](https://acasadom.github.io/harel/examples/inline_transaction.html) of the docs.

A shop whose orders commit in its own transaction: placing an order writes the shop's row, takes
the stock and starts the order machine, on one SQLite connection — and if the stock isn't there,
all of it rolls back. The runner is `DurableRunner(..., execution="inline",
on_action_error="raise")`, over `ConnectionStore`, a store written outside harel that writes
through the caller's connection and never commits on its own.

```bash
uv run python -m examples.inline_transaction.run
```

Files:

- `order.stm` — the machine: `Reserving` (takes the stock) → `AwaitingPayment` → `Paid`.
- `actions.py` — `reserve_stock`, on the shop's connection.
- `store.py` — `ConnectionStore`, built on `harel.engine.store.base` and checked against
  `harel.testing`'s contracts.
- `run.py` — the shop: its tables, harel's (`harel.engine.schema.store_schema`), and the
  transactions.
