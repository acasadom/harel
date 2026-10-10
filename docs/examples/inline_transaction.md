# inline_transaction — one transaction for the shop and the machine

## What it does

A shop keeps its data in one SQLite database: its `orders` and `stock` tables, and harel's next
to them. Placing an order is a single transaction that writes three things: the shop's order
row, the stock the order takes, and the order machine's state. If there isn't enough stock, all
three roll back — no order row, no stock taken, no half-started machine. Paying updates the
shop's row and advances the machine, again together:

```text
place 2 widgets:
  order-1: order row = placed, machine = AwaitingPayment, widgets left = 3
place 9 widgets (only 3 left):
  refused: not enough widget for 9 — and nothing of it was kept
  order-2: order row = —, machine = —, widgets left = 3
pay order-1:
  order-1: order row = paid, machine = Paid, widgets left = 3
```

## The machine

<!-- machine: examples/inline_transaction/order.stm order -->
```{mermaid}
stateDiagram-v2
[*] --> Reserving
state "Reserving" as Reserving
Reserving : on enter#58; reserve_stock
state "Paid" as Paid
Paid : outcome#58; success
Reserving --> AwaitingPayment
AwaitingPayment --> Paid : Pay
Paid --> [*]
```

Entering `Reserving` runs `reserve_stock`, which takes the stock in the shop's own table and
raises `OutOfStock` when there isn't enough.

## How it is built

- **`store.py` — `ConnectionStore`**, an `ExecutionStore` written outside harel. harel's own
  `SqliteStore` opens its connection and commits every write itself; this one writes through the
  caller's connection and never commits or rolls back — the caller's transaction does. It is
  built from the public pieces: the protocol's records and helpers from
  `harel.engine.store.base`, the table names from `harel.engine.schema.Names`, and it passes the
  same contracts harel's stores do (`harel.testing`). See [writing your own
  backend](../guide/stores.md#writing-your-own-backend).
- **`run.py` — the shop.** `open_shop` creates the shop's tables and harel's —
  `store_schema("sqlite")`, the schema as data (see [naming and schema
  ownership](../guide/stores.md#naming-and-schema-ownership)) — and builds the runner:

  ```text
  DurableRunner(ConnectionStore(conn), definitions, execution="inline", on_action_error="raise")
  ```

  `place_order` is one `with conn:` block — a transaction that commits on success and rolls
  back on an exception — around the shop's `INSERT` and `runner.create(...)`.
- **`actions.py`** — `reserve_stock`, which runs on the same connection.

An order refused, rolled back as one:

```{mermaid}
sequenceDiagram
  participant Shop as shop (one thread, one connection)
  participant R as DurableRunner (inline)
  participant A as reserve_stock
  participant DB as SQLite (one transaction)
  Shop->>DB: INSERT INTO orders ...
  Shop->>R: create("order", item, qty = 9)
  R->>DB: the execution's writes (ConnectionStore)
  R->>A: on enter Reserving
  A->>DB: UPDATE stock ... (0 rows: not enough)
  A--xR: raises OutOfStock
  R--xShop: OutOfStock (on_action_error = "raise")
  Shop->>DB: ROLLBACK — the order row, the stock, the execution
```

## How it runs

- **Semantics: synchronous** — `DurableRunner`: the shop needs the order placed, or refused,
  before its transaction ends.
- **Execution model: inline.** The connection, and so the transaction, belong to the shop's
  thread. On the background loop the store would run on another thread, outside the shop's
  transaction; inline, every store call and every action runs right there, on the shop's
  connection. A coroutine action would be refused: there is no loop to await it on.
- **`on_action_error="raise"`.** With the default, `"fail"`, a raising action dead-letters the
  execution, and that step is committed — inside the shop's transaction, which would then commit
  a failed order next to the shop's row. `"raise"` lets the exception reach the shop, whose
  transaction rolls back.

The same shape fits a sync web view whose framework keeps a connection per thread, or a batch
job that must not leave half a unit of work behind. A `DistributedRunner(..., execution="inline")`
does the same for queuing work: the execution and its queued `Start` commit with the caller's
writes, and a worker runs it afterwards.

## Run it

```text
uv run python -m examples.inline_transaction.run
```
