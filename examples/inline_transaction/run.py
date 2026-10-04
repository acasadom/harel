"""An order that commits with the shop's own writes — `DurableRunner(execution="inline")`.

    uv run python -m examples.inline_transaction.run

The shop keeps its data in one SQLite database: its `orders` and `stock` tables, and harel's
tables next to them (created from `harel.engine.schema.store_schema`). Placing an order is one
transaction: the shop inserts its row, and the machine starts — its action takes the stock,
and its state is written by `ConnectionStore`, all on the shop's connection. If the stock
isn't there, the action raises (`on_action_error="raise"`), and all of it rolls back: no order
row, no stock taken, no execution.
"""

import sqlite3
from pathlib import Path

from examples.inline_transaction import actions
from examples.inline_transaction.store import ConnectionStore
from harel import DurableRunner, Event, definition_from_dsl_file
from harel.engine.schema import store_schema

ORDER_STM = Path(__file__).parent / "order.stm"


def open_shop(path: str = ":memory:") -> tuple[sqlite3.Connection, DurableRunner]:
    conn = sqlite3.connect(path)
    with conn:  # the shop's schema, and harel's next to it
        conn.execute("CREATE TABLE IF NOT EXISTS stock (item TEXT PRIMARY KEY, available INT NOT NULL)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, item TEXT, qty INT, status TEXT)"
        )
        for statement in store_schema("sqlite"):
            conn.execute(statement)
        conn.execute("INSERT OR IGNORE INTO stock VALUES ('widget', 5)")
    actions.connection = conn
    defn = definition_from_dsl_file(ORDER_STM, "order", validate=True)
    runner = DurableRunner(
        ConnectionStore(conn), {defn.id: defn}, execution="inline", on_action_error="raise"
    )
    return conn, runner


def place_order(conn: sqlite3.Connection, runner: DurableRunner, order_id: str, item: str, qty: int) -> None:
    """One transaction: the shop's row and the machine's start — or neither."""
    with conn:  # commits on success, rolls back on an exception
        conn.execute("INSERT INTO orders VALUES (?, ?, ?, 'placed')", (order_id, item, qty))
        runner.create("order", context={"item": item, "qty": qty}, execution_id=order_id)


def pay(conn: sqlite3.Connection, runner: DurableRunner, order_id: str) -> None:
    with conn:
        runner.process(order_id, Event(kind="Pay"))
        conn.execute("UPDATE orders SET status = 'paid' WHERE id = ?", (order_id,))


def show(conn: sqlite3.Connection, runner: DurableRunner, order_id: str) -> None:
    row = conn.execute("SELECT status FROM orders WHERE id = ?", (order_id,)).fetchone()
    exe = runner.store.load(order_id)
    (stock,) = conn.execute("SELECT available FROM stock WHERE item = 'widget'").fetchone()
    print(
        f"  {order_id}: order row = {row[0] if row else '—'}, "
        f"machine = {exe.active_path if exe else '—'}, widgets left = {stock}"
    )


def main() -> None:
    conn, runner = open_shop()

    print("place 2 widgets:")
    place_order(conn, runner, "order-1", "widget", 2)
    show(conn, runner, "order-1")

    print("place 9 widgets (only 3 left):")
    try:
        place_order(conn, runner, "order-2", "widget", 9)
    except actions.OutOfStock as exc:
        print(f"  refused: {exc} — and nothing of it was kept")
    show(conn, runner, "order-2")

    print("pay order-1:")
    pay(conn, runner, "order-1")
    show(conn, runner, "order-1")


if __name__ == "__main__":
    main()
