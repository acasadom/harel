"""The order's action: take the stock, in the shop's own table, on the shop's connection.

The action runs in the caller's thread (`execution="inline"`), so it uses the same connection
— and so the same transaction — as the shop and the store. `connection` is set by the shop
when it opens its database.
"""

import sqlite3
from typing import Optional

connection: Optional[sqlite3.Connection] = None


class OutOfStock(Exception):
    pass


def reserve_stock(stm, event, **kw) -> None:
    item, qty = stm.execution_ctx["item"], stm.execution_ctx["qty"]
    assert connection is not None, "the shop sets the connection before running orders"
    taken = connection.execute(
        "UPDATE stock SET available = available - ? WHERE item = ? AND available >= ?", (qty, item, qty)
    ).rowcount
    if not taken:
        raise OutOfStock(f"not enough {item} for {qty}")
