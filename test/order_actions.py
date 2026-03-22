"""No-op action implementations for `test/data/order.stm`.

The order machine's actions are irrelevant to the *transition* behaviour (the tests
only assert the state reached after each event), so every hook is a no-op. The two
selectors return `True` so the order routes through the "needs carrier" / "carrier
confirmed" branches (the DSL maps the `true` branch by `str(result) == "True"`).

Lives at the test root so the DSL's dotted `order_actions.<fn>` action paths resolve
by bare-name import, like `scenarios` / `stm_actions`.
"""

from __future__ import annotations

from typing import Any


def _noop(stm: Any, event: Any, **kwargs: Any) -> None:
    return None


# state hooks (all no-ops — only the routing matters)
on_received = _noop
on_validating = _noop
record_status = _noop
on_reserving_stock = _noop
on_reserving_stock_exit = _noop
on_authorizing = _noop
on_charging = _noop
on_charging_activity = _noop
on_packing = _noop
on_shipping_enter = _noop
on_shipping_exit = _noop
on_prepare_label = _noop
on_notify_carrier = _noop
on_awaiting_carrier = _noop
on_carrier_confirmed = _noop
on_rejected = _noop
on_failed = _noop
on_cancelled = _noop
on_delivered = _noop


# selectors: drive the happy path (carrier needed, carrier confirms)
def needs_carrier(stm: Any, event: Any, **kwargs: Any) -> bool:
    return True


def carrier_confirmed(stm: Any, event: Any, **kwargs: Any) -> bool:
    return True
