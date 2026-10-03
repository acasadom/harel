"""Actions for the place-order example.

Every action has the engine's contract ``(stm, event, **inputs)`` and may read
or mutate the order's context (``stm.execution_ctx``). Here they just record a
human-readable step in ``execution_ctx["history"]``, and the carrier selector picks
a shipping service. A real app would charge a card, reserve stock, call a carrier,
etc. (The payment retry needs no function: the model counts declines and decides.)
"""


def _record(stm, message: str) -> None:
    stm.execution_ctx.setdefault("history", []).append(message)


def on_cart(stm, event, **kw):
    _record(stm, "order created (cart)")


def request_payment(stm, event, **kw):
    _record(stm, "payment requested")


def on_retry(stm, event, **kw):
    ctx = stm.execution_ctx
    _record(stm, f"payment declined ({ctx.get('last_decline') or 'no reason'}) -> retrying")


def capture_payment(stm, event, **kw):
    if stm.execution_ctx.get("simulate_payment_error"):
        raise RuntimeError("gateway timeout — payment processor unreachable")
    _record(stm, "payment captured")


def start_fulfilment(stm, event, **kw):
    _record(stm, "fulfilment started")


def pick(stm, event, **kw):
    if stm.execution_ctx.get("simulate_fulfilment_error"):
        raise OSError("warehouse API unavailable")
    _record(stm, "items picked")


def pack(stm, event, **kw):
    _record(stm, "items packed")


def choose_carrier(stm, event, **kw):
    """Selector: pick the shipping service for the packed order.

    A real app would ask a shipping-rates API; here, heavy parcels and international
    destinations go standard, everything else express. Returns one of the branch keys
    the transition declares (`returns {"express", "standard"}`).
    """
    ctx = stm.execution_ctx
    domestic = ctx.get("destination", "ES") == "ES"
    return "express" if domestic and ctx.get("weight_kg", 1.0) <= 5 else "standard"


def book_express(stm, event, **kw):
    _record(stm, "express courier booked")


def book_standard(stm, event, **kw):
    _record(stm, "standard carrier booked")


def ship(stm, event, **kw):
    _record(stm, "shipped")


def deliver(stm, event, **kw):
    _record(stm, "delivered")


def cancel_order(stm, event, **kw):
    ctx = stm.execution_ctx
    reason = f" after {ctx['declines']} declines" if ctx.get("declines") else ""
    _record(stm, f"order cancelled{reason}")


def on_payment_error(stm, event, **kw):
    err = stm.execution_ctx.get("_error", {})
    _record(stm, f"payment error: {err.get('type')} — {err.get('message')}")


def on_fulfilment_error(stm, event, **kw):
    err = stm.execution_ctx.get("_error", {})
    _record(stm, f"fulfilment error: {err.get('type')} — {err.get('message')}")
