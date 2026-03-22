"""Actions for the place-order example.

Every action has the engine's contract ``(stm, event, **inputs)`` and may read
or mutate the order's context (``stm.execution_ctx``). Here they just record a
human-readable step in ``execution_ctx["history"]`` (and the payment selector
decides whether to retry). A real app would charge a card, reserve stock, call a
carrier, etc.
"""


def _record(stm, message: str) -> None:
    stm.execution_ctx.setdefault("history", []).append(message)


def on_cart(stm, event, **kw):
    _record(stm, "order created (cart)")


def request_payment(stm, event, **kw):
    _record(stm, "payment requested")


def payment_retry(stm, event, **kw):
    """Selector: count the attempt and decide whether to retry or give up.

    Returns a branch key ("retry"/"giveup") that the transition's mapper turns
    into the next state.
    """
    attempts = stm.execution_ctx.get("attempts", 0) + 1
    stm.execution_ctx["attempts"] = attempts
    decision = "retry" if attempts < stm.execution_ctx.get("max_attempts", 2) else "giveup"
    _record(stm, f"payment declined (attempt {attempts}) -> {decision}")
    return decision


def on_retry(stm, event, **kw):
    _record(stm, "scheduling a payment retry")


def capture_payment(stm, event, **kw):
    _record(stm, "payment captured")


def start_fulfilment(stm, event, **kw):
    _record(stm, "fulfilment started")


def pick(stm, event, **kw):
    _record(stm, "items picked")


def pack(stm, event, **kw):
    _record(stm, "items packed")


def ready(stm, event, **kw):
    _record(stm, "ready to ship")


def ship(stm, event, **kw):
    _record(stm, "shipped")


def deliver(stm, event, **kw):
    _record(stm, "delivered")


def cancel_order(stm, event, **kw):
    _record(stm, "order cancelled")
