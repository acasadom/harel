"""Actions for the webhook_payment example.

In a real app: call a fulfillment service, send an email, update inventory, etc.
Here they write a human-readable log into the execution context.
"""


def _log(stm, msg: str) -> None:
    stm.execution_ctx.setdefault("log", []).append(msg)


def start_fulfillment(stm, event, **kw):
    payment_id = event.data.get("payment_id", "")
    amount = event.data.get("amount", 0)
    _log(stm, f"fulfillment started — payment {payment_id}, {amount} cents")


def on_done(stm, event, **kw):
    _log(stm, "order complete")


def on_failed(stm, event, **kw):
    reason = event.data.get("reason", "unknown")
    _log(stm, f"payment failed: {reason}")


def on_expired(stm, event, **kw):
    _log(stm, "timed out — no payment received within the window")
