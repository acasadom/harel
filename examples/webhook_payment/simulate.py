"""Demo script — sends fake Stripe webhook payloads against the local server.

    # Start the server first:
    uvicorn examples.webhook_payment.app:app

    # Then in another terminal:
    python -m examples.webhook_payment.simulate           # happy path
    python -m examples.webhook_payment.simulate fail      # payment declined
    python -m examples.webhook_payment.simulate timeout   # create order, wait ~15 s for Timeout
    python -m examples.webhook_payment.simulate dedup     # happy path + send same webhook twice

Uses only stdlib (urllib) — no extra deps beyond what the server needs.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE = "http://localhost:8000"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _request(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(f"{BASE}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "detail": e.read().decode()}


def post(path: str, body: dict | None = None) -> dict:
    return _request("POST", path, body)


def get(path: str) -> dict:
    return _request("GET", path)


# ---------------------------------------------------------------------------
# Fake Stripe event builders
# ---------------------------------------------------------------------------


def _stripe_event(event_type: str, order_id: str, **pi_fields) -> dict:
    """Build a minimal fake Stripe event payload (same shape as the real one)."""
    return {
        "id": f"evt_{uuid.uuid4().hex[:16]}",
        "type": event_type,
        "data": {
            "object": {
                "id": f"pi_{uuid.uuid4().hex[:16]}",
                "metadata": {"harel_order_id": order_id},
                **pi_fields,
            }
        },
    }


def succeeded_event(order_id: str, amount: int = 4999) -> dict:
    return _stripe_event("payment_intent.succeeded", order_id, amount_received=amount)


def failed_event(order_id: str, reason: str = "Your card was declined.") -> dict:
    return _stripe_event(
        "payment_intent.payment_failed",
        order_id,
        last_payment_error={"message": reason},
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def _show(label: str, order: dict) -> None:
    print(f"  {label:<30} state={order.get('state')!r}  status={order.get('status')!r}")
    for entry in order.get("log", []):
        print(f"    · {entry}")


def _wait(order_id: str, until_state_changes_from: str, timeout: float = 2.0) -> dict:
    """Poll until the order leaves `until_state_changes_from` or the timeout expires.

    The webhook endpoint returns 204 immediately (enqueue, not inline-process).
    The worker picks up the event asynchronously, so there is a brief gap before
    the state is visible on GET /orders/{id}.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        current = get(f"/orders/{order_id}")
        if current.get("state") != until_state_changes_from:
            return current
        time.sleep(0.05)
    return get(f"/orders/{order_id}")


def happy_path() -> None:
    print("\n=== happy path ===")
    order = post("/orders")
    _show("created", order)

    webhook = succeeded_event(order["id"], amount=4999)
    post("/webhooks/stripe", webhook)
    _show("after PaymentSucceeded", _wait(order["id"], until_state_changes_from="AwaitingPayment"))


def failure_path() -> None:
    print("\n=== failure path ===")
    order = post("/orders")
    _show("created", order)

    webhook = failed_event(order["id"])
    post("/webhooks/stripe", webhook)
    _show("after PaymentFailed", _wait(order["id"], until_state_changes_from="AwaitingPayment"))


def timeout_path() -> None:
    print("\n=== timeout path (waiting ~15 s for the durable Timeout to fire) ===")
    order = post("/orders")
    _show("created", order)

    for elapsed in range(1, 20):
        time.sleep(1)
        current = get(f"/orders/{order['id']}")
        if current.get("status") != "RUNNING":
            _show(f"after {elapsed} s", current)
            return
        if elapsed % 5 == 0:
            print(f"  ... {elapsed} s elapsed, still {current.get('state')!r}")

    _show("final", get(f"/orders/{order['id']}"))


def dedup_path() -> None:
    print("\n=== idempotency — same webhook delivered twice ===")
    order = post("/orders")
    _show("created", order)

    webhook = succeeded_event(order["id"])
    event_id = webhook["id"]

    post("/webhooks/stripe", webhook)
    after_first = _wait(order["id"], until_state_changes_from="AwaitingPayment")
    _show(f"after first delivery  (id={event_id[:12]}…)", after_first)

    post("/webhooks/stripe", webhook)  # exact same payload
    time.sleep(0.2)  # give the worker time to attempt processing the duplicate
    _show(f"after second delivery (id={event_id[:12]}…)", get(f"/orders/{order['id']}"))
    print("  (state unchanged — the duplicate was a no-op)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_SCENARIOS = {
    "happy": happy_path,
    "fail": failure_path,
    "timeout": timeout_path,
    "dedup": dedup_path,
}

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "happy"
    fn = _SCENARIOS.get(mode)
    if fn is None:
        print(f"unknown mode {mode!r}. choices: {', '.join(_SCENARIOS)}")
        sys.exit(1)
    fn()
