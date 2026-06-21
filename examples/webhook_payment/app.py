"""Webhook-driven payment flow — harel + FastAPI.

    pip install -r examples/webhook_payment/requirements.txt
    uvicorn examples.webhook_payment.app:app        # API server
    python -m examples.webhook_payment.worker       # event + timer worker (separate process)

Three endpoints:

  POST /orders           — start a new payment flow; returns {id, state, status, log}
  POST /webhooks/stripe  — enqueue the webhook event; returns 204 immediately
  GET  /orders/{id}      — poll current state (eventually consistent after a webhook)

What this example shows:

  Enqueue, don't process — the webhook endpoint does not call runner.process() inline.
  It publishes the event to the transport (SqliteTransport) and returns 204 immediately.
  The worker (worker.py) consumes it asynchronously.  The API server stays fast regardless
  of how slow the actions are.

  Eventual consistency — GET /orders/{id} may still show the pre-webhook state for a
  few milliseconds while the worker processes the event.  This is acceptable here: Stripe
  does not poll the order state after delivering a webhook; neither does the user (no one
  is waiting on the response of the webhook POST).  If your protocol requires immediate
  consistency after a POST, process inline instead.

  Idempotency — Stripe retries failed deliveries for up to 72 h.
  Passing Event(id=stripe_event["id"]) is all it takes: the engine deduplicates on that
  id at the store level, so a retried delivery is a safe no-op.

  Durable timeout — `timeout 15` in payment.stm fires a Timeout event if no webhook
  arrives, even across process restarts.  Handled by the worker — no cron, no beat task.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from harel import Event, SqliteStore, definition_from_dsl_file
from harel.engine.distributed import DistributedRunner
from harel.engine.transport.sqlite import SqliteTransport

# ---------------------------------------------------------------------------
# Bootstrap — store and transport share the same .db file (WAL mode is safe)
# ---------------------------------------------------------------------------

PAYMENT_STM = Path(__file__).parent / "payment.stm"
DB_PATH = Path(__file__).parent / "payments.db"

defn = definition_from_dsl_file(PAYMENT_STM, "payment")
store = SqliteStore(DB_PATH)
transport = SqliteTransport(DB_PATH)
runner = DistributedRunner(store, transport, {defn.id: defn})

app = FastAPI(title="harel · webhook payment demo")

# ---------------------------------------------------------------------------
# Domain API
# ---------------------------------------------------------------------------


class OrderOut(BaseModel):
    id: str
    state: str
    status: str
    log: list[str]


def _out(exe) -> OrderOut:
    return OrderOut(
        id=exe.id,
        state=exe.active_path,
        status=exe.status.name,
        log=exe.context.get("log", []),
    )


@app.post("/orders", response_model=OrderOut, status_code=201)
def create_order():
    """Start a new payment flow.

    create() runs the initial start() inline and returns the execution already in
    AwaitingPayment — no worker round-trip needed to get the first state.
    Store the returned id in your Stripe PaymentIntent metadata so the webhook
    handler can route events back to this Execution.
    """
    exe = runner.create(defn.id)
    return _out(exe)


@app.get("/orders/{order_id}", response_model=OrderOut)
def get_order(order_id: str):
    """Return current state. Eventual consistency: the state may lag a few ms
    after a webhook while the worker processes the enqueued event."""
    exe = store.load(order_id)
    if exe is None:
        raise HTTPException(404, "order not found")
    return _out(exe)


# ---------------------------------------------------------------------------
# Stripe webhook receiver — enqueue only, do not process inline
# ---------------------------------------------------------------------------

_KIND: dict[str, str] = {
    "payment_intent.succeeded": "PaymentSucceeded",
    "payment_intent.payment_failed": "PaymentFailed",
}


@app.post("/webhooks/stripe", status_code=204)
def stripe_webhook(payload: dict):
    """Map a Stripe webhook payload to a harel Event and publish it to the
    transport.  Returns 204 before the event is processed — the worker picks
    it up asynchronously.

    Production checklist:
    - Validate the Stripe-Signature header with stripe.WebhookSignature.verify_header()
    - Serve over HTTPS only
    - Return 204 for unrecognised event types (stops Stripe from retrying)
    """
    kind = _KIND.get(payload.get("type", ""))
    if kind is None:
        return

    pi = payload.get("data", {}).get("object", {})
    order_id = (pi.get("metadata") or {}).get("harel_order_id")
    if not order_id:
        return

    data: dict = {}
    if kind == "PaymentSucceeded":
        data = {"payment_id": pi.get("id", ""), "amount": pi.get("amount_received", 0)}
    elif kind == "PaymentFailed":
        data = {"reason": ((pi.get("last_payment_error") or {}).get("message", "unknown"))}

    # id = Stripe event id → duplicate retries are no-ops (engine-level dedup)
    runner.send(order_id, Event(kind=kind, id=payload.get("id"), data=data))
