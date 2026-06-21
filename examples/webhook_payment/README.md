# Webhook-driven payment flow — example (harel + FastAPI)

A payment lifecycle driven entirely by **Stripe webhook events**, modelled as a
statechart. The API creates one `Execution` per payment intent; each incoming
webhook is mapped to a harel `Event` and advances the machine.

```bash
pip install -r examples/webhook_payment/requirements.txt
uvicorn examples.webhook_payment.app:app          # terminal 1 — API server
python -m examples.webhook_payment.worker         # terminal 2 — timer worker
```

```bash
# In a third terminal — no Stripe account needed:
python -m examples.webhook_payment.simulate           # happy path
python -m examples.webhook_payment.simulate fail      # payment declined
python -m examples.webhook_payment.simulate timeout   # wait ~15 s for abandonment
python -m examples.webhook_payment.simulate dedup     # same webhook delivered twice
```

## What this example shows

### Idempotency for free

Stripe retries failed webhook deliveries for up to 72 hours. With traditional
approaches you need a separate idempotency table. With harel, passing
`Event(id=stripe_event["id"])` is all it takes — the engine deduplicates on
that id at the store level, so a retried delivery is a safe no-op.

Run `simulate dedup` to see it: the order stays in `Done` after the second
delivery of the same event.

### Durable timeout without a cron job

`timeout 15` in `payment.stm` arms a durable timer when the machine enters
`AwaitingPayment`. If no webhook arrives within the window the machine moves
to `Expired`, even across process restarts. No celery-beat, no cron, no
separate cleanup job — the timer is committed in the same transaction as the
execution state.

Timer firing runs in **`worker.py`**, a separate process from the API server.
This keeps the two concerns cleanly apart: the API server only does request/response
work; the worker sweeps due timers every 5 s without ever blocking a webhook response.
Change `timeout 15` to `timeout 600` (10 min) for production.

### Enqueue, don't process inline

`POST /webhooks/stripe` calls `runner.send()`, which is a thin
`transport.publish(execution_id, event)`. It returns 204 before the event is
processed. The worker picks it up asynchronously.

`GET /orders/{id}` is **eventually consistent** after a webhook: the state may
lag a few milliseconds while the worker processes. This is acceptable here
because Stripe doesn't poll the order state after delivering a webhook — the
204 is all it needs. If your protocol requires immediate consistency after a
POST, call `runner.process()` inline instead and return the updated state.

### Thin webhook handler

The `POST /webhooks/stripe` endpoint is a JSON→Event mapping. It extracts the
Stripe event type, maps it to a harel Event kind, pulls the `harel_order_id`
from the PaymentIntent metadata, and calls `runner.send()`.

All "what happens next" logic lives in `payment.stm` — no `if status == X`
scattered across handler code.

## The statechart

```
AwaitingPayment ──PaymentSucceeded──► Fulfilling ──(auto)──► Done   ✓ success
                ──PaymentFailed─────────────────────────────► Failed  ✗ failed
                ──Timeout (15 s)─────────────────────────────► Expired ✗ abandoned
```

## Files

- **`payment.stm`** — the machine. Three terminal states, one durable timeout.
- **`actions.py`** — `start_fulfillment` / `on_done` / `on_failed` / `on_expired`.
  In a real app: call a fulfillment service, send email, update inventory.
- **`app.py`** — the FastAPI glue: create/get orders, receive Stripe webhooks,
  fire due timers in the background.
- **`simulate.py`** — sends fake Stripe payloads so you can demo all paths
  without a Stripe account.

## Extending this example

- **Multiple webhook types** — add entries to `_KIND` in `app.py` and new
  `event` declarations to `payment.stm`.
- **Retries with backoff** — add a `Retrying` composite state that uses
  `harel.lib.exponential_backoff` (see the [place_order](../place_order/) example
  and `lib.py`).
- **Fan-out** — use `invoke X for item in cart` + `join all` to process
  multiple line items in parallel with a single `join all` barrier.
- **Stripe signature validation** — add `stripe.WebhookSignature.verify_header()`
  in the webhook handler before trusting the payload.
