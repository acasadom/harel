# webhook_payment — a payment driven by webhooks

## What it does

A payment lifecycle driven by Stripe-style webhook events. `POST /orders` creates one payment
and returns its id; the payment provider later calls `POST /webhooks/stripe` with
`payment_intent.succeeded` or `payment_intent.payment_failed`, and the payment moves on. If no
webhook arrives within 15 seconds, the payment is abandoned. `GET /orders/{id}` shows where it
is. `simulate.py` plays the provider, so no account is needed:

```text
python -m examples.webhook_payment.simulate           # happy path
python -m examples.webhook_payment.simulate fail      # payment declined
python -m examples.webhook_payment.simulate timeout   # no webhook: abandoned after ~15 s
python -m examples.webhook_payment.simulate dedup     # the same webhook delivered twice
```

## The machine

<!-- machine: examples/webhook_payment/payment.stm payment -->
```{mermaid}
stateDiagram-v2
[*] --> Initializing
Initializing : on enter#58; setup_order
AwaitingPayment : timeout#58; 15
Fulfilling : on enter#58; start_fulfillment
Done : on enter#58; on_done
Done : outcome#58; success
Failed : on enter#58; on_failed
Failed : outcome#58; failed
Abandoned : on enter#58; on_abandoned
Abandoned : outcome#58; abandoned
SvcError : on enter#58; on_svc_error
SvcError : outcome#58; failed
Initializing --> AwaitingPayment
AwaitingPayment --> Fulfilling : PaymentSucceeded
AwaitingPayment --> Failed : PaymentFailed
AwaitingPayment --> Abandoned : Timeout
Fulfilling --> Done
Fulfilling --> SvcError : error
Done --> [*]
Failed --> [*]
Abandoned --> [*]
SvcError --> [*]
```

What it shows:

- **A durable timeout** (`timeout 15` on `AwaitingPayment`): the timer is committed with the
  state, so the payment is abandoned on time even across restarts — no cron job.
- **Deduplication for free**: the webhook handler sends `Event(id=<the provider's event id>)`,
  and the engine records the ids it has processed, so a redelivered webhook is a no-op.
- **`on error`**: if `start_fulfillment` raises (the fulfilment service down), the payment goes
  to `SvcError` instead of dead-lettering. A declined payment is a modelled event, not an
  error.

## How it is built

Two processes share the store and the queue, two SQLite files (`payments.db`,
`payments-queue.db`):

- **`app.py` — the API.** FastAPI, with plain `def` handlers. `create_order` calls
  `runner.create(...)`, which commits the execution and queues its `Start`; it never runs the
  machine's actions. `stripe_webhook` maps the payload to an `Event` and calls `runner.send(...)`,
  which only publishes it, and answers `204`. `get_order` reads the store.
- **`worker.py` — the worker.** `runner.worker().run(stop)`: claims the next message, runs the
  machine for it, commits, acknowledges — and, when idle, fires the due timers.
- **`simulate.py`** — sends fake provider payloads over HTTP.

A webhook, end to end:

```{mermaid}
sequenceDiagram
  actor P as Provider
  participant A as API (FastAPI handler)
  participant T as queue (SqliteTransport)
  participant W as worker
  participant S as store (SqliteStore)
  P->>A: POST /webhooks/stripe (succeeded)
  A->>T: publish(order id, PaymentSucceeded, id = provider's event id)
  A-->>P: 204 — nothing has run yet
  W->>T: claim
  T-->>W: lease(PaymentSucceeded)
  W->>S: load, dedupe on the event id
  W->>W: AwaitingPayment to Fulfilling to Done (start_fulfillment, on_done)
  W->>S: commit
  W->>T: ack
```

And the timeout, when no webhook comes:

```{mermaid}
sequenceDiagram
  participant W as worker
  participant S as store
  participant T as queue
  Note over S: entering AwaitingPayment committed a timer, due in 15 s
  W->>S: due_timers(now)
  W->>T: publish(Timeout)
  W->>T: claim
  T-->>W: lease(Timeout)
  W->>S: AwaitingPayment to Abandoned, commit
```

## How it runs

- **Semantics: asynchronous** — `DistributedRunner` and a worker. The provider wants a fast
  `2xx`, and a slow action (a fulfilment call) must not hold the webhook open: the handler hands
  the event to the queue and answers, and the worker advances the machine. The price is
  eventual consistency: `GET /orders/{id}` may lag a webhook by a few milliseconds. A protocol
  that needs the new state in the response would use a `DurableRunner`'s `process` instead.
- **Execution model: background**, in both processes. FastAPI runs `def` handlers in a thread
  pool, outside its event loop, which is where a sync runner belongs. (`async def` handlers
  would call `AsyncDistributedRunner` instead: a sync runner refuses a call from inside a
  running loop.)
- **One worker.** SQLite has one writer per file, so more workers add contention, not
  throughput. The same code scales out unchanged on a server backend (Postgres, Redis, …).

## Run it

```text
pip install -r examples/webhook_payment/requirements.txt
uvicorn examples.webhook_payment.app:app          # terminal 1 — the API
python -m examples.webhook_payment.worker         # terminal 2 — the worker
python -m examples.webhook_payment.simulate       # terminal 3
```
