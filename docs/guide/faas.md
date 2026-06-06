# Remote actions (FaaS — Lambda / HTTP functions)

An action's implementation can run as a **remote function** — an AWS Lambda, or any HTTP-triggered
function (OpenFaaS, Fermyon Spin, Cloudflare Workers, Knative, GCF/Azure) — instead of an in-process
Python callable. The statechart engine orchestrates; the side-effecting work runs serverless. The
DSL does not change at all: a handler binds to a remote impl exactly like a local one. And because
the whole thing reduces to one `invoke(payload) -> {context, result}` seam, a transport harel
doesn't ship is a few lines, not a new integration.

## Why this needs no engine change

An action is already a **pure function of serializable data**: its signature is
`(stm, event, **inputs)`, the only state it touches is `stm.execution_ctx` (the Execution's
`context` dict, already JSON — it round-trips through every store backend), and its return value
is consumed only by a [selector](dsl-reference.md) to route. The binding seam already accepts
callables (`actions={"charge": fn}`). So a remote action is just a callable bound the normal way —
nothing in `core.py` changes.

`harel.faas` (the `faas` extra) ships both sides of the wire:

```bash
pip install "harel[faas]"   # boto3 (Lambda) + requests (OpenFaaS)
```

## Write the action once

The *same* `(stm, event, **inputs)` function runs in-process or as the function body. On the
function side, `handler(fn)` adapts it to a FaaS entrypoint — it reconstructs the `stm` from the
incoming JSON, runs your action, and returns `{context, result}`:

```python
from harel import handler


def charge(stm, event, **inputs):
    """An ordinary action: mutates context, returns a routing result."""
    stm.execution_ctx["charged"] = inputs["amount"]
    return "ok"


# the Lambda / OpenFaaS entrypoint — the same `charge` you'd bind locally
entrypoint = handler(charge)

# what the runtime hands it (client -> function JSON) and what it returns:
out = entrypoint(
    {
        "context": {},
        "event": {"kind": "Pay", "data": {}},
        "inputs": {"amount": 42},
        "idempotency_key": "exe:1:0",
    }
)
assert out == {"context": {"charged": 42}, "result": "ok"}
```

Deploy that `entrypoint` as the Lambda handler (or the OpenFaaS function body); bind the *same*
`charge` locally with `actions={"charge": charge}` for tests. One source of truth.

## Bind the remote impl on the client

On the orchestrating side, bind a remote stub like any implementation — in `actions={...}` or via
in-DSL `bind`. Two transports ship:

- **`lambda_action(function_name, ...)`** — AWS Lambda (boto3, `RequestResponse`).
- **`http_action(url, ...)`** — **any HTTP-triggered function**. The same transport covers
  OpenFaaS, **Fermyon Spin**, **Cloudflare Workers**, **Knative**, `faasd`, and Google Cloud
  Functions / Azure Functions with an HTTP trigger — only the URL differs, so there is no
  per-provider factory.

```python
# docs-test: skip
from harel import definition_from_dsl_file, http_action, lambda_action

defn = definition_from_dsl_file(
    "charge.stm",
    actions={
        "charge": lambda_action("charge-fn", region="us-east-1"),       # AWS Lambda
        # "charge": http_action("http://gateway:8080/function/charge"), # OpenFaaS
        # "charge": http_action("http://spin.local/charge"),            # Fermyon Spin
        # "charge": http_action("https://charge.acme.workers.dev"),     # Cloudflare Workers
    },
)
```

The stub serializes `{context, event, inputs, idempotency_key}`, invokes the function
synchronously (Lambda `RequestResponse` / an HTTP `POST`), applies the returned `context` to the
Execution, and returns `result` (the selector's routing key). The DSL stays transport-agnostic —
a `bind { charge = ... }` in the `.stm` is unaware whether the impl is local or remote.

### Any other transport — the `remote_action` seam

Both factories are just `remote_action(invoke)` over a specific synchronous `invoke(payload) ->
{context, result}`. For a transport harel doesn't ship — gRPC, NATS, a queue, a non-HTTP runtime —
write that one function against the [wire contract](#the-wire-contract) and bind the result.
Adding a provider is zero new framework code:

```python
# docs-test: skip
from harel import remote_action

def invoke(payload):                  # your transport: payload in, {context, result} out
    resp = my_grpc_stub.Run(payload)
    return {"context": resp.context, "result": resp.result}

actions = {"charge": remote_action(invoke)}
```

## The wire contract

```text
client -> function:  { "context": {...}, "event": {"kind", "data"}, "inputs": {...}, "idempotency_key": "..." }
function -> client:  { "context": {...}, "result": <any> }
```

Full context crosses each way (not a patch): there is a single writer per Execution, and the
context is small. Switch to a patch only if a context ever grows large.

## Idempotency across the at-least-once window

Delivery is **at least once** (see [Durability](durability.md)): if the worker crashes after the
remote invoke but before the commit, the event is redelivered and the action — local or remote —
**runs again**. The payload carries `idempotency_key` (the same stable
`stm.idempotency_key = {execution_id}:{version}:{index}`) so the function can dedupe its side
effect against the callee's native idempotency (Stripe's idempotency key, a DynamoDB conditional
put). harel records nothing extra — a harel-side record would roll back with the failed commit, so
it could not survive the crash window. This is the **B** approach; see [Durability](durability.md)
for the full reasoning and the `idempotent()` helper for local actions.

## Errors

A 5xx / Lambda runtime error propagates out of the stub → the driver's action-error policy fails
the Execution terminally (`status=FAILED`, the dead-letter). A *modelled* failure is different: the
function returns `{"result": "failed"}` and a selector routes on it — an expected outcome, not an
exception.

## Considerations

- **Sync / blocking.** The remote invoke blocks the worker until the result, like a slow local
  action. Keep long-running work modelled as a state with a `timeout`, not a 10-minute action;
  Lambda `RequestResponse` fits a bounded call.
- **Cold start.** A cold function adds latency to the blocking effect — use provisioned
  concurrency / keep-warm for latency-sensitive machines.

## Testing

Three tiers, mirroring the other backends:

1. **Unit, no Docker** (`test/unit/faas/`, default): a fake `invoke` runs `handler(fn)` in-process
   and returns the JSON — covers serialization, the context apply, routing, the `idempotency_key`,
   and `remote_action` over an arbitrary transport.
2. **Real HTTP server, no Docker** (`test/unit/faas/`, default): an `http.server` in a thread mounts
   `handler(fn)`, driven through `http_action` over a real socket. This is the contract every
   HTTP-triggered FaaS shares (OpenFaaS / Spin / Cloudflare / Knative), so it covers them all
   deterministically.
3. **Real Lambda on LocalStack** (`test/integration/test_faas_lambda.py`, marked `stack`): invokes a
   zip-packaged Lambda through real boto3 — the one transport a fake can't fully cover. See
   `deploy/README.md` for the build+run commands. Skips unless the stack is up.
