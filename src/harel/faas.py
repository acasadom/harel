"""Run an action's implementation as a **remote function** (AWS Lambda / OpenFaaS).

The statechart engine orchestrates while the side-effecting work runs serverless.
A remote action needs **no engine change**: an action is already a pure function of
serializable data (`(stm, event, **inputs)` mutating the JSON `stm.execution_ctx`),
and the binding seam already accepts callables. So a remote action is a callable
bound the normal way — `actions={"charge": lambda_action("charge-fn")}` or in-DSL
`bind`. The DSL stays transport-agnostic, exactly like a handler is impl-agnostic.

Two sides, the **same** action code:

* **client** (this process): `lambda_action` (AWS Lambda, boto3) / `http_action` (any
  HTTP endpoint — OpenFaaS, Spin, Cloudflare Workers, Knative, GCF/Azure HTTP) return a
  stub that serializes the context/event/inputs, invokes the remote function
  synchronously, applies the returned context, and returns the result (a selector's
  routing key). Both are just `remote_action(invoke)` over a specific transport — bind
  your own `invoke` for anything harel doesn't ship (gRPC, a queue, …).
* **server** (the function): `handler(fn)` wraps your ordinary action so it runs
  against the JSON payload — `stm.execution_ctx` is the incoming context, the return
  is the routing result. Deploy the *same* `fn` as a Lambda or run it in-process.

Wire contract (JSON, full context — single writer per Execution, so it's safe and
small; switch to a patch only if context grows):

    client -> function: {context, event:{kind,data}, inputs, idempotency_key}
    function -> client: {context, result}

`idempotency_key` rides the payload so a side-effecting remote action can be made
**effect-once** across the at-least-once redelivery window (a crash between invoke
and commit re-runs the action) — the same `stm.idempotency_key` the local
`harel.idempotency.idempotent` helper uses. The function passes it to the callee's
native idempotency (Stripe's key, a DynamoDB conditional put); harel records nothing
(see `harel.idempotency` on why an in-harel record can't survive the crash window).

A 5xx / Lambda error propagates out of the stub → the driver's `_on_action_error`
fails the Execution terminally (the dead-letter). A *modelled* failure is the function
returning `result: "failed"`, routed by a selector — not an exception.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Optional

from harel.spec.states import Event

# the JSON the client sends and the function returns. Kept as bare dicts (not a
# pydantic model) so the function side has zero harel import surface if it wants.
Payload = dict


def _build_payload(stm: Any, event: Event, inputs: dict) -> Payload:
    """The client -> function JSON for one action invocation."""
    return {
        "context": stm.execution_ctx,
        "event": {"kind": event.kind, "data": event.data},
        "inputs": inputs,
        # stable across an at-least-once redelivery (see harel.idempotency); the
        # function passes it to the callee's native idempotency to dedupe the effect.
        "idempotency_key": getattr(stm, "idempotency_key", None),
    }


def _apply(stm: Any, out: Payload) -> Any:
    """Apply a function -> client response: replace the context in place (so the
    Execution's own dict object keeps its identity) and return the routing result."""
    stm.execution_ctx.clear()
    stm.execution_ctx.update(out.get("context", {}))
    return out.get("result")


def remote_action(invoke: Callable[[Payload], Payload]) -> Callable:
    """Build an action stub `(stm, event, **inputs)` over **any** synchronous
    `invoke(payload) -> response` transport. This is the seam every provider reduces
    to — `lambda_action`/`http_action` are just `remote_action` over a specific
    `invoke`. Bind your own to reach a transport harel doesn't ship (gRPC, NATS, a
    queue, a non-HTTP runtime): write `invoke` against the wire contract (build the
    payload, return `{context, result}`) and bind the result like any impl:

        action = remote_action(my_invoke)
        defn = definition_from_dsl(src, actions={"charge": action})
    """

    def action(stm: Any, event: Event, **inputs: Any) -> Any:
        return _apply(stm, invoke(_build_payload(stm, event, dict(inputs))))

    return action


# --- client factories ------------------------------------------------------


def lambda_action(
    function_name: str,
    client: Any = None,
    region: Optional[str] = None,
    endpoint_url: Optional[str] = None,
) -> Callable:
    """An action backed by an AWS Lambda, invoked synchronously (`RequestResponse`).

    Bind it like any impl: `actions={"charge": lambda_action("charge-fn")}`. Pass a
    pre-built boto3 `client` (or `endpoint_url` for LocalStack — dummy creds, like the
    SqsTransport). The Lambda runs your action wrapped in `handler(fn)`.
    """
    if client is None:
        import boto3

        kwargs: dict[str, Any] = {}
        if region is not None:
            kwargs["region_name"] = region
        if endpoint_url is not None:
            # LocalStack: an injected endpoint with throwaway creds (no AWS account)
            kwargs.update(
                endpoint_url=endpoint_url,
                aws_access_key_id="test",
                aws_secret_access_key="test",
                region_name=region or "us-east-1",
            )
        client = boto3.client("lambda", **kwargs)

    def invoke(payload: Payload) -> Payload:
        resp = client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )
        # a handler/runtime error surfaces as FunctionError -> raise so the driver's
        # action-error policy fails the Execution (the dead-letter), not a silent miss.
        if resp.get("FunctionError"):
            body = resp["Payload"].read().decode()
            raise RuntimeError(f"Lambda {function_name} failed: {body}")
        return json.loads(resp["Payload"].read())

    return remote_action(invoke)


def http_action(url: str, session: Any = None, timeout: float = 30.0) -> Callable:
    """An action backed by an HTTP function endpoint, invoked synchronously (POST JSON).

    This is the transport every HTTP-triggered FaaS shares — **OpenFaaS, Fermyon Spin,
    Cloudflare Workers, Knative, faasd, Google Cloud Functions / Azure Functions with an
    HTTP trigger**. Only the URL differs, so there is no per-provider factory; point it at
    the endpoint and bind it like any impl:

        actions={"charge": http_action("http://gateway:8080/function/charge")}  # OpenFaaS
        actions={"charge": http_action("http://spin.local/charge")}            # Spin

    `url` is the function endpoint. Pass a pre-built `requests.Session` to reuse a
    connection / add auth headers. The function runs your action wrapped in `handler(fn)`.
    """
    if session is None:
        import requests

        session = requests.Session()

    def invoke(payload: Payload) -> Payload:
        resp = session.post(url, json=payload, timeout=timeout)
        # a non-2xx (5xx etc.) raises -> the driver fails the Execution terminally.
        resp.raise_for_status()
        return resp.json()

    return remote_action(invoke)


# OpenFaaS is just an HTTP endpoint; kept as a named alias for discoverability.
openfaas_action = http_action


# --- server side -----------------------------------------------------------


class _ServerProxy:
    """The `stm` reconstructed on the function side from the incoming payload: the
    same surface a local action sees (`execution_ctx` + `idempotency_key`), so the
    *same* action code runs in-process or remotely."""

    def __init__(self, context: dict, idempotency_key: Optional[str]) -> None:
        self.execution_ctx = context
        self.idempotency_key = idempotency_key


def handler(fn: Callable) -> Callable[[Payload, Any], Payload]:
    """Wrap an action `(stm, event, **inputs)` as a FaaS entrypoint
    `(payload, _ctx=None) -> response`. Reconstructs the `stm` from the payload, runs
    your ordinary action, and returns `{context, result}`. The same `fn` is bound
    locally via `actions={...}` or deployed as the function body — write it once.

        # my_function.py (the Lambda / OpenFaaS handler)
        from harel.faas import handler
        from myactions import charge
        lambda_handler = handler(charge)
    """

    def entrypoint(payload: Payload, _ctx: Any = None) -> Payload:
        stm = _ServerProxy(payload.get("context", {}), payload.get("idempotency_key"))
        event = Event(kind=payload["event"]["kind"], data=payload["event"].get("data", {}))
        result = fn(stm, event, **payload.get("inputs", {}))
        return {"context": stm.execution_ctx, "result": result}

    return entrypoint
