"""harel.faas — remote actions (Lambda / OpenFaaS).

The wire contract is exercised with a **fake in-process invoke** (no Docker): the
function side is the real `handler(fn)` wrapper, reached through fake boto3/requests
clients. This proves serialization, context apply, routing, and that the
`idempotency_key` rides the payload. RIE/LocalStack are the opt-in `stack` tier.
"""

import json

import pytest

from harel import (
    DurableRunner,
    Event,
    definition_from_dsl,
    handler,
    http_action,
    lambda_action,
    openfaas_action,
    remote_action,
)
from harel.engine.store import DictStore
from harel.faas import _ServerProxy


def _roundtrip(obj):
    """JSON serialize+parse, mimicking what crosses the wire (fresh dict, no aliasing)."""
    return json.loads(json.dumps(obj))


# --- the action that runs on both sides (write it once) ----------------------------------------


def enrich(stm, event, **inputs):
    """An ordinary `(stm, event, **inputs)` action: mutates context, returns a result."""
    stm.execution_ctx["seen"] = event.kind
    stm.execution_ctx["total"] = stm.execution_ctx.get("total", 0) + inputs.get("amount", 0)
    stm.execution_ctx["key"] = stm.idempotency_key
    return "ok"


# --- fake transports: the function side is the REAL handler(fn) --------------------------------


class _FakeLambdaClient:
    """Mimics boto3's `lambda` client: `invoke()` runs the handler in-process and
    returns the same `{Payload, FunctionError?}` shape boto3 yields."""

    def __init__(self, fn, fail=False):
        self._handler = handler(fn)
        self._fail = fail
        self.calls = []

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803 (boto3 casing)
        self.calls.append((FunctionName, InvocationType, json.loads(Payload)))
        if self._fail:
            return {"FunctionError": "Unhandled", "Payload": _Body(b'{"errorMessage": "boom"}')}
        payload = json.loads(Payload)
        out = json.dumps(self._handler(payload)).encode()
        return {"Payload": _Body(out)}


class _Body:
    """boto3 returns a streaming body with `.read()`."""

    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    """Mimics a requests.Session: `post()` runs the handler in-process."""

    def __init__(self, fn, status=200):
        self._handler = handler(fn)
        self._status = status
        self.calls = []

    def post(self, url, json, timeout):  # noqa: A002 (requests' kw is `json`)
        self.calls.append((url, json, timeout))
        if self._status >= 400:
            return _FakeResponse(None, self._status)
        # serialize like real HTTP would, so the server side gets its own dict (no
        # aliasing with the client's context across the clear+update apply).
        payload = _roundtrip(json)
        return _FakeResponse(_roundtrip(self._handler(payload)), self._status)


# --- handler (server side) ---------------------------------------------------------------------


def test_handler_round_trips_context_and_result():
    fn = handler(enrich)
    out = fn(
        {
            "context": {"total": 5},
            "event": {"kind": "Charge", "data": {"x": 1}},
            "inputs": {"amount": 3},
            "idempotency_key": "exe:1:0",
        }
    )
    assert out == {"context": {"total": 8, "seen": "Charge", "key": "exe:1:0"}, "result": "ok"}


def test_handler_tolerates_missing_optional_fields():
    def mark(stm, event, **inputs):
        stm.execution_ctx["ran"] = True  # a plain hook returns nothing

    fn = handler(mark)
    # no inputs, no idempotency_key, no event.data
    out = fn({"context": {}, "event": {"kind": "E"}})
    assert out == {"context": {"ran": True}, "result": None}


def test_server_proxy_surface_matches_a_local_stm():
    stm = _ServerProxy({"a": 1}, "exe:0:0")
    assert stm.execution_ctx == {"a": 1}
    assert stm.idempotency_key == "exe:0:0"


# --- lambda_action (client side) ---------------------------------------------------------------


class _Stm:
    def __init__(self, ctx, key=None):
        self.execution_ctx = ctx
        self.idempotency_key = key


def test_lambda_action_invokes_applies_context_and_routes():
    client = _FakeLambdaClient(enrich)
    action = lambda_action("enrich-fn", client=client)
    stm = _Stm({"total": 10}, key="exe:2:1")
    result = action(stm, Event(kind="Charge"), amount=5)

    assert result == "ok"  # the routing key a selector would branch on
    assert stm.execution_ctx == {"total": 15, "seen": "Charge", "key": "exe:2:1"}
    # the payload carried the idempotency_key and the inputs
    fn_name, inv_type, payload = client.calls[0]
    assert fn_name == "enrich-fn"
    assert inv_type == "RequestResponse"
    assert payload["idempotency_key"] == "exe:2:1"
    assert payload["inputs"] == {"amount": 5}


def test_lambda_action_context_object_identity_preserved():
    # the stub mutates execution_ctx in place (clear+update), not reassigns it, so the
    # Execution's own dict keeps its identity across the remote call.
    client = _FakeLambdaClient(enrich)
    action = lambda_action("enrich-fn", client=client)
    ctx = {"total": 0}
    stm = _Stm(ctx)
    action(stm, Event(kind="E"))
    assert stm.execution_ctx is ctx


def test_lambda_action_function_error_raises():
    client = _FakeLambdaClient(enrich, fail=True)
    action = lambda_action("enrich-fn", client=client)
    with pytest.raises(RuntimeError, match="Lambda enrich-fn failed"):
        action(_Stm({}), Event(kind="E"))


# --- http_action (client side: OpenFaaS / Spin / CF Workers / Knative / …) ---------------------


def test_http_action_posts_applies_context_and_routes():
    session = _FakeSession(enrich)
    action = http_action("http://gw/function/enrich", session=session)
    stm = _Stm({"total": 1}, key="exe:3:0")
    result = action(stm, Event(kind="Charge"), amount=2)

    assert result == "ok"
    assert stm.execution_ctx == {"total": 3, "seen": "Charge", "key": "exe:3:0"}
    url, payload, _timeout = session.calls[0]
    assert url == "http://gw/function/enrich"
    assert payload["idempotency_key"] == "exe:3:0"


def test_http_action_http_error_raises():
    session = _FakeSession(enrich, status=502)
    action = http_action("http://gw/function/enrich", session=session)
    with pytest.raises(RuntimeError, match="HTTP 502"):
        action(_Stm({}), Event(kind="E"))


def test_openfaas_action_is_an_http_action_alias():
    # OpenFaaS is just an HTTP endpoint; the named alias points at the same factory,
    # so Spin/Cloudflare/Knative/etc. all use http_action with a different URL.
    assert openfaas_action is http_action


# --- remote_action (the transport-agnostic seam) ------------------------------------------------


def test_remote_action_over_an_arbitrary_invoke():
    # any synchronous invoke(payload) -> {context, result} works — no HTTP, no boto3.
    # this is what a user binds for a transport harel doesn't ship (gRPC, a queue, …).
    seen = {}

    def invoke(payload):
        seen.update(payload)
        return {"context": {**payload["context"], "did": "remote"}, "result": "done"}

    action = remote_action(invoke)
    stm = _Stm({"n": 1}, key="exe:9:2")
    result = action(stm, Event(kind="Go"), flag=True)

    assert result == "done"
    assert stm.execution_ctx == {"n": 1, "did": "remote"}
    assert seen["idempotency_key"] == "exe:9:2"
    assert seen["inputs"] == {"flag": True}
    assert seen["event"] == {"kind": "Go", "data": {}}


# --- end-to-end through the real DurableRunner -------------------------------------------------

SRC = """
machine M {
   initial Idle
   state Idle { on enter init }
   state Charging { on enter charge }
   state Done { outcome success }
   from Idle to Charging on Go
   from Charging to Done on Done
}
"""


def init(stm, event, **inputs):
    stm.execution_ctx["total"] = 0


def test_remote_action_runs_through_the_engine():
    # bind a REMOTE charge (fake lambda) exactly like a local impl: the engine drives
    # it as a normal action, the context flows back, and a later event sees the result.
    client = _FakeLambdaClient(enrich)
    defn = definition_from_dsl(
        SRC,
        "M",
        actions={"init": init, "charge": lambda_action("charge-fn", client=client)},
    )
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)
    exe = runner.process(exe.id, Event(kind="Go"))  # enters Charging -> remote charge

    assert exe.context["seen"] == "Go"
    # the remote action received the engine's stable idempotency key
    assert exe.context["key"] == f"{exe.id}:1:0"
    assert client.calls  # the remote function was actually invoked
