"""End-to-end `http_action` over a REAL HTTP server (no Docker, deterministic).

The other faas tests use a fake in-process invoke; this one stands up an actual
`http.server` in a thread that mounts `handler(fn)`, then drives it through
`http_action` over a real socket — proving JSON crosses the wire, a separate server
runs the same action code, and the returned context is applied. This is the contract
every HTTP-triggered FaaS shares (OpenFaaS / Spin / Cloudflare Workers / Knative), so
it covers them all without a per-provider stack. Skips if `requests` is absent.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

pytest.importorskip("requests")  # the `faas` extra; present transitively in dev

from harel import (  # noqa: E402  (after importorskip)
    DurableRunner,
    Event,
    definition_from_dsl,
    handler,
    http_action,
)
from harel.engine.store import DictStore


# the action deployed "remotely" — the same (stm, event, **inputs) shape as a local one
def charge(stm, event, **inputs):
    stm.execution_ctx["charged"] = inputs.get("amount", 0)
    stm.execution_ctx["key"] = stm.idempotency_key
    # route on whether the (fake) charge cleared a threshold
    return "ok" if inputs.get("amount", 0) > 0 else "skip"


@pytest.fixture
def server_url():
    """A real HTTP server in a thread whose POST handler is `handler(charge)`."""
    entrypoint = handler(charge)

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802 (http.server casing)
            body = self.rfile.read(int(self.headers["Content-Length"]))
            out = entrypoint(json.loads(body))
            payload = json.dumps(out).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):  # silence the per-request stderr line
            pass

    httpd = HTTPServer(("127.0.0.1", 0), _Handler)  # port 0 -> an ephemeral free port
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address
    try:
        yield f"http://{host}:{port}/"
    finally:
        httpd.shutdown()
        thread.join()


def test_http_action_against_a_real_server(server_url):
    action = http_action(server_url)

    class _Stm:
        execution_ctx = {"n": 1}
        idempotency_key = "exe:5:0"

    stm = _Stm()
    result = action(stm, Event(kind="Pay"), amount=42)

    assert result == "ok"  # the server routed on the inputs it received over the wire
    assert stm.execution_ctx == {"n": 1, "charged": 42, "key": "exe:5:0"}


def test_remote_http_action_drives_a_machine(server_url):
    # bind the real-HTTP charge into a machine and run it through the DurableRunner:
    # the engine drives the remote action exactly like a local one.
    src = """
    machine M {
       initial Idle
       state Idle {}
       state Charging { on enter charge }
       state Done { outcome success }
       from Idle to Charging on Go
       from Charging to Done on Done
    }
    """
    defn = definition_from_dsl(src, "M", actions={"charge": http_action(server_url)})
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)
    # the charge action takes no `amount` input here -> the server returns "skip",
    # but the context it mutated still flows back through the real HTTP round-trip.
    exe = runner.process(exe.id, Event(kind="Go"))

    assert exe.context["charged"] == 0
    assert exe.context["key"] == f"{exe.id}:1:0"  # the engine's stable idempotency key
