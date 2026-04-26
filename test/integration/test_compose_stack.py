"""Integration against the running docker-compose stack (real Redis + workers).

Precondition: the stack is already up (see deploy/README.md). The test does NOT
launch workers — it is a client: it connects to the same Redis (the transport)
and the same sqlite file (the shared store on a named volume), creates
Executions, publishes events, and waits for the worker containers to drive them
to completion. Skips unless the stack is configured + reachable.

It is meant to run as the compose `test` service (so it shares the workers' VM
kernel + volume — sqlite WAL needs that, which a macOS bind mount can't give):

    docker compose -f deploy/docker-compose.yml up -d --build --scale worker=3 redis worker
    docker compose -f deploy/docker-compose.yml run --rm test
"""

import os
import time
from pathlib import Path

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.execution import Status
from harel.spec.states import Event
from harel.worker import build_store, build_transport  # SAME store/transport the workers use

pytestmark = pytest.mark.stack

_DEFS = Path(__file__).resolve().parents[2] / "deploy" / "definitions"


@pytest.fixture
def stack():
    """Skip unless we're running inside the compose stack (STM_STACK=1, set by the
    `test` service). Backend-agnostic — the store/transport are built from the same
    env the workers use, so this covers any store×transport combination."""
    if os.environ.get("STM_STACK") != "1":
        pytest.skip("not running in the docker-compose stack (see deploy/README.md)")


def _runner(machine):
    defn = definition_from_dsl((_DEFS / f"{machine}.stm").read_text(), machine)
    store = build_store()  # matches the workers' STM_STORE_BACKEND
    transport = build_transport()  # matches the workers' STM_TRANSPORT_BACKEND
    return DistributedRunner(store, transport, {defn.id: defn}), store, defn


def _await(predicate, timeout=60.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_flat_machines_driven_by_the_worker_stack(stack):
    runner, store, defn = _runner("flat")
    ids = [runner.create(defn.id).id for _ in range(8)]
    for eid in ids:
        runner.send(eid, Event(kind="Go"))

    done = _await(lambda: all((e := store.load(i)) is not None and e.status is Status.DONE for i in ids))
    assert done, "the worker stack did not finish the flat machines in time"
    finals = [store.load(i) for i in ids]
    assert all(e.active_path == "C" for e in finals)
    assert all(e.context["trace"] == ["A.enter", "B.enter", "C.enter"] for e in finals)


def test_orthogonal_fan_out_and_join_on_the_worker_stack(stack):
    runner, store, defn = _runner("ortho")
    parents = [runner.create(defn.id) for _ in range(4)]
    for p in parents:
        assert p.active_path == "Fork" and len(p.children) == 2
        runner.send(p.id, Event(kind="Go"))

    done = _await(
        lambda: all((e := store.load(p.id)) is not None and e.status is Status.DONE for p in parents)
    )
    assert done, "the worker stack did not join the orthogonal machines in time"
    for p in parents:
        final = store.load(p.id)
        assert final.active_path == "Done"
        assert final.context["trace"] == ["Done"]
        regions = [store.load(cid) for cid in p.children]
        assert all(r is not None and r.status is Status.DONE for r in regions)
        assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]


def test_cooperative_cancel_on_the_worker_stack(stack):
    # NB: no backlog event is sent here — with live workers a Working-advancing event
    # would race the cancel (a worker could process it before CANCELLING lands). The
    # "discards the backlog" property is proven deterministically in
    # test_distributed_workers (workers start after the cancel). Here we just prove the
    # cooperative cancel runs the modelled cleanup end-to-end on the real stack.
    runner, store, defn = _runner("critical")
    ids = [runner.create(defn.id).id for _ in range(4)]
    for eid in ids:
        runner.cancel(eid)  # cooperative: CANCELLING + injected Cancel -> runs the cleanup

    at_releasing = _await(
        lambda: all((e := store.load(i)) is not None and e.active_path == "Releasing" for i in ids)
    )
    assert at_releasing, "the worker stack did not reach the Cancel cleanup state"

    for eid in ids:
        runner.send(eid, Event(kind="Refunded"))  # complete the cleanup
    done = _await(lambda: all((e := store.load(i)) is not None and e.status is Status.DONE for i in ids))
    assert done, "the worker stack did not finish the cooperative cancel in time"
    for eid in ids:
        final = store.load(eid)
        assert final.active_path == "Cancelled"
        assert final.context["trace"] == ["working", "releasing", "cancelled"]


def test_timeout_fires_on_the_worker_stack(stack):
    runner, store, defn = _runner("retry")
    eid = runner.create(defn.id).id  # parked at Trying with a 1s timer; no event sent

    # the only way out is the worker timer sweep delivering a Timeout
    done = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.DONE)
    assert done, "the timeout did not fire via the worker stack"
    final = store.load(eid)
    assert final.active_path == "Failed"
    assert final.context["trace"] == ["trying", "failed"]


def test_retry_budget_exhaustion_on_the_worker_stack(stack):
    runner, store, defn = _runner("retry_budget")
    eid = runner.create(defn.id).id  # retries with backoff; never succeeds

    # the composite's 3s budget timeout (not the inner Waiting timeout) ends it
    done = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.DONE, timeout=30.0)
    assert done, "the retry budget did not exhaust on the worker stack"
    final = store.load(eid)
    assert final.active_path == "Failed"
    assert final.outcome == "failed"
    assert "attempt" in final.context["trace"]  # it really retried before failing


def test_unhandled_action_error_fails_not_crashes_on_the_worker_stack(stack):
    runner, store, defn = _runner("boom")
    eid = runner.create(defn.id).id  # at A
    runner.send(eid, Event(kind="Go"))  # -> Boom.on_enter raises

    failed = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.FAILED)
    assert failed, "the worker did not fail the execution on an unhandled error"
    assert store.load(eid).error == "RuntimeError: kaboom"

    # the worker stack is still alive: a fresh machine still gets driven to completion
    other, ostore, odefn = _runner("flat")
    oid = other.create(odefn.id).id
    other.send(oid, Event(kind="Go"))
    assert _await(lambda: (e := ostore.load(oid)) is not None and e.status is Status.DONE), (
        "the worker stack stopped processing after an unhandled error"
    )


def test_suspend_then_resume_on_the_worker_stack(stack):
    runner, store, defn = _runner("flat")
    eid = runner.create(defn.id).id  # parked at B
    runner.suspend(eid)
    runner.send(eid, Event(kind="Go"))  # parked while suspended, not processed

    # it must NOT advance while suspended (give the workers a window to prove it)
    time.sleep(2.0)
    assert store.load(eid).status is Status.SUSPENDED
    assert store.load(eid).active_path == "B"

    runner.resume(eid)  # the parked Go becomes claimable within suspend_recheck
    done = _await(lambda: (e := store.load(eid)) is not None and e.status is Status.DONE)
    assert done, "the suspended machine did not resume and finish on the worker stack"
    final = store.load(eid)
    assert final.active_path == "C"
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]
