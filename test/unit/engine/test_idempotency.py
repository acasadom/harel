"""Idempotency key exposure + the opt-in `idempotent` helper (the `B` approach).

The driver sets a stable `stm.idempotency_key = {execution_id}:{version}:{index}`
before each action. It is deterministic (pure engine + pre-commit version), so an
at-least-once redelivery of the same event reproduces the same key per action — the
hook a side effect uses to dedupe against an external backend.
"""

from harel import (
    DictIdempotency,
    DurableRunner,
    Event,
    definition_from_dsl,
    idempotent,
)
from harel.engine.store import DictStore

SRC = """
machine M {
   initial A
   state A { on enter capture }
   state B { on enter capture }
   from A to B on Go
}
"""


def capture(stm, event, **inputs):
    """Record the idempotency key the driver assigned to this action call."""
    stm.execution_ctx.setdefault("keys", []).append(stm.idempotency_key)


def _runner():
    defn = definition_from_dsl(SRC, "M", actions={"capture": capture})
    return DurableRunner(DictStore(), {defn.id: defn}), defn


# --- key exposure / format -------------------------------------------------------------------


def test_key_exposed_and_formatted():
    runner, defn = _runner()
    exe = runner.create(defn.id)  # enters A at version 0
    assert exe.context["keys"] == [f"{exe.id}:0:0"]
    exe = runner.process(exe.id, Event(kind="Go"))  # enters B; loaded at version 1
    assert exe.context["keys"] == [f"{exe.id}:0:0", f"{exe.id}:1:0"]


def test_index_is_deterministic_across_executions():
    # two independent runs differ only in the (random) execution id; the
    # `:version:index` part is identical — i.e. replay-stable per action
    a, defn = _runner()
    exe_a = a.create(defn.id)
    b, _ = _runner()
    exe_b = b.create(defn.id)
    suffix = lambda e: [k.split(":", 1)[1] for k in e.context["keys"]]  # noqa: E731
    assert suffix(exe_a) == suffix(exe_b) == ["0:0"]


# --- DictIdempotency / the idempotent() helper -----------------------------------------------


class _Stm:
    def __init__(self, key):
        self.idempotency_key = key
        self.execution_ctx = {}


def test_dict_idempotency_runs_once_and_caches():
    backend = DictIdempotency()
    calls = []
    assert backend.run_once("k", lambda: (calls.append(1), "first")[1]) == "first"
    assert backend.run_once("k", lambda: (calls.append(1), "second")[1]) == "first"  # cached
    assert len(calls) == 1
    assert backend.run_once("other", lambda: "x") == "x"  # different key runs


def test_idempotent_dedupes_per_key():
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def charge(stm, event, **inputs):
        runs.append(stm.idempotency_key)
        return f"charged:{stm.idempotency_key}"

    ev = Event(kind="E")
    # same key (an at-least-once redelivery) -> body runs once, cached result returned
    assert charge(_Stm("e:1:0"), ev) == "charged:e:1:0"
    assert charge(_Stm("e:1:0"), ev) == "charged:e:1:0"
    assert runs == ["e:1:0"]
    # a different key (a different action/event) runs again
    assert charge(_Stm("e:2:0"), ev) == "charged:e:2:0"
    assert runs == ["e:1:0", "e:2:0"]


def test_idempotent_without_key_always_runs():
    # a non-durable run (no idempotency_key) just runs the body every time
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def act(stm, event, **inputs):
        runs.append(1)
        return "ok"

    nokey = _Stm(None)
    assert act(nokey, Event(kind="E")) == "ok"
    assert act(nokey, Event(kind="E")) == "ok"
    assert len(runs) == 2


def test_idempotent_action_in_a_real_run():
    # bind an idempotent-wrapped action and drive it through DurableRunner: the
    # body runs once per (state-entry) key, deduped through the backend
    backend = DictIdempotency()
    runs = []

    @idempotent(backend)
    def capture_once(stm, event, **inputs):
        runs.append(stm.idempotency_key)

    defn = definition_from_dsl(SRC, "M", actions={"capture": capture_once})
    runner = DurableRunner(DictStore(), {defn.id: defn})
    exe = runner.create(defn.id)
    runner.process(exe.id, Event(kind="Go"))
    assert runs == [f"{exe.id}:0:0", f"{exe.id}:1:0"]  # one run per distinct key
