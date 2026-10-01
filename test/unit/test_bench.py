"""Smoke test for the benchmarks in bench/: the machine they share validates, and a tiny
run over the in-memory backends drives every execution to Done with its action run once,
in both measurement modes and with several concurrent producers. Keeps the benches from
silently rotting between measurements."""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "bench"))

from bench_async import _DSL, _make_actions, _run_once  # noqa: E402

from harel.dsl import definition_from_dsl  # noqa: E402
from harel.engine.aio_store import AsyncDictStore  # noqa: E402
from harel.engine.aio_transport import AsyncInMemoryTransport  # noqa: E402
from harel.engine.execution import Status  # noqa: E402

N = 5


@pytest.fixture
def actions(monkeypatch):
    mod = _make_actions(use_sleep=False)
    monkeypatch.setitem(sys.modules, "bench_actions", mod)
    return mod


@pytest.mark.parametrize("e2e, producers", [(False, 1), (True, 1), (True, 3)])
async def test_bench_run_drives_every_execution_to_done(actions, e2e, producers):
    defn = definition_from_dsl(_DSL, "Bench", validate=True)
    store = AsyncDictStore()

    elapsed, eps = await asyncio.wait_for(
        _run_once(defn, store, AsyncInMemoryTransport(), N, concurrency=4, e2e=e2e, producers=producers),
        timeout=10,
    )

    assert elapsed > 0 and eps > 0
    exes = [await store.load(i) for i in await store.ids_with_prefix("")]
    assert len(exes) == N
    assert all(e.status is Status.DONE and e.outcome == "success" for e in exes)
    assert actions.calls == N  # the action runs once per execution, on its Start
