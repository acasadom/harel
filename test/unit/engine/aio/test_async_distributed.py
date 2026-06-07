"""Async distributed pipeline over in-memory async backends.

Drives the full AsyncDistributedRunner + AsyncWorker loop (claim→route→ack) over
`AsyncDictStore` + `AsyncInMemoryTransport`, for a flat machine and an orthogonal one
(fork → two regions → join). Mirrors the sync `test_redis_transport` pipeline tests but
fully async (single worker draining deterministically via `step()`).
"""

from harel.dsl import definition_from_dsl
from harel.engine.aio.distributed import AsyncDistributedRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.aio_transport import AsyncInMemoryTransport
from harel.engine.execution import Status
from harel.spec.states import Event


def _h(label: str) -> str:
    return f'stm_actions.rec(at: "{label}")'


FLAT = f"""
machine M {{
  initial A
  state A {{ on enter {_h("A.enter")} }}
  state B {{ on enter {_h("B.enter")} }}
  state C {{ on enter {_h("C.enter")} }}
  from A to B
  from B to C on Go
}}
"""

ORTHO = f"""
machine M {{
  initial Fork
  orthogonal Fork {{
    state A {{
      initial A1
      state A1 {{ on enter {_h("A1")} }}
      state A2 {{ on enter {_h("A2")} }}
      from A1 to A2 on Go
    }}
    state B {{
      initial B1
      state B1 {{ on enter {_h("B1")} }}
      state B2 {{ on enter {_h("B2")} }}
      from B1 to B2 on Go
    }}
  }}
  state Done {{ on enter {_h("Done")} }}
  from Fork to Done
}}
"""


async def _drain(runner: AsyncDistributedRunner) -> None:
    w = runner.worker()
    while await w.step():
        pass


async def test_async_pipeline_flat():
    defn = definition_from_dsl(FLAT, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    exe = await runner.create(defn.id)
    assert exe.active_path == "B"
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)

    final = await store.load(exe.id)
    assert final.active_path == "C"
    assert final.status is Status.DONE
    assert final.context["trace"] == ["A.enter", "B.enter", "C.enter"]


async def test_async_pipeline_orthogonal():
    defn = definition_from_dsl(ORTHO, "M")
    store = AsyncDictStore()
    runner = AsyncDistributedRunner(store, AsyncInMemoryTransport(), {defn.id: defn})

    exe = await runner.create(defn.id)
    assert exe.active_path == "Fork"
    child_ids = list(exe.children)
    await runner.send(exe.id, Event(kind="Go"))
    await _drain(runner)

    final = await store.load(exe.id)
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    regions = [await store.load(cid) for cid in child_ids]
    assert sorted(r.context["trace"] for r in regions) == [["A1", "A2"], ["B1", "B2"]]
