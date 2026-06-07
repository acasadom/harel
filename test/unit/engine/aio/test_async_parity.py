"""Parity: the async core reproduces the sync engine byte-for-byte.

Drives every shared `scenarios.SCENARIOS` (incl. both orthogonal forks) through the
`AsyncDriver` + `AsyncDictStore` and asserts the result equals the sync oracle
(`scenarios.run_new`, the production `Driver`). Since the pure engine is unchanged, the
trace, context and status match exactly. `asyncio_mode = "auto"` runs the `async def` tests.
"""

import pytest
from scenarios import SCENARIOS, run_new

from harel.dsl import definition_from_dsl
from harel.engine.aio.driver import AsyncDriver
from harel.engine.aio_store import AsyncDictStore
from harel.engine.execution import Execution
from harel.spec.states import Event


async def run_async(scenario) -> dict:
    defn = definition_from_dsl(scenario["dsl"], scenario["stm"])
    exe = Execution(definition_id=defn.id, context=dict(scenario.get("context", {})))

    driver = AsyncDriver(defn, AsyncDictStore())
    await driver.start(exe)
    trace = [{"event": "Start", "end_state": exe.active_path}]
    for ev in scenario["events"]:
        event = Event(kind=ev["kind"], data=dict(ev.get("data", {})))
        await driver.inject(exe, event)
        trace.append({"event": ev["kind"], "end_state": exe.active_path})

    return {"trace": trace, "context": dict(exe.context), "status": exe.status.value}


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s["name"] for s in SCENARIOS])
async def test_async_matches_sync(scenario):
    assert await run_async(scenario) == run_new(scenario)
