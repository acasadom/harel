"""Verify that fan-out spawns start their on-enter actions concurrently.

With the old sequential for-loop, N async actions took sum(durations).
With asyncio.gather they take max(durations). We use a small artificial
delay (0.05 s each, 3 regions) and assert total wall time is well under
the sequential sum (0.15 s), proving the actions overlapped.
"""

import asyncio
import time

import pytest

from harel.dsl import definition_from_dsl
from harel.engine.aio.durable import AsyncDurableRunner
from harel.engine.aio_store import AsyncDictStore
from harel.engine.resolve import DictResolver

FANOUT_DSL = """
machine fanout {
  initial Running

  state Running {
    invoke acme.child for item in items
    with { item: item }
  }

  final Done   success
  final Failed failed
  from Running join all to Done else to Failed
}
"""

CHILD_DSL = """
machine child {
  carry result

  initial Working
  state Working {
    on enter slow_action
  }
  final Done success
  from Working to Done
}
"""

DELAY = 0.05  # seconds per region


async def slow_action(stm, event, **_):
    await asyncio.sleep(DELAY)
    stm.result = "ok"


@pytest.mark.asyncio
async def test_spawns_run_concurrently():
    n = 3
    child_defn = definition_from_dsl(CHILD_DSL, "child", actions={"slow_action": slow_action})
    fanout_defn = definition_from_dsl(FANOUT_DSL, "fanout")

    store = AsyncDictStore()
    runner = AsyncDurableRunner(
        store,
        {fanout_defn.id: fanout_defn, child_defn.id: child_defn},
        resolver=DictResolver({"acme.child": child_defn}),
    )

    t0 = time.monotonic()
    exe = await runner.create(fanout_defn.id, {"items": list(range(n))})
    elapsed = time.monotonic() - t0

    # Sequential: n * DELAY = 0.15 s. Concurrent: ~DELAY = 0.05 s.
    # 60% threshold gives generous CI headroom while still catching regression.
    assert elapsed < DELAY * n * 0.6, (
        f"Expected concurrent execution (~{DELAY:.2f}s) but took {elapsed:.3f}s "
        f"(sequential would be ~{DELAY * n:.2f}s)"
    )
    assert exe.outcome == "success"
