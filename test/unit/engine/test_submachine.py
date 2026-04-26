"""Submachine invocation (black-box composition): a state `invoke`s another
machine (a separate Definition) as a child Execution. Input is seeded with
`with { childKey: parentKey }`; on completion the submachine's `Finished`
(outcome + carried result) is delivered to the parent as a `Returned` event, which
the invoke-state routes with `on Returned where ...`. Resolution is a runner-level
seam (`MachineResolver`); here a `DictResolver` registers the submachine by FQN.

Driven by `DurableRunner` (synchronous): `create()` starts the parent, the relay
creates+starts the child, the child runs to its terminal, and its completion
flows back to the parent — all within the one call.
"""

from harel.dsl import definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.resolve import DictResolver
from harel.engine.store import DictStore
from harel.engine.transport import InMemoryTransport

# the submachine: decides success/failed from its seeded input `ok`, and carries
# `score` back to the caller on its `Finished`.
CHILD = """
machine child {
  carry score
  initial Decide
  state Decide {}
  final Won  success
  final Lost failed
  from Decide select scenarios.decide {
    "won"  to Won
    "lost" to Lost
  }
}
"""

# the parent: invokes acme.child, seeding ok/score from its own context, then
# routes on the completion's outcome + carried score.
PARENT = """
machine parent {
  initial Run
  state Run {
    invoke acme.child
    with { ok: approved  score: points }
  }
  final Done   success
  final Failed failed
  from Run to Done   on Returned where outcome == "success" and score >= 50
  from Run to Weak   on Returned where outcome == "success"
  from Run to Failed on Returned where outcome == "failed"
  final Weak passed
}
"""


def _run(parent_ctx):
    store = DictStore()
    child = definition_from_dsl(CHILD, "child")
    parent = definition_from_dsl(PARENT, "parent")
    runner = DurableRunner(store, {parent.id: parent}, resolver=DictResolver({"acme.child": child}))
    exe = runner.create(parent.id, context=parent_ctx)
    return store.load(exe.id)


def test_invoke_routes_on_success_outcome_and_carried_result():
    final = _run({"approved": True, "points": 70})
    # the submachine won (ok=True) and carried score=70 >= 50 -> Done
    assert final.active_path == "Done"
    assert final.status is Status.DONE
    assert final.outcome == "success"


def test_invoke_carried_result_drives_the_branch():
    final = _run({"approved": True, "points": 10})
    # success but score 10 < 50 -> the second branch (Weak); proves the carried
    # `score` rode the completion event and the `where` saw it
    assert final.active_path == "Weak"
    assert final.outcome == "passed"


def test_invoke_routes_on_failed_outcome():
    final = _run({"approved": False, "points": 99})
    # the submachine lost (ok=False) -> Failed, regardless of score
    assert final.active_path == "Failed"
    assert final.outcome == "failed"


# a parent that re-enters the SAME invoke-state in a loop (twice), to exercise the
# per-entry child id (a stable id would reuse the first, already-done child).
LOOPER = """
machine looper {
  initial Run
  state Run {
    invoke acme.child
    with { ok: yes }
  }
  state Bump { on enter scenarios.bump }
  final Done success
  from Run  to Bump on Returned where outcome == "success"
  from Bump select scenarios.loop_or_done {
    "loop" to Run
    "done" to Done
  }
}
"""


def test_invoke_re_entered_in_a_loop_spawns_a_fresh_child_each_time():
    store = DictStore()
    child = definition_from_dsl(CHILD, "child")
    looper = definition_from_dsl(LOOPER, "looper")
    runner = DurableRunner(store, {looper.id: looper}, resolver=DictResolver({"acme.child": child}))
    exe = runner.create(looper.id, context={"yes": True})

    final = store.load(exe.id)
    assert final.active_path == "Done"
    assert final.context["count"] == 2  # the loop ran twice
    # a distinct child Execution per entry (seq 0 and 1), both completed
    assert store.load(f"{exe.id}:Run:0").status is Status.DONE
    assert store.load(f"{exe.id}:Run:1").status is Status.DONE


# a parent that FANS OUT the child over a collection: one addressed instance per
# slice (its `ok` seeded from the slice), joined with `join all`.
FANNER = """
machine fanner {
  initial Process
  state Process {
    invoke acme.child for slice in slices
    with { ok: slice }
  }
  final Done   success
  final Failed failed
  from Process join all to Done else to Failed
}
"""


def _run_fanout(slices):
    store = DictStore()
    child = definition_from_dsl(CHILD, "child")
    fanner = definition_from_dsl(FANNER, "fanner")
    runner = DurableRunner(store, {fanner.id: fanner}, resolver=DictResolver({"acme.child": child}))
    exe = runner.create(fanner.id, context={"slices": slices})
    return store.load(exe.id)


def test_fanout_invoke_one_instance_per_slice_join_all_success():
    final = _run_fanout([True, True, True])
    # three addressed instances, all won -> join all -> Done
    assert final.active_path == "Done"
    assert final.outcome == "success"
    assert len(final.children) == 3
    assert all(cs.submachine for cs in final.children.values())


def test_fanout_invoke_join_all_fails_if_any_instance_fails():
    final = _run_fanout([True, False, True])
    # one instance lost -> join all -> Failed
    assert final.active_path == "Failed"
    assert final.outcome == "failed"


# inline submachine definitions (QML-style): the target is defined at the invoke
# site, built as its own Definition and auto-registered (no external resolver).
INLINE_SINGLE = """
machine parent {
  initial Run
  state Run {
    invoke {
      initial Decide
      state Decide {}
      final Won  success
      final Lost failed
      from Decide select scenarios.decide { "won" to Won  "lost" to Lost }
    }
    with { ok: approved }
  }
  final Done   success
  final Failed failed
  from Run to Done   on Returned where outcome == "success"
  from Run to Failed on Returned where outcome == "failed"
}
"""

INLINE_FANOUT = """
machine parent {
  initial Process
  state Process {
    invoke for slice in slices {
      initial Decide
      state Decide {}
      final Won  success
      final Lost failed
      from Decide select scenarios.decide { "won" to Won  "lost" to Lost }
    }
    with { ok: slice }
  }
  final Done   success
  final Failed failed
  from Process join all to Done else to Failed
}
"""


def _run_inline(text, ctx):
    store = DictStore()
    parent = definition_from_dsl(text, "parent")
    runner = DurableRunner(store, {parent.id: parent})  # no external resolver: inline auto-registered
    exe = runner.create(parent.id, context=ctx)
    return store.load(exe.id), parent


def test_inline_single_invoke():
    final, parent = _run_inline(INLINE_SINGLE, {"approved": True})
    assert final.active_path == "Done"
    assert final.outcome == "success"
    assert list(parent.submachines) == ["parent#Run"]  # the inline target was lifted


def test_inline_single_invoke_failure_branch():
    final, _ = _run_inline(INLINE_SINGLE, {"approved": False})
    assert final.active_path == "Failed"


def test_inline_fanout():
    final, _ = _run_inline(INLINE_FANOUT, {"slices": [True, True]})
    assert final.active_path == "Done"
    assert len(final.children) == 2
    lost, _ = _run_inline(INLINE_FANOUT, {"slices": [True, False]})
    assert lost.active_path == "Failed"


def _run_distributed(parent_ctx):
    store = DictStore()
    transport = InMemoryTransport()
    child = definition_from_dsl(CHILD, "child")
    parent = definition_from_dsl(PARENT, "parent")
    runner = DistributedRunner(
        store, transport, {parent.id: parent}, resolver=DictResolver({"acme.child": child})
    )
    exe = runner.create(parent.id, context=parent_ctx)
    worker = runner.worker()
    while worker.step():
        pass
    return store.load(exe.id)


def test_invoke_works_over_the_transport():
    # the submachine child runs as its own transport group; its Finished is routed
    # back to the parent's group and delivered as the Returned completion
    won = _run_distributed({"approved": True, "points": 70})
    assert won.active_path == "Done"
    assert won.outcome == "success"

    lost = _run_distributed({"approved": False, "points": 70})
    assert lost.active_path == "Failed"
    assert lost.outcome == "failed"
