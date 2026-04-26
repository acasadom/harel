"""Orthogonal join carries a payload: each region reports its terminal `outcome`
plus a declared `carry`-projection of its context on `Finished` (a system event
with an opaque payload). The parent records them per region and exposes them as
`context["region_results"]` (keyed by region) for a selector/guard to route on.
The engine does NOT infer an aggregate verdict — the parent's outcome is whatever
terminal the model routes to after the join (policy stays in the model).
"""

from scenarios import _Runner

from harel.dsl import definition_from_dsl
from harel.engine.execution import Execution, Status
from harel.spec.states import Event

# Region A fails (terminal `Bad` declares outcome) and carries its `note`; region
# B completes plainly.
AGG = """
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      initial A1
      carry note
      state A1 { on enter scenarios.set_note }
      final Bad failed
      from A1 to Bad on Go
    }
    state B {
      initial B1
      state B1 {}
      state B2 {}
      from B1 to B2 on Go
    }
  }
  state Done {}
  from Fork to Done
}
"""

# Same regions, but the parent routes out of the join with a selector reading
# `region_results`, into `Cleanup` — a terminal that declares its OWN outcome, so
# the model's outcome overrides the aggregated default.
ROUTE = """
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      initial A1
      carry note
      state A1 { on enter scenarios.set_note }
      final Bad failed
      from A1 to Bad on Go
    }
    state B {
      initial B1
      state B1 {}
      state B2 {}
      from B1 to B2 on Go
    }
  }
  final Cleanup aborted
  state Done {}
  from Fork select scenarios.join_route { "failed" to Cleanup  "ok" to Done }
}
"""

# Both regions complete plainly (no outcome, no carry): the join leaves the
# parent's context untouched and its outcome None.
PLAIN = """
machine M {
  initial Fork
  orthogonal Fork {
    state A {
      initial A1
      state A1 {}
      state A2 {}
      from A1 to A2 on Go
    }
    state B {
      initial B1
      state B1 {}
      state B2 {}
      from B1 to B2 on Go
    }
  }
  state Done {}
  from Fork to Done
}
"""


def _run(dsl_src, event="Go"):
    defn = definition_from_dsl(dsl_src, "M")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind=event))
    return exe


def test_region_finished_carries_outcome_and_projected_context():
    parent = _run(AGG)
    a = next(cs for cs in parent.children.values() if cs.root_path == "Fork.A")
    b = next(cs for cs in parent.children.values() if cs.root_path == "Fork.B")
    # region A reported its terminal outcome + the carried `note`; B completed plainly
    assert a.outcome == "failed"
    assert a.result == {"note": "from-A"}
    assert b.outcome is None
    assert b.result == {}


def test_region_results_exposed_in_parent_context_keyed_by_region():
    parent = _run(AGG)
    assert parent.context["region_results"] == {
        "Fork.A": {"outcome": "failed", "note": "from-A"},
        "Fork.B": {"outcome": None},
    }


def test_engine_does_not_guess_an_aggregate_outcome():
    parent = _run(AGG)
    # the parent routed to a plain Done (no declared outcome); the engine does NOT
    # infer a verdict from the regions — aggregation is the model's job (a selector
    # to an outcome-bearing terminal, see ROUTE). region_results is still exposed.
    assert parent.active_path == "Done"
    assert parent.status is Status.DONE
    assert parent.outcome is None


def test_selector_routes_on_region_results():
    parent = _run(ROUTE)
    assert parent.active_path == "Cleanup"  # join_route saw a failed region


def test_model_terminal_outcome_overrides_the_default_aggregation():
    parent = _run(ROUTE)
    # Cleanup declares outcome: aborted -> the model's result wins over the
    # aggregated region default ("failed")
    assert parent.outcome == "aborted"


def test_plain_join_leaves_context_and_outcome_untouched():
    parent = _run(PLAIN)
    assert parent.active_path == "Done"
    assert parent.status is Status.DONE
    assert parent.outcome is None
    assert "region_results" not in parent.context


def test_child_state_round_trips_through_json():
    parent = _run(AGG)
    again = Execution.model_validate_json(parent.model_dump_json())
    a = next(cs for cs in again.children.values() if cs.root_path == "Fork.A")
    assert a.outcome == "failed"
    assert a.result == {"note": "from-A"}
