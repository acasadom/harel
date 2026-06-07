"""DSL unit coverage: features (outcome / no-history / timeout-context /
dynamic), composition (imports + fragment `use` + params), selectors, error
handling, and validate integration."""

from pathlib import Path

import pytest

from harel.definition.validate import ValidationError, validate
from harel.dsl import DslError, definition_from_dsl, definition_from_dsl_file, parse

DATA = Path(__file__).parents[2] / "data"


# --- feature coverage ---------------------------------------------------------


def test_outcome_and_timeout_context_and_no_history():
    defn = definition_from_dsl(
        """
        machine M {
          initial A
          state A { timeout context backoff  no history }
          state B { outcome failed }
          from A to B on Done
          from A to A on Tick
        }
        """,
        "M",
    )
    assert defn.get("A").timeout == {"context": "backoff"}
    assert defn.get("A").allow_history is False
    assert defn.get("B").outcome == "failed"


def test_action_with_inputs():
    defn = definition_from_dsl(
        'machine M { initial A  state A { on enter mod.f(retries: 3, label: "x") } }',
        "M",
    )
    act = defn.get("A").on_enter
    assert act.function == "mod.f"
    assert act.inputs == {"retries": 3, "label": "x"}


def test_handler_bound_via_bind_block():
    defn = definition_from_dsl("bind { go = pkg.run }\nmachine M { initial A  state A { on enter go } }", "M")
    assert defn.get("A").on_enter.function == "pkg.run"


def test_literal_action_needs_no_binding():
    defn = definition_from_dsl("machine M { initial A  state A { on enter pkg.mod.run } }", "M")
    assert defn.get("A").on_enter.function == "pkg.mod.run"


def test_actions_param_overrides_bind_and_swaps():
    text = "bind { go = pkg.default }\nmachine M { initial A  state A { on enter go } }"
    defn = definition_from_dsl(text, "M", actions={"go": "pkg.custom"})
    assert defn.get("A").on_enter.function == "pkg.custom"


def test_actions_param_supplies_callable():
    def my_action(stm, event):
        return None

    defn = definition_from_dsl(
        "machine M { initial A  state A { on enter go } }", "M", actions={"go": my_action}
    )
    assert defn.get("A").on_enter.function is my_action


def test_unbound_handler_raises():
    with pytest.raises(DslError, match="unbound action handlers: go, stop"):
        definition_from_dsl("machine M { initial A  state A { on enter go  on exit stop } }", "M")


def test_final_sugar_parses_terminal_with_outcome():
    defn = definition_from_dsl(
        "event Ok {}  event Bad {}  machine M { initial A  state A {}  final Done success"
        "  final Boom failed { on exit p.q }"
        "  from A to Done on Ok  from A to Boom on Bad }",
        "M",
    )
    assert defn.get("Done").outcome == "success"
    assert defn.get("Boom").outcome == "failed"
    assert defn.get("Boom").on_exit.function == "p.q"  # body hooks still work
    assert {i.code for i in validate(defn) if i.severity == "error"} == set()


def _join_selector(defn):
    # a `from Fork ...` transition is owned by the enclosing scope (the root), with
    # source == Fork — find it across scopes.
    return next(
        t.selector
        for n in defn.index.values()
        for t in n.transitions
        if t.selector and t.source.full_path == "Fork"
    )


def test_join_sugar_builds_selector_and_validates_clean():
    defn = definition_from_dsl_file(DATA / "join_sugar.stm", "M")
    sel = _join_selector(defn)
    assert sel.action.function == "harel.lib.join_success"
    assert sel.action.inputs == {"mode": "all"}
    assert sel.mapper == {"pass": "Done"}
    assert sel.default == "Failed"
    assert {i.code for i in validate(defn) if i.severity == "error"} == set()


def test_join_any_sets_mode():
    defn = definition_from_dsl(
        "machine M { initial Fork  orthogonal Fork {"
        "  state A { initial A1  state A1 {}  final Aok success  from A1 to Aok on Go } }"
        "  final Done success  final Failed failed  from Fork join any to Done else to Failed }",
        "M",
    )
    assert _join_selector(defn).action.inputs == {"mode": "any"}


def test_join_sugar_routes_all_success_to_done_and_a_failure_to_else():
    from scenarios import _Runner

    from harel.engine.execution import Execution, Status
    from harel.spec.states import Event

    def run(kind):
        defn = definition_from_dsl_file(DATA / "join_sugar.stm", "M")
        exe = Execution(definition_id=defn.id)
        runner = _Runner(defn)
        runner.start(exe)
        runner.inject(exe, Event(kind=kind))
        return exe

    # both regions succeed -> join all -> Done (success)
    won = run("Pass")
    assert won.active_path == "Done"
    assert won.status is Status.DONE
    assert won.outcome == "success"

    # region A fails (B still succeeds) -> not all -> else -> Failed
    lost = run("Fail")
    assert lost.active_path == "Failed"
    assert lost.outcome == "failed"


def test_region_carry_parses():
    defn = definition_from_dsl_file(DATA / "join_outcome.stm", "Join")
    assert defn.get("Fork.A").carry == ("note",)
    assert defn.get("Fork.A.Bad").outcome == "failed"


def test_join_consumes_region_results_end_to_end():
    from scenarios import _Runner

    from harel.engine.execution import Execution, Status
    from harel.spec.states import Event

    defn = definition_from_dsl_file(DATA / "join_outcome.stm", "Join")
    exe = Execution(definition_id=defn.id)
    runner = _Runner(defn)
    runner.start(exe)
    runner.inject(exe, Event(kind="Go"))

    # the selector saw region A's failure (via region_results) and routed to
    # Cleanup, whose own outcome overrides the aggregated region default
    assert exe.active_path == "Cleanup"
    assert exe.status is Status.DONE
    assert exe.outcome == "aborted"
    assert exe.context["region_results"]["Fork.A"] == {"outcome": "failed", "note": "from-A"}


# --- composition --------------------------------------------------------------


def test_use_splices_fragment_from_import():
    defn = definition_from_dsl_file(DATA / "charge_retry.stm", "charge")
    # the imported Retry fragment was spliced as the child composite `Authorize`
    assert {
        "Charging.Authorize",
        "Charging.Authorize.Attempt",
        "Charging.Authorize.Waiting",
        "Charging.Authorize.Succeeded",
    } <= set(defn.index)
    # the consumer's event is declared and the forwarded backoff handler resolved
    assert "PaymentResult" in defn.events
    assert defn.get("Charging.Authorize.Waiting").on_enter.function == "harel.lib.exponential_backoff"


def test_aliased_import_namespaces_fragments():
    text = """
    import "retry.stm" as r
    machine M {
      initial Host
      state Host { use r.Retry(work = a.w, check = a.c, backoff = a.b, base = 1, cap = 2) as Inner }
      from Host to Host on Fail
    }
    """
    defn = definition_from_dsl(text, "M", base_path=DATA)
    assert {"Host.Inner", "Host.Inner.Attempt", "Host.Inner.Waiting", "Host.Inner.Succeeded"} <= set(
        defn.index
    )


def test_fragment_only_file_has_no_machine():
    with pytest.raises(DslError, match="no machine"):
        definition_from_dsl_file(DATA / "retry.stm")


# --- errors -------------------------------------------------------------------


def test_syntax_error_is_dsl_error():
    with pytest.raises(DslError):
        parse("machine M { initial }")  # missing target


def test_unknown_fragment():
    with pytest.raises(DslError, match="unknown fragment"):
        definition_from_dsl("machine M { initial A  state A { use Ghost } }", "M")


def test_multiple_machines_requires_name():
    text = "machine A { initial X  state X {} }\nmachine B { initial Y  state Y {} }"
    with pytest.raises(DslError, match="multiple machines"):
        definition_from_dsl(text)
    # selecting one works
    assert definition_from_dsl(text, "B").get("Y") is not None


def test_unknown_machine_name():
    with pytest.raises(DslError, match="no machine named"):
        definition_from_dsl("machine A { initial X  state X {} }", "Nope")


# --- named guards -------------------------------------------------------------


def _outgoing(defn, src_path, kind):
    # transitions live on the scope node that owns them, keyed by their source
    for node in defn.index.values():
        for t in node.transitions:
            if t.source.full_path == src_path and t.event_filter and t.event_filter.kind == kind:
                return t
    raise AssertionError(f"no {kind} transition from {src_path}")


def test_named_guard_referenced_in_where():
    defn = definition_from_dsl(
        """
        guard ok = status == "Success"
        event E { status: string }
        machine M { initial A  state A {}  state B {}  from A to B on E where ok }
        """,
        "M",
    )
    assert _outgoing(defn, "A", "E").event_filter.predicates == {"status__eq": "Success"}


def test_unbound_guard_raises():
    with pytest.raises(DslError, match="unbound guards: missing"):
        definition_from_dsl(
            "machine M { initial A  state A {}  state B {}  from A to B on E where missing }", "M"
        )


def _leaves(pred):
    if pred.node == "leaf":
        return {(pred.field, pred.op, pred.value)}
    return {leaf for c in pred.children for leaf in _leaves(c)}


def test_guard_ref_composed_with_predicate_in_where():
    defn = definition_from_dsl(
        """
        guard ok = kind == "foo"
        event E { kind: string  status: string }
        machine M { initial A  state A {}  state B {}  from A to B on E where ok and status == "x" }
        """,
        "M",
    )
    pred = _outgoing(defn, "A", "E").event_filter.predicate
    # `ok` resolved to its predicate and AND-composed with the inline `status == x`
    assert pred.node == "all"
    assert _leaves(pred) == {("kind", "eq", "foo"), ("status", "eq", "x")}


def test_guard_ref_composed_with_or_and_not():
    defn = definition_from_dsl(
        """
        guard ok = kind == "foo"
        event E { kind: string  status: string }
        machine M { initial A  state A {}  state B {}  from A to B on E where status == "x" or not ok }
        """,
        "M",
    )
    pred = _outgoing(defn, "A", "E").event_filter.predicate
    assert pred.node == "any"
    assert _leaves(pred) == {("status", "eq", "x"), ("kind", "eq", "foo")}
    assert any(c.node == "not" for c in pred.children)  # `not ok` kept as a negation


def test_guard_supplied_programmatically():
    # `g` is referenced but not declared in-DSL; guards= binds it (the seam)
    defn = definition_from_dsl(
        "event E { status: string }"
        "  machine M { initial A  state A {}  state B {}  from A to B on E where g }",
        "M",
        guards={"g": {"status__eq": "y"}},
    )
    assert _outgoing(defn, "A", "E").event_filter.predicates == {"status__eq": "y"}


def test_guards_param_overrides_in_dsl_guard():
    defn = definition_from_dsl(
        """
        guard ok = status == "a"
        event E { status: string }
        machine M { initial A  state A {}  state B {}  from A to B on E where ok }
        """,
        "M",
        guards={"ok": {"status__eq": "b"}},  # programmatic wins, like actions=
    )
    assert _outgoing(defn, "A", "E").event_filter.predicates == {"status__eq": "b"}


# --- parametrized fragments ---------------------------------------------------

PARAM_FRAGMENT = """
event Fail {}
event Abort {}
fragment Retry(work: action, give_up: state, ok: guard) {
  initial Working
  state Working { on enter work }
  state Backoff { timeout 5 }
  from Working to Backoff on Fail where ok
  from Backoff to Working on Timeout
  from Working to give_up on Abort
}
machine M {
  initial Run
  state Run {
    initial Idle
    state Idle {}
    use Retry(work = app.run, give_up = Aborted, ok = (status == "yes")) as R
    from Idle to R on Fail
  }
  state Aborted {}
}
"""


def test_fragment_params_action_state_guard():
    defn = definition_from_dsl(PARAM_FRAGMENT, "M")
    # action param bound
    assert defn.get("Run.R.Working").on_enter.function == "app.run"
    # guard param substituted into the Fail transition's predicate
    assert _outgoing(defn, "Run.R.Working", "Fail").event_filter.predicates == {"status__eq": "yes"}
    # state param: the Abort target resolved to the consumer-scope state `Aborted`
    assert _outgoing(defn, "Run.R.Working", "Abort").target.full_path == "Aborted"


def test_invoke_and_with_parse():
    defn = definition_from_dsl(
        """
        machine M {
          initial Run
          state Run {
            invoke acme.jobs.worker
            with { ok: approved  n: count }
          }
          final Done success
          from Run to Done on Returned where outcome == "success"
        }
        """,
        "M",
    )
    run = defn.get("Run")
    assert run.invoke == "acme.jobs.worker"
    assert run.invoke_with == {"ok": "approved", "n": "count"}


def test_fragment_event_param_substitutes_trigger_kind():
    defn = definition_from_dsl(
        """
        fragment Step(go: event) {
          initial W
          state W {}
          final Ok success
          from W to Ok on go
        }
        machine M {
          initial Start
          state Start {}
          use Step(go = Proceed) as S
          from Start to S
        }
        """,
        "M",
    )
    # the fragment's `on go` was rewritten to the consumer-supplied event `Proceed`
    assert _outgoing(defn, "S.W", "Proceed").target.full_path == "S.Ok"


def test_fragment_event_param_wrong_arg_kind_raises():
    text = """
        fragment Step(go: event) { initial W  state W {}  final Ok success  from W to Ok on go }
        machine M { initial Start  state Start {}  use Step(go = (status == "x")) as S  from Start to S }
        """
    with pytest.raises(DslError, match="must be an event name"):
        definition_from_dsl(text, "M")


def test_use_missing_arg_raises():
    text = PARAM_FRAGMENT.replace(
        'Retry(work = app.run, give_up = Aborted, ok = (status == "yes"))', "Retry(work = app.run)"
    )
    with pytest.raises(DslError, match="missing args"):
        definition_from_dsl(text, "M")


def test_use_unknown_arg_raises():
    text = PARAM_FRAGMENT.replace(
        'Retry(work = app.run, give_up = Aborted, ok = (status == "yes"))',
        'Retry(work = app.run, give_up = Aborted, ok = (status == "yes"), bogus = x)',
    )
    with pytest.raises(DslError, match="unknown args: bogus"):
        definition_from_dsl(text, "M")


def test_use_paramless_fragment_with_empty_parens():
    # a no-parameter fragment can be instantiated as `use Frag()` (empty arg list)
    defn = definition_from_dsl(
        "fragment Ping {\n  state P {}\n}\nmachine M {\n  initial A\n  state A {}\n  use Ping() as Q\n}\n",
        "M",
    )
    assert "Q.P" in defn.index


def test_use_empty_parens_on_fragment_with_params_reports_missing():
    # `()` on a fragment that declares params is a located "missing args", not a syntax error
    with pytest.raises(DslError, match="missing args: x"):
        definition_from_dsl(
            "fragment F(x: value) {\n  state S {}\n}\n"
            "machine M {\n  initial A\n  state A {}\n  use F() as Y\n}\n",
            "M",
        )


# --- value parameters ---------------------------------------------------------


def test_value_param_in_timeout():
    defn = definition_from_dsl(
        """
        event E {}
        fragment Timed(budget: value) {
          initial Work
          state Work { timeout budget }
          state Next {}
          from Work to Next on Timeout
        }
        machine M {
          initial Host
          state Host { use Timed(budget = 30) as T }
          from Host to Host on E
        }
        """,
        "M",
    )
    assert defn.get("Host.T.Work").timeout == 30


def test_value_param_in_action_inputs():
    defn = definition_from_dsl(
        """
        fragment F(act: action, n: value) {
          initial A
          state A { on enter act(retries: n) }
        }
        machine M {
          initial Host
          state Host { use F(act = pkg.run, n = 7) as Inner }
        }
        """,
        "M",
    )
    assert defn.get("Host.Inner.A").on_enter.inputs == {"retries": 7}


def test_value_param_outside_fragment_is_unbound():
    with pytest.raises(DslError, match="unbound value params: budget"):
        definition_from_dsl("machine M { initial A  state A { timeout budget } }", "M")


def test_charge_retry_builds_and_validates_clean():
    defn = definition_from_dsl_file(DATA / "charge_retry.stm", "charge")
    w = defn.get("Charging.Authorize.Waiting")
    assert w.on_enter.function == "harel.lib.exponential_backoff"
    assert w.on_enter.inputs == {"base": 5, "cap": 600}
    assert w.timeout == {"context": "backoff"}
    assert {i.code for i in validate(defn) if i.severity == "error"} == set()


# --- selectors (enum + else) --------------------------------------------------

SELECTOR = """
event E {{}}
machine M {{
  initial Decide
  state Decide {{}}
  state Run {{ outcome success }}  state Done {{ outcome success }}  state Err {{ outcome failed }}
  from Decide select app.classify{enum} on E {{
    "running" to Run
    "done"    to Done
    {extra}
  }}
}}
"""


def _selector(defn):
    return next(t.selector for n in defn.index.values() for t in n.transitions if t.selector)


def test_selector_else_and_enum_built():
    defn = definition_from_dsl(SELECTOR.format(enum=' returns {"running", "done"}', extra="else to Err"), "M")
    sel = _selector(defn)
    assert sel.mapper == {"running": "Run", "done": "Done"}
    assert sel.default == "Err"
    assert sel.enum == ["running", "done"]


def test_selector_phantom_branch_fails_validate():
    # declared enum omits "done", which the mapper still routes -> phantom
    defn = definition_from_dsl(SELECTOR.format(enum=' returns {"running"}', extra="else to Err"), "M")
    assert "selector_phantom_branch" in {i.code for i in validate(defn)}


def test_selector_non_exhaustive_fails_validate():
    # declares a value the mapper does not cover, and no else
    defn = definition_from_dsl(SELECTOR.format(enum=' returns {"running", "done", "failed"}', extra=""), "M")
    assert "selector_non_exhaustive" in {i.code for i in validate(defn)}


def test_selector_exhaustive_with_else_is_clean():
    defn = definition_from_dsl(
        SELECTOR.format(enum=' returns {"running", "done", "failed"}', extra="else to Err"), "M"
    )
    assert {i.code for i in validate(defn) if i.severity == "error"} == set()


# --- validate integration -----------------------------------------------------


def test_validate_flag_raises_on_defect():
    # two automatic transitions from A => nondeterministic
    text = "machine M { initial A  state A {}  state B {}  state C {}  from A to B  from A to C }"
    with pytest.raises(ValidationError):
        definition_from_dsl(text, "M", validate=True)


def test_undeclared_event_fails_validation():
    # an event used in a transition but never declared is a validation error
    text = "machine M { initial A  state A {}  final Done success  from A to Done on Go }"
    with pytest.raises(ValidationError) as ei:
        definition_from_dsl(text, "M", validate=True)
    assert any(i.code == "unknown_event" for i in ei.value.issues)
    # declaring it makes the machine valid
    ok = definition_from_dsl("event Go {}  " + text, "M", validate=True)
    assert ok.id == "M"
