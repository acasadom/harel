"""The execution context in the model: `context.x` in guards, a typed `context { ... }`
schema, `set` assignments on transitions and the guarded `choose`."""

from pathlib import Path

import pytest

from harel import engine
from harel.definition.events import ContextError
from harel.definition.validate import validate
from harel.dsl import DslError, definition_from_dsl
from harel.engine.distributed import DistributedRunner
from harel.engine.durable import DurableRunner
from harel.engine.execution import Status
from harel.engine.store import DictStore
from harel.engine.transport import InMemoryTransport
from harel.spec.states import Event

JOB = (Path(__file__).parents[2] / "data" / "context.stm").read_text()


def _runner(source: str, name: str = "M", *, validate_it: bool = True, trace: bool = False):
    defn = definition_from_dsl(source, name, validate=validate_it)
    store = DictStore()
    return DurableRunner(store, {defn.id: defn}, trace=trace), store, defn


def _codes(source: str, name: str = "M") -> set:
    return {i.code for i in validate(definition_from_dsl(source, name))}


# --- guards over the context ---------------------------------------------------------------
GUARDS = """
event Go { kind: string }
guard many = context.n >= 3
machine M {
  initial A
  state A {}
  final Many success {}
  final Few success {}
  final Typed success {}
  from A to Many on Go where many
  from A to Typed on Go where context.n == 1 and event.kind == "x"
  from A to Few on Go where context.n < 3
}
"""


@pytest.mark.parametrize(
    "context,kind,expected",
    [
        ({"n": 5}, "y", "Many"),  # a named guard over the context
        ({"n": 1}, "x", "Typed"),  # context and event, `event.kind` = the bare `kind`
        ({"n": 1}, "y", "Few"),
        ({}, "y", "A"),  # context.n absent: no guard over it holds
        ({"n": "lots"}, "y", "A"),  # not comparable to a number: no guard holds either
    ],
)
def test_guards_read_the_execution_context(context, kind, expected):
    runner, _, defn = _runner(GUARDS)
    exe = runner.create(defn.id, context=context)
    assert runner.process(exe.id, Event(kind="Go", data={"kind": kind})).active_path == expected


def test_a_reference_names_a_known_namespace_and_one_field():
    with pytest.raises(DslError, match="unknown namespace 'result'"):
        definition_from_dsl(
            "machine M {\n initial A\n state A {}\n from A to A on E where result.x == 1\n}", "M"
        )
    with pytest.raises(DslError, match="not a nested path"):
        definition_from_dsl(
            "machine M {\n initial A\n state A {}\n from A to A on E where context.a.b == 1\n}", "M"
        )


# --- a guard comparing two references -------------------------------------------------------
REF_GUARDS = """
event Fail { code: int  limit: int }
guard exhausted = context.retries >= context.max_retries
machine M {
  context {
    retries: int
    max_retries: int?
    floor: float?
  }
  initial A
  state A {}
  final GaveUp failed {}
  final OverLimit failed {}
  final Floored failed {}
  from A to GaveUp on Fail where exhausted
  from A to OverLimit on Fail where code > event.limit
  from A to Floored on Fail where context.floor >= event.code
  from A to A on Fail set context.retries = context.retries + 1
}
"""


@pytest.mark.parametrize(
    "context,data,expected",
    [
        ({"retries": 2, "max_retries": 2}, {"code": 1, "limit": 5}, "GaveUp"),  # context vs context
        ({"retries": 0, "max_retries": 2}, {"code": 7, "limit": 5}, "OverLimit"),  # event vs event
        ({"retries": 0, "floor": 9.5}, {"code": 7, "limit": 9}, "Floored"),  # context vs event
        ({"retries": 0, "floor": 1.0}, {"code": 7, "limit": 9}, "A"),
        ({"retries": 5}, {"code": 1, "limit": 5}, "A"),  # the right side absent: doesn't hold
    ],
)
def test_a_guard_compares_with_a_reference(context, data, expected):
    runner, _, defn = _runner(REF_GUARDS)
    exe = runner.create(defn.id, context=context)
    assert runner.process(exe.id, Event(kind="Fail", data=data)).active_path == expected


def test_a_reference_that_cant_be_compared_doesnt_hold():
    runner, store, defn = _runner(REF_GUARDS)
    exe = runner.create(defn.id, context={"retries": 3})
    store.load(exe.id).context["max_retries"] = "three"  # as an action could leave it
    assert runner.process(exe.id, Event(kind="Fail", data={"code": 1, "limit": 5})).active_path == "A"


def test_retries_run_out_against_the_context():
    runner, _, defn = _runner(REF_GUARDS)
    exe = runner.create(defn.id, context={"retries": 0, "max_retries": 2})
    paths = [
        runner.process(exe.id, Event(kind="Fail", data={"code": 1, "limit": 5})).active_path for _ in range(3)
    ]
    assert paths == ["A", "A", "GaveUp"]


def test_the_right_side_reference_is_checked_like_the_left():
    assert _codes(REF_GUARDS) == set()
    assert "unknown_context_field" in _codes(
        REF_GUARDS.replace(">= context.max_retries", ">= context.max_retry")
    )
    assert "unknown_event_field" in _codes(REF_GUARDS.replace("event.limit", "event.ceiling"))
    automatic = """
machine M {
  context { n: int }
  initial A
  state A {}
  final B success {}
  from A choose {
    when context.n > event.limit to B
    else to A
  }
}
"""
    assert "event_ref_without_event" in _codes(automatic)


def test_comparing_references_of_different_types_is_a_warning():
    mixed = REF_GUARDS.replace(
        "event Fail { code: int  limit: int }", "event Fail { code: string  limit: int }"
    )
    issues = [i for i in validate(definition_from_dsl(mixed, "M")) if i.code == "compare_type_mismatch"]
    assert issues and all(i.severity == "warning" for i in issues)
    assert any("context.floor (float) with event.code (string)" in i.message for i in issues)
    assert "compare_type_mismatch" not in _codes(REF_GUARDS)  # float vs int, int vs int: fine


def test_a_bare_name_on_the_right_is_not_a_reference():
    with pytest.raises(DslError):
        definition_from_dsl("machine M {\n initial A\n state A {}\n from A to A on E where x == y\n}", "M")


# --- regions see the parent's context -------------------------------------------------------
REGIONS = """
event Go {}
machine M {
  initial Fork
  orthogonal Fork {
    with { limit: max }
    state R {
      carry verdict
      initial W
      state W {}
      final Hit success {}
      final Miss success {}
      from W to Hit on Go where context.limit > 1 set context.seen = true
      from W to Miss on Go
    }
  }
  final Done success {}
  from Fork to Done
}
"""


def test_a_region_starts_with_what_the_fork_passes_down():
    runner, store, defn = _runner(REGIONS)
    exe = runner.create(defn.id, context={"max": 5, "verdict": "parent-value"})
    (cid,) = store.load(exe.id).children
    assert store.load(cid).context == {"limit": 5}  # only the `with` projection, renamed

    runner.process(exe.id, Event(kind="Go"))

    assert store.load(cid).active_path == "Fork.R.Hit"  # the region's guard saw what it was given
    assert store.load(cid).context["seen"] is True
    assert "seen" not in store.load(exe.id).context  # a copy: the two evolve apart


def test_carry_reports_only_what_the_region_produced():
    # the parent holds a `verdict` the region never writes: `carry verdict` must not report it
    runner, store, defn = _runner(REGIONS)
    exe = runner.create(defn.id, context={"max": 5, "verdict": "parent-value"})

    done = runner.process(exe.id, Event(kind="Go"))

    assert done.status is Status.DONE
    assert done.context["region_results"] == {"Fork.R": {"outcome": "success"}}


def test_without_with_a_region_starts_empty():
    runner, store, defn = _runner(REGIONS.replace("    with { limit: max }\n", ""))
    exe = runner.create(defn.id, context={"max": 5})
    (cid,) = store.load(exe.id).children
    assert store.load(cid).context == {}


def test_with_on_a_state_that_passes_nothing_down_is_a_warning():
    source = "machine M {\n initial A\n state A { with { x: y } }\n final B success {}\n from A to B\n}"
    issues = {(i.code, i.severity) for i in validate(definition_from_dsl(source, "M"))}
    assert ("with_without_children", "warning") in issues


# --- the context schema ------------------------------------------------------------------------
def test_create_checks_the_context_against_its_schema():
    runner, _, defn = _runner(JOB, "job")
    with pytest.raises(ContextError, match="missing required field 'attempts'"):
        runner.create(defn.id)
    with pytest.raises(ContextError, match="'attempts' must be int, got str"):
        runner.create(defn.id, context={"attempts": "0"})
    with pytest.raises(ContextError, match="'attempts' must be int, got bool"):
        runner.create(defn.id, context={"attempts": True})
    exe = runner.create(
        defn.id, context={"attempts": 0, "last_code": None, "extra": [1]}
    )  # optional None, extras ok
    assert exe.status is Status.RUNNING


def test_a_deferred_start_may_supply_the_required_fields():
    defn = definition_from_dsl(JOB, "job")
    store = DictStore()
    runner = DistributedRunner(store, InMemoryTransport(), {defn.id: defn})
    exe = runner.create(defn.id, start_on_create=False)  # nothing required yet
    with pytest.raises(ContextError):
        runner.start(exe.id)  # still missing at start
    runner.start(exe.id, data={"attempts": 0})
    worker = runner.worker()
    while worker.step():
        pass
    assert store.load(exe.id).status is Status.RUNNING


def test_schema_rules_in_validate():
    assert "unknown_context_field" in _codes(
        JOB.replace("context.attempts >= 3", "context.attemps >= 3"), "job"
    )
    assert "unknown_context_field" in _codes(
        JOB.replace("context.last_code = event.code", "context.code = event.code"), "job"
    )
    assert "assign_type_mismatch" in _codes(JOB.replace("context.attempts + 1", '"many"'), "job")
    assert "unknown_event_field" in _codes(JOB.replace("event.code {", "event.reason {"), "job")
    assert _codes(JOB, "job") == set()
    assert "assign_type_mismatch" in _codes(EXPRESSION_ERRORS)  # `/` yields a float, n is an int


# --- set -----------------------------------------------------------------------------------------
ORDER = """
event Go { by: int }
machine M {
  initial A
  state A { on exit stm_actions.rec(at: "exit") }
  final B success { on enter stm_actions.rec(at: "enter") }
  from A to B on Go set context.n = context.n + event.by, context.was = context.n
}
"""


def test_set_is_evaluated_up_front_and_applied_between_exit_and_entry():
    import stm_actions

    seen = []
    original = stm_actions.rec

    def rec(stm, event, at):
        seen.append((at, stm.execution_ctx.get("n")))

    stm_actions.rec = rec
    try:
        runner, store, defn = _runner(ORDER)
        exe = runner.create(defn.id, context={"n": 1})
        runner.process(exe.id, Event(kind="Go", data={"by": 2}))
    finally:
        stm_actions.rec = original

    assert seen == [("exit", 1), ("enter", 3)]  # on exit sees the old value, on enter the new
    assert store.load(exe.id).context["was"] == 1  # every right-hand side read the old context


@pytest.mark.parametrize(
    "rhs,expected",
    [
        ("context.n * 2", 8),
        ("context.n - 1", 3),
        ("context.n / 8", 0.5),
        ('"text"', "text"),
        ("[1, 2]", [1, 2]),
    ],
)
def test_set_right_hand_sides(rhs, expected):
    source = f"event Go {{}}\nmachine M {{\n initial A\n state A {{}}\n from A to A on Go set context.out = {rhs}\n}}"
    runner, store, defn = _runner(source)
    exe = runner.create(defn.id, context={"n": 4})
    assert runner.process(exe.id, Event(kind="Go")).context["out"] == expected


EXPRESSION_ERRORS = """
event Go { by: any }
machine M {
  context { n: int  label: string? }
  initial A
  state A {}
  final Handled failed {}
  from A to A on Go set context.n = context.n / event.by
  from A to Handled on error where type == "ExpressionError"
}
"""


@pytest.mark.parametrize(
    "by,message",
    [
        (0, "division by zero"),
        ("x", "`/` needs numbers, got str 'x'"),
        (None, "`/` needs numbers, got NoneType None"),
        (2, "context.n is declared int, `set` gave float 2.0"),  # 4 / 2 is a float
    ],
)
def test_a_failing_expression_is_routed_like_an_action_error(by, message):
    # statically invalid on purpose — validate() flags the float division into an int
    # (asserted below) — so this checks the runtime refuses it too
    runner, store, defn = _runner(EXPRESSION_ERRORS, validate_it=False)
    exe = runner.create(defn.id, context={"n": 4})

    after = runner.process(exe.id, Event(kind="Go", data={"by": by}))

    assert after.active_path == "Handled"  # `on error` took it, from where it was
    assert after.context["_error"] == {"type": "ExpressionError", "message": message}
    assert after.context["n"] == 4  # nothing applied


def test_a_failing_expression_without_on_error_dead_letters():
    no_handler = EXPRESSION_ERRORS.replace(
        '  from A to Handled on error where type == "ExpressionError"\n', ""
    )
    runner, store, defn = _runner(no_handler, validate_it=False)
    exe = runner.create(defn.id, context={"n": 4})

    after = runner.process(exe.id, Event(kind="Go", data={"by": 0}))

    assert (after.status, after.active_path, after.error) == (
        Status.FAILED,
        "A",
        "ExpressionError: division by zero",
    )


def test_a_missing_reference_is_an_expression_error():
    source = (
        "event Go {}\nmachine M {\n initial A\n state A {}\n from A to A on Go set context.n = context.m\n}"
    )
    runner, _, defn = _runner(source)
    exe = runner.create(defn.id)
    assert runner.process(exe.id, Event(kind="Go")).error == "ExpressionError: context.m is not set"


def test_the_trace_records_what_set_wrote():
    runner, store, defn = _runner(JOB, "job", trace=True)
    exe = runner.create(defn.id, context={"attempts": 0})
    runner.process(exe.id, Event(kind="Fail", data={"code": 7}))

    steps = store.read_trace(exe.id)
    assert "assigned" not in steps[0]
    assert steps[1]["assigned"] == {"attempts": 1, "last_code": 7}


def test_set_must_write_the_context_and_read_explicitly():
    with pytest.raises(DslError, match="must be `context.<field>`"):
        definition_from_dsl("machine M {\n initial A\n state A {}\n from A to A on E set event.x = 1\n}", "M")
    with pytest.raises(DslError):  # a bare name is not an operand: `event.x` or `context.x`
        definition_from_dsl(
            "machine M {\n initial A\n state A {}\n from A to A on E set context.x = y\n}", "M"
        )
    with pytest.raises(DslError):  # one arithmetic operation, no more
        definition_from_dsl(
            "machine M {\n initial A\n state A {}\n from A to A on E set context.x = 1 + 2 + 3\n}", "M"
        )


def test_set_on_an_automatic_transition_cant_read_the_event():
    source = (
        "machine M {\n initial A\n state A {}\n final B success {}\n from A to B set context.x = event.y\n}"
    )
    assert "event_ref_without_event" in _codes(source)


# --- choose ---------------------------------------------------------------------------------
def test_choose_takes_the_first_branch_whose_guard_holds_else_the_default():
    runner, store, defn = _runner(JOB, "job")
    exe = runner.create(defn.id, context={"attempts": 0})
    for code in (1, 2, 3):
        after = runner.process(exe.id, Event(kind="Fail", data={"code": code}))
        assert after.active_path == "Cooling"  # guards see the context before this `set`
        runner.process(exe.id, Event(kind="Recovered"))
    after = runner.process(exe.id, Event(kind="Fail", data={"code": 4}))
    assert (after.active_path, after.status, after.context["attempts"]) == ("GaveUp", Status.DONE, 4)


CHOICE_NO_ELSE = """
event Go {}
machine M {
  initial A
  state A {}
  final Big success {}
  from A choose on Go {
    when context.n > 10 to Big
  }
}
"""


def test_choose_without_else_doesnt_fire_when_no_branch_holds():
    runner, store, defn = _runner(CHOICE_NO_ELSE)
    exe = runner.create(defn.id, context={"n": 1})
    assert runner.process(exe.id, Event(kind="Go")).active_path == "A"
    store.load(exe.id).context["n"] = 50
    assert runner.process(exe.id, Event(kind="Go")).active_path == "Big"


def test_an_automatic_choose_routes_on_entry():
    source = """
    machine M {
      initial Gate
      state Gate {}
      final Fast success {}
      final Slow success {}
      from Gate choose {
        when context.express == true to Fast
        else to Slow
      }
    }
    """
    runner, _, defn = _runner(source)
    assert runner.create(defn.id, context={"express": True}).active_path == "Fast"
    assert runner.create(defn.id, context={}).active_path == "Slow"


def test_choose_targets_are_checked_and_reachable():
    with pytest.raises(Exception, match="cannot resolve choice target 'Nowhere'"):
        definition_from_dsl(JOB.replace("to Cooling\n  }", "to Nowhere\n  }"), "job")
    assert "unreachable" not in _codes(JOB, "job")  # GaveUp is reached only through the choose


def test_a_choose_on_cancel_must_land_on_terminals():
    source = """
    machine M {
      initial A
      state A {}
      state Busy {}
      final Stopped cancelled {}
      from A choose on Cancel {
        when context.urgent == true to Stopped
        else to Busy
      }
      from Busy to Stopped on Go
    }
    """
    assert "cancel_target_not_terminal" in _codes(source)
    runner, _, defn = _runner(source, validate_it=False)
    exe = runner.create(defn.id, context={"urgent": True})
    assert engine.has_cancel_handler(defn, exe, Event(kind="Cancel")) is False  # one branch is unsafe


@pytest.mark.parametrize("word", ["set", "choose", "when"])
def test_the_new_keywords_still_work_as_state_and_event_names(word):
    source = (
        f"event {word} {{}}\nmachine M {{\n initial A\n state A {{}}\n state {word} {{}}\n"
        f" final {word}X success {{}}\n from A to {word} on {word}\n from {word} to {word}X on {word}\n}}"
    )
    runner, _, defn = _runner(source)
    exe = runner.create(defn.id)
    assert runner.process(exe.id, Event(kind=word)).active_path == word


def test_choose_inside_a_fragment_resolves_its_state_parameter():
    source = """
    event Fail {}
    event Retry {}
    fragment Retrying(give_up: state) {
      initial Working
      state Working {}
      state Cooling {}
      from Working choose on Fail set context.n = context.n + 1 {
        when context.n >= 1 to give_up
        else to Cooling
      }
      from Cooling to Working on Retry
    }
    machine M {
      initial Job
      use Retrying(give_up=Out) as Job
      final Out failed {}
    }
    """
    runner, _, defn = _runner(source)
    exe = runner.create(defn.id, context={"n": 0})
    assert runner.process(exe.id, Event(kind="Fail")).active_path == "Job.Cooling"
    runner.process(exe.id, Event(kind="Retry"))
    assert runner.process(exe.id, Event(kind="Fail")).active_path == "Out"


def test_the_type_of_an_arithmetic_result_is_checked():
    def source(assignment: str) -> str:
        return (
            "event Go {}\nmachine M {\n context { n: int  x: float  name: string }\n"
            f" initial A\n state A {{}}\n from A to A on Go set {assignment}\n}}"
        )

    assert "assign_type_mismatch" in _codes(source("context.name = context.n + 1"))  # int into a string
    assert "assign_type_mismatch" in _codes(source("context.n = context.x * 2"))  # float into an int
    assert "assign_type_mismatch" not in _codes(source("context.x = context.n * 2"))  # int into a float
    assert "assign_type_mismatch" not in _codes(source("context.n = context.n - 1"))


def test_the_context_schema_is_machine_level_only():
    with pytest.raises(
        DslError, match="`context` is only allowed at the machine level, not on state A"
    ) as err:
        definition_from_dsl("machine M {\n  initial A\n  state A { context { n: int } }\n}", "M")
    assert err.value.line == 3


# --- an automatic choose that can never be re-evaluated ------------------------------------
HANGS = """
machine M {
  context { n: int }
  initial A
  state A {}
  final Done success {}
  from A choose { when context.n >= 10 to Done }
}
"""


def test_an_automatic_choose_without_else_that_can_never_rerun_is_an_error():
    assert "choose_can_hang" in _codes(HANGS)
    assert "choose_can_hang" not in _codes(HANGS.replace("to Done }", "to Done\n else to A }"))
    # a teardown event ends the execution, it doesn't re-run the choose
    only_cancel = HANGS.replace(
        "from A choose", "final Off cancelled {}\n  from A to Off on Cancel\n  from A choose"
    )
    assert "choose_can_hang" in _codes(only_cancel)


def test_waiting_for_a_condition_is_a_valid_automatic_choose():
    # every event handled in W re-drains it, so the choose is re-evaluated
    source = """
    event Update {}
    machine M {
      context { ready: bool }
      initial W
      state W {}
      final Go success {}
      from W choose { when context.ready == true to Go }
      from W to W on Update set context.ready = true
    }
    """
    assert "choose_can_hang" not in _codes(source)
    runner, _, defn = _runner(source)
    exe = runner.create(defn.id, context={"ready": False})
    assert exe.active_path == "W"
    assert runner.process(exe.id, Event(kind="Update")).active_path == "Go"


def test_with_reads_declared_context_fields():
    # the schema covers every execution of the machine, its regions' keys included
    schema = "context { max: int  verdict: string?  limit: int?  seen: bool? }"
    source = REGIONS.replace("machine M {", "machine M {\n  " + schema)
    assert "unknown_context_field" not in _codes(source)
    assert "unknown_context_field" in _codes(source.replace("with { limit: max }", "with { limit: maximum }"))
