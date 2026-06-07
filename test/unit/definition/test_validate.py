"""Static validation of a `Definition` (`validate` / `validate_or_raise`).

Each test builds a small machine with one defect and asserts the issue codes
the validator reports; a clean machine reports nothing. Machines are built via
`build_definition` with a falsy global_context so action names are used verbatim
(no module resolution).
"""

import pytest

from harel.definition.builder import build_definition
from harel.definition.events import EventType, FieldSpec
from harel.definition.validate import ValidationError, validate, validate_or_raise


def build(config: dict):
    return build_definition(config, {}, "M")


def codes(defn, severity=None):
    return {i.code for i in validate(defn) if severity is None or i.severity == severity}


# --- a clean machine reports nothing -----------------------------------------

CLEAN = {
    "start": "A",
    "events": {"Step": {}, "Pick": {"choice": "string"}},
    "states": {
        "A": {"on_enter": "mod.a"},
        "Outer": {
            "start": "In1",
            "states": {"In1": {}, "In2": {}},
            "transitions": [{"from": "In1", "to": "In2", "on_event": {"type": "Step"}}],
        },
        "Done": {"outcome": "success"},
    },
    "transitions": [
        {"from": "A", "to": "Outer"},
        {"from": "Outer", "to": "Done", "on_event": {"type": "Step"}},
        {
            "from": "A",
            "on_event": {"type": "Pick"},
            "selector": {"function": "mod.pick", "mapper": {"true": "Outer", "false": "Done"}},
        },
    ],
}


def test_clean_machine_has_no_issues():
    assert validate(build(CLEAN)) == []


def test_validate_or_raise_returns_issue_list_when_clean():
    assert validate_or_raise(build(CLEAN)) == []


# --- structural defects -------------------------------------------------------


def test_selector_target_unresolved():
    cfg = {
        "start": "A",
        "states": {"A": {}, "B": {}},
        "transitions": [
            {"from": "A", "selector": {"function": "mod.f", "mapper": {"ok": "B", "no": "Ghost"}}}
        ],
    }
    assert "selector_target_unresolved" in codes(build(cfg), "error")


def test_missing_initial():
    cfg = {
        "start": "Outer",
        "states": {
            "Outer": {"states": {"In1": {}, "In2": {}}, "transitions": [{"from": "In1", "to": "In2"}]}
        },
    }
    assert "missing_initial" in codes(build(cfg), "error")


def test_initial_unresolved():
    cfg = {
        "start": "Outer",
        "states": {"Outer": {"start": "Nope", "states": {"In1": {}}}},
    }
    assert "initial_unresolved" in codes(build(cfg), "error")


def test_nondeterministic_automatic():
    cfg = {
        "start": "A",
        "states": {"A": {}, "B": {}, "C": {}},
        "transitions": [{"from": "A", "to": "B"}, {"from": "A", "to": "C"}],
    }
    assert "nondeterministic_automatic" in codes(build(cfg), "error")


def test_unreachable_is_a_warning():
    cfg = {
        "start": "A",
        "states": {"A": {}, "Island": {}},
        "transitions": [{"from": "A", "to": "A", "on_event": {"type": "Ping"}}],
    }
    issues = validate(build(cfg))
    assert any(i.code == "unreachable" and i.path == "Island" and i.severity == "warning" for i in issues)


def test_state_reachable_only_via_selector_else_is_reachable():
    # a state routed solely through a selector's `else` branch must NOT be flagged
    # unreachable — this is exactly how the `join all ... else to X` sugar reaches X.
    from harel.dsl import definition_from_dsl

    src = """
    machine M {
      initial Fork
      orthogonal Fork {
        state A { initial A1  state A1 {} }
        state B { initial B1  state B1 {} }
      }
      final Done success
      final Failed failed
      from Fork join all to Done else to Failed
    }
    """
    assert "unreachable" not in codes(definition_from_dsl(src, "M"))


def test_timeout_invalid():
    cfg = {"start": "A", "states": {"A": {"timeout": -5}}}
    assert "timeout_invalid" in codes(build(cfg), "error")


def test_timeout_dynamic_ok_but_unhandled_warns():
    cfg = {"start": "A", "states": {"A": {"timeout": {"context": "backoff"}}}}
    cs = codes(build(cfg))
    assert "timeout_invalid" not in cs and "timeout_malformed" not in cs
    assert "timeout_unhandled" in cs


def test_timeout_handled_is_clean():
    cfg = {
        "start": "A",
        "states": {"A": {"timeout": 5}, "B": {}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Timeout"}}],
    }
    assert "timeout_unhandled" not in codes(build(cfg))


def test_inner_timeout_handled_by_ancestor_is_clean():
    # an inner state's timeout with no own handler is fine when an enclosing state
    # has an `on Timeout` transition — the Timeout bubbles up to it (engine parity)
    from harel.dsl import definition_from_dsl

    src = """
    machine M {
      initial C
      state C {
        initial Inner
        state Inner { timeout 5 }
        state Other {}
        from Inner to Other on Step
      }
      final Failed failed
      from C to Failed on Timeout
    }
    """
    assert "timeout_unhandled" not in codes(definition_from_dsl(src, "M"))


def test_outcome_on_nonterminal_warns():
    cfg = {
        "start": "A",
        "states": {"A": {"outcome": "failed"}, "B": {}},
        "transitions": [{"from": "A", "to": "B"}],
    }
    assert "outcome_on_nonterminal" in codes(build(cfg), "warning")


# --- terminal outcomes (the execution-boundary verdict) -----------------------


def test_terminal_missing_outcome_is_an_error():
    cfg = {
        "start": "A",
        "states": {"A": {}, "Done": {}},
        "transitions": [{"from": "A", "to": "Done", "on_event": {"type": "Go"}}],
    }
    issues = validate(build(cfg))
    assert any(i.code == "terminal_missing_outcome" and i.path == "Done" for i in issues)


def test_terminal_with_outcome_is_clean():
    cfg = {
        "start": "A",
        "states": {"A": {}, "Done": {"outcome": "success"}},
        "transitions": [{"from": "A", "to": "Done", "on_event": {"type": "Go"}}],
    }
    assert "terminal_missing_outcome" not in codes(build(cfg))


def test_leaf_sink_caught_by_a_composite_exit_is_exempt():
    # In2 sinks but Outer has an outgoing transition (Outer -> Done): the bubble is
    # caught, the Execution does not end at In2 -> In2 needs no outcome.
    cfg = {
        "start": "Outer",
        "states": {
            "Outer": {
                "start": "In1",
                "states": {"In1": {}, "In2": {}},
                "transitions": [{"from": "In1", "to": "In2", "on_event": {"type": "Step"}}],
            },
            "Done": {"outcome": "success"},
        },
        "transitions": [{"from": "Outer", "to": "Done", "on_event": {"type": "Step"}}],
    }
    paths = {i.path for i in validate(build(cfg)) if i.code == "terminal_missing_outcome"}
    assert paths == set()  # neither In2 (caught) nor Done (declared) is flagged


def test_terminal_nested_in_a_plain_composite_is_required():
    # L sinks and bubbles all the way to the root (Outer has no exit) -> it ends the
    # Execution -> L must declare an outcome; the composite Outer itself is exempt.
    cfg = {"start": "Outer", "states": {"Outer": {"start": "L", "states": {"L": {}}}}}
    issues = validate(build(cfg))
    flagged = {i.path for i in issues if i.code == "terminal_missing_outcome"}
    assert flagged == {"Outer.L"}  # the leaf, not the composite


def test_orthogonal_region_terminal_requires_outcome():
    # region A's terminal A2 ends region A's Execution -> must declare an outcome.
    cfg = {
        "start": "Fork",
        "states": {
            "Fork": {
                "type": "OrthogonalState",
                "states": {
                    "A": {
                        "type": "ParallelState",
                        "start": "A1",
                        "states": {"A1": {}, "A2": {}},
                        "transitions": [{"from": "A1", "to": "A2", "on_event": {"type": "Go"}}],
                    },
                },
            },
            "Done": {"outcome": "success"},
        },
        "transitions": [{"from": "Fork", "to": "Done"}],
    }
    issues = validate(build(cfg))
    assert any(i.code == "terminal_missing_outcome" and i.path == "Fork.A.A2" for i in issues)


# --- submachine invoke --------------------------------------------------------


def test_invoke_with_automatic_exit_is_an_error():
    # an automatic transition would fire before the submachine returns
    cfg = {
        "start": "Run",
        "states": {"Run": {"invoke": "acme.child"}, "Done": {"outcome": "success"}},
        "transitions": [{"from": "Run", "to": "Done"}],
    }
    assert "invoke_automatic_exit" in codes(build(cfg), "error")


def test_invoke_on_composite_is_an_error():
    cfg = {
        "start": "Run",
        "states": {
            "Run": {"invoke": "acme.child", "start": "X", "states": {"X": {"outcome": "success"}}},
            "Done": {"outcome": "success"},
        },
        "transitions": [{"from": "Run", "to": "Done", "on_event": {"type": "Returned"}}],
    }
    assert "invoke_on_composite" in codes(build(cfg), "error")


def test_invoke_with_returned_handler_is_clean():
    cfg = {
        "start": "Run",
        "states": {"Run": {"invoke": "acme.child"}, "Done": {"outcome": "success"}},
        "transitions": [{"from": "Run", "to": "Done", "on_event": {"type": "Returned"}}],
    }
    assert codes(build(cfg), "error") == set()


# --- typed events -------------------------------------------------------------


def test_unknown_event_when_declared():
    cfg = {
        "start": "A",
        "events": {"Known": {}},
        "states": {"A": {}, "B": {}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Mystery"}}],
    }
    assert "unknown_event" in codes(build(cfg), "error")


def test_unknown_event_field():
    cfg = {
        "start": "A",
        "events": {"Note": {"status": "string"}},
        "states": {"A": {}, "B": {}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Note", "missing__eq": "x"}}],
    }
    assert "unknown_event_field" in codes(build(cfg), "error")


def test_reserved_events_never_flagged():
    cfg = {
        "start": "A",
        "events": {"Known": {}},  # declared registry, but Timeout is reserved
        "states": {"A": {"timeout": 5}, "B": {}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Timeout"}}],
    }
    assert "unknown_event" not in codes(build(cfg))


def test_op_type_mismatch_warns():
    cfg = {
        "start": "A",
        "events": {"Note": {"label": "string"}},
        "states": {"A": {}, "B": {}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Note", "label__lt": "z"}}],
    }
    assert "op_type_mismatch" in codes(build(cfg), "warning")


def test_undeclared_event_is_an_error():
    # an event referenced but not declared is an error (a typo can't slip through) —
    # even when the machine declares no events at all.
    cfg = {
        "start": "A",
        "states": {"A": {}, "B": {"outcome": "success"}},
        "transitions": [{"from": "A", "to": "B", "on_event": {"type": "Whatever"}}],
    }
    issues = validate(build(cfg))
    assert any(i.code == "unknown_event" and i.severity == "error" for i in issues)


# --- event parsing ------------------------------------------------------------


def test_events_parsed_bare_and_dict_specs():
    cfg = {
        "start": "A",
        "events": {"E": {"a": "int", "b": {"type": "string", "required": False}}},
        "states": {"A": {}},
    }
    events = build(cfg).events
    assert events == {
        "E": EventType(
            name="E", fields={"a": FieldSpec(type="int"), "b": FieldSpec(type="string", required=False)}
        )
    }


# --- the raising entry point + the build flag ---------------------------------


def test_validate_or_raise_raises_on_error():
    cfg = {
        "start": "A",
        "states": {"A": {}, "B": {}, "C": {}},
        "transitions": [{"from": "A", "to": "B"}, {"from": "A", "to": "C"}],
    }
    with pytest.raises(ValidationError) as ei:
        validate_or_raise(build(cfg))
    assert any(i.code == "nondeterministic_automatic" for i in ei.value.issues)


def test_warnings_do_not_raise():
    cfg = {
        "start": "A",
        "events": {"Ping": {}},
        "states": {"A": {}, "Island": {"outcome": "success"}},
        "transitions": [{"from": "A", "to": "A", "on_event": {"type": "Ping"}}],
    }
    # only an `unreachable` warning => no raise (Island declares its outcome so the
    # terminal-outcome rule does not add an error)
    assert validate_or_raise(build(cfg))  # non-empty (the warning) but does not raise


def test_build_definition_validate_flag_raises():
    cfg = {
        "start": "A",
        "states": {"A": {}, "B": {}, "C": {}},
        "transitions": [{"from": "A", "to": "B"}, {"from": "A", "to": "C"}],
    }
    with pytest.raises(ValidationError):
        build_definition(cfg, {}, "M", validate=True)
