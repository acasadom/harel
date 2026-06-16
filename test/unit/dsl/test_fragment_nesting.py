"""Nested fragments: a fragment may `use` another fragment, and forward its own
parameters (action / guard / value / state / event) as the nested use's args.

Forwarding-by-name resolves against the enclosing fragment's scope: action via the
bindings, guard via the guards, value via the values, state/event via the substitution
maps the `_Ctx` now carries. A forwarded name that the enclosing fragment doesn't bind
is a hard `DslError` (no silent leak of the parameter name)."""

import pytest

from harel.dsl import definition_from_dsl
from harel.dsl.parser import DslError


def _outgoing(defn, src_path, kind):
    for node in defn.index.values():
        for t in node.transitions:
            if t.source.full_path == src_path and t.event_filter and t.event_filter.kind == kind:
                return t
    raise AssertionError(f"no {kind} transition from {src_path}")


def test_fragment_uses_fragment_basic():
    """A fragment can instantiate another fragment with no forwarding — it splices as a
    nested composite under the outer one."""
    defn = definition_from_dsl(
        """
        fragment Inner { initial IA  state IA {}  state IB {}  from IA to IB on Go }
        fragment Outer { initial OX  state OX {}  use Inner() as In  from OX to In on Step }
        machine M { initial S  state S {}  use Outer() as Out  from S to Out on Begin }
        """,
        "M",
    )
    # the inner fragment is spliced two composites deep, with its own states/transition
    assert defn.get("Out.In.IA") is not None
    assert defn.get("Out.In.IB") is not None
    assert _outgoing(defn, "Out.In.IA", "Go").target.full_path == "Out.In.IB"


def test_forward_action_param():
    defn = definition_from_dsl(
        """
        fragment Inner(act: action) { initial IA  state IA { on enter act }  from IA to IA on Go }
        fragment Outer(work: action) { initial OX  state OX {}  use Inner(act = work) as In
                                       from OX to In on Step }
        machine M { initial S  state S {}  use Outer(work = pkg.mod.do) as Out  from S to Out on Begin }
        """,
        "M",
    )
    assert defn.get("Out.In.IA").on_enter.function == "pkg.mod.do"


def test_forward_guard_param():
    defn = definition_from_dsl(
        """
        fragment Inner(gate: guard) { initial IA  state IA {}  state IB {}  from IA to IB on Go where gate }
        fragment Outer(ok: guard) { initial OX  state OX {}  use Inner(gate = ok) as In
                                    from OX to In on Step }
        machine M { initial S  state S {}  use Outer(ok = (status == "ready")) as Out
                    from S to Out on Begin }
        """,
        "M",
    )
    assert _outgoing(defn, "Out.In.IA", "Go").event_filter.predicates == {"status__eq": "ready"}


def test_forward_value_param():
    defn = definition_from_dsl(
        """
        fragment Inner(budget: value) { initial IA  state IA {}  state IB {}  timeout budget
                                        from IA to IB on Go }
        fragment Outer(b: value) { initial OX  state OX {}  use Inner(budget = b) as In
                                   from OX to In on Step }
        machine M { initial S  state S {}  use Outer(b = 30) as Out  from S to Out on Begin }
        """,
        "M",
    )
    assert defn.get("Out.In").timeout == 30  # the literal, not the param name "b"


def test_forward_state_param():
    defn = definition_from_dsl(
        """
        fragment Inner(target: state) { initial IA  state IA {}  from IA to target on Go }
        fragment Outer(dest: state) { initial OX  state OX {}  use Inner(target = dest) as In
                                      from OX to In on Step }
        machine M { initial S  state S {}  state Done {}  use Outer(dest = Done) as Out
                    from S to Out on Begin  from Out to Done on Fin }
        """,
        "M",
    )
    # the inner `target` resolved all the way out to the machine-scope state `Done`
    assert _outgoing(defn, "Out.In.IA", "Go").target.full_path == "Done"


def test_forward_event_param():
    defn = definition_from_dsl(
        """
        event Kick {}
        fragment Inner(trigger: event) { initial IA  state IA {}  state IB {}  from IA to IB on trigger }
        fragment Outer(ev: event) { initial OX  state OX {}  use Inner(trigger = ev) as In
                                    from OX to In on Step }
        machine M { initial S  state S {}  use Outer(ev = Kick) as Out  from S to Out on Begin }
        """,
        "M",
    )
    assert _outgoing(defn, "Out.In.IA", "Kick").target.full_path == "Out.In.IB"


def test_forward_value_three_levels_deep():
    """Forwarding chains: a value param threaded through two nestings reaches the leaf."""
    defn = definition_from_dsl(
        """
        fragment Leaf(t: value) { initial LA  state LA {}  state LB {}  timeout t  from LA to LB on Go }
        fragment Mid(m: value) { initial MX  state MX {}  use Leaf(t = m) as L  from MX to L on Step }
        fragment Top(n: value) { initial TX  state TX {}  use Mid(m = n) as M2  from TX to M2 on Step }
        machine M { initial S  state S {}  use Top(n = 7) as T  from S to T on Begin }
        """,
        "M",
    )
    assert defn.get("T.M2.L").timeout == 7


def test_forward_unknown_name_is_a_clear_error():
    """Forwarding a name the enclosing fragment doesn't declare is a hard error, not a
    silent leak of the literal name."""
    with pytest.raises(DslError, match="unbound value params"):
        definition_from_dsl(
            """
            fragment Inner(budget: value) { initial A  state A {}  state B {}  timeout budget
                                            from A to B on Go }
            fragment Outer(b: value) { initial X  state X {}  use Inner(budget = nope) as In
                                       from X to In on Go }
            machine M { initial S  state S {}  use Outer(b = 5) as U  from S to U on Go }
            """,
            "M",
        )
