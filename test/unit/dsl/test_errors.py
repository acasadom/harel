"""Located, caret-annotated DSL errors.

Every parse failure and every structural failure that maps to a concrete source
node carries a 1-based `line`/`column` and renders the offending line with a `^`
caret (and, for the common slips, a `hint`). A positionless `DslError` still
str()s to just its message, so substring matchers keep working.
"""

import pytest

from harel.dsl import DslError, definition_from_dsl
from harel.dsl.parser import DslError as ParserDslError


def _err(text: str, **kw) -> DslError:
    with pytest.raises(DslError) as ei:
        definition_from_dsl(text, **kw)
    return ei.value


# --- syntax errors (the parse layer) ------------------------------------------


def test_syntax_error_has_position_and_caret():
    e = _err("machine M {\n  initial A\n  state A {}\n  state B {}\n  from A -> B\n}\n")
    assert e.line == 5
    assert e.column == 10
    s = str(e)
    assert "line 5, column 10" in s
    assert "from A -> B" in s and "^" in s  # the caret snippet
    assert "->" in (e.hint or "")  # the arrow hint


def test_unclosed_block_points_at_end_of_input():
    e = _err("machine M {\n  initial A\n  state A {\n")
    assert "end of input" in e.message
    assert e.line is not None and "^" in str(e)


def test_bad_selector_arrow_hint():
    e = _err("machine M {\n  initial A\n  state A {}\n  state B {}\n  from A => B\n}\n")
    assert "=>" in (e.hint or "")


# --- structural errors (the loader / builder layer) ---------------------------


def test_unresolvable_target_is_located():
    e = _err("machine M {\n  initial A\n  state A {}\n  from A to Nope\n}\n")
    assert "cannot resolve transition target 'Nope'" in e.message
    assert e.line == 4
    assert "from A to Nope" in str(e) and "^" in str(e)


def test_unbound_handler_anchored_at_machine():
    e = _err("machine M {\n  initial A\n  state A { on enter go }\n}\n")
    assert "unbound action handlers: go" in e.message
    assert e.line == 1  # anchored at the machine declaration
    assert "machine M" in str(e)


def test_unknown_fragment_is_located():
    e = _err("machine M {\n  initial A\n  state A {}\n  use Bogus(a = 1) as X\n}\n")
    assert "unknown fragment 'Bogus'" in e.message
    assert e.line == 4
    assert "use Bogus" in str(e)


def test_use_missing_args_is_located():
    e = _err(
        "fragment F(x: value, y: value) {\n  state S {}\n}\n"
        "machine M {\n  initial A\n  state A {}\n  use F(x = 1) as Y\n}\n"
    )
    assert "missing args: y" in e.message
    assert e.line == 7


# --- back-compat: a positionless error is just its message --------------------


def test_positionless_error_str_is_message_only():
    e = DslError("plain message")
    assert str(e) == "plain message"
    assert e.line is None


def test_dslerror_is_the_same_symbol_from_parser_and_package():
    assert DslError is ParserDslError
