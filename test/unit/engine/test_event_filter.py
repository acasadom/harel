"""Event-filter semantics (the engine's `_matches`): operator table, the fixed
`lt`==`eq` bug, missing-field (a predicate on an absent field fails), and the
composable `all`/`any`/`not` combinators."""

from harel.definition.builder import _event_filter
from harel.definition.model import EventFilter
from harel.engine.core import _matches
from harel.spec.states import Event


def _ev(**data) -> Event:
    return Event(kind="E", data=data)


def test_lt_is_strict_less_than():
    ef = EventFilter(kind="E", predicates={"n__lt": 5})
    assert _matches(ef, _ev(n=4))
    assert not _matches(ef, _ev(n=5))
    assert not _matches(ef, _ev(n=6))


def test_other_operators():
    assert _matches(EventFilter(kind="E", predicates={"n__eq": 5}), _ev(n=5))
    assert not _matches(EventFilter(kind="E", predicates={"n__eq": 5}), _ev(n=4))
    assert _matches(EventFilter(kind="E", predicates={"n__le": 5}), _ev(n=5))
    assert _matches(EventFilter(kind="E", predicates={"n__gt": 5}), _ev(n=6))
    assert _matches(EventFilter(kind="E", predicates={"n__ge": 5}), _ev(n=5))
    assert _matches(EventFilter(kind="E", predicates={"n__ne": 5}), _ev(n=4))
    assert _matches(EventFilter(kind="E", predicates={"s__in": [1, 2, 3]}), _ev(s=2))


def test_kind_alternation():
    ef = EventFilter(kind="A | B")
    assert _matches(ef, Event(kind="A"))
    assert _matches(ef, Event(kind="B"))
    assert not _matches(ef, Event(kind="C"))


def test_missing_field_fails():
    ef = _event_filter("E", {"n__eq": 5})
    assert _matches(ef, _ev(n=5))
    assert not _matches(ef, _ev())  # n absent -> predicate cannot be evaluated -> fails


def test_any_combinator():
    ef = _event_filter("E", {"any": [{"s__eq": "a"}, {"s__eq": "b"}]})
    assert _matches(ef, _ev(s="a"))
    assert _matches(ef, _ev(s="b"))
    assert not _matches(ef, _ev(s="c"))


def test_all_combinator():
    ef = _event_filter("E", {"all": [{"s__eq": "a"}, {"n__gt": 1}]})
    assert _matches(ef, _ev(s="a", n=2))
    assert not _matches(ef, _ev(s="a", n=1))
    assert not _matches(ef, _ev(s="b", n=2))


def test_not_combinator():
    ef = _event_filter("E", {"not": {"s__eq": "x"}})
    assert _matches(ef, _ev(s="y"))
    assert not _matches(ef, _ev(s="x"))


def test_nested_and_flat_combined_are_anded():
    ef = _event_filter(
        "E",
        {"k__eq": "v", "any": [{"r__eq": 1}, {"all": [{"r__eq": 2}, {"ok__eq": True}]}]},
    )
    assert _matches(ef, _ev(k="v", r=1))
    assert _matches(ef, _ev(k="v", r=2, ok=True))
    assert not _matches(ef, _ev(k="v", r=2, ok=False))  # inner all fails
    assert not _matches(ef, _ev(k="x", r=1))  # flat leaf fails
