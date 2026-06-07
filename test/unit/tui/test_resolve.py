"""DefinitionSource — resolves a Definition for a listed Execution, never raising."""

from harel.dsl import definition_from_dsl
from harel.engine.resolve import ResolveError
from harel.tui.resolve import DefinitionSource

DEFN = definition_from_dsl("machine M { initial A  state A {} }", "M")


class _CountingResolver:
    def __init__(self, fail=False):
        self.calls = 0
        self._fail = fail

    def resolve(self, fqn):
        self.calls += 1
        if self._fail:
            raise ResolveError(fqn)
        return DEFN


def test_registry_hit():
    src = DefinitionSource(registry={"M": DEFN})
    assert src.get("M") is DEFN


def test_resolver_miss_returns_none_not_raise():
    src = DefinitionSource(resolver=_CountingResolver(fail=True))
    assert src.get("nope") is None  # ResolveError swallowed


def test_resolver_hit_and_caching():
    resolver = _CountingResolver()
    src = DefinitionSource(resolver=resolver)
    assert src.get("M") is DEFN
    assert src.get("M") is DEFN  # cached
    assert resolver.calls == 1  # not re-resolved


def test_miss_is_cached_too():
    resolver = _CountingResolver(fail=True)
    src = DefinitionSource(resolver=resolver)
    assert src.get("x") is None
    assert src.get("x") is None
    assert resolver.calls == 1  # a miss isn't re-resolved on every poll


def test_empty_resolves_nothing():
    assert DefinitionSource.empty().get("anything") is None
