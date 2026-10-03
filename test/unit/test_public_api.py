"""What `harel` exports: every name in `__all__` exists, and the errors a caller catches come
from the package root, as the very classes the engine raises."""

import harel


def test_every_exported_name_exists():
    assert [name for name in harel.__all__ if not hasattr(harel, name)] == []


def test_the_errors_a_caller_catches_are_exported():
    from harel.definition.events import ContextError
    from harel.engine.control import PurgeRefused
    from harel.engine.core import ExpressionError
    from harel.engine.store import StoreConflict

    assert harel.ContextError is ContextError
    assert harel.ExpressionError is ExpressionError
    assert harel.StoreConflict is StoreConflict
    assert harel.PurgeRefused is PurgeRefused
