"""The contract of the published machine versions: `assert_definitions_contract(store)` (and
`assert_async_definitions_contract` for an async store). harel runs it on its own backends;
run it on yours.

The contract: `load_definition` returns None for a version never published, and the record
`publish_definition` stored — source and all — once it was. A version is stored once:
publishing it again, with any content, stores nothing and returns the one stored first.
Versions of a machine are independent of each other.

Everything is namespaced by `ns`, so it holds on a real backend shared with other data.
"""

from harel.engine.store import DefinitionRecord


def _record(ns: str, version: int, digest: str) -> DefinitionRecord:
    source = {"main": "m.stm", "files": {"m.stm": f"machine {ns}m {{ }}\n", "lib/é.stm": "event E {}\n"}}
    return DefinitionRecord(f"{ns}m", version, digest, source, published_at=1700000000.5)


def _check_first(stored: DefinitionRecord, first: DefinitionRecord) -> None:
    assert stored == first, f"expected the first record published, got {stored!r}"


def assert_definitions_contract(store, ns: str = "def-") -> None:
    first = _record(ns, 1, "aaaa")
    assert store.load_definition(first.definition_id, 1) is None
    _check_first(store.publish_definition(first), first)
    _check_first(store.load_definition(first.definition_id, 1), first)

    # the same version again, with other content: nothing changes
    _check_first(store.publish_definition(_record(ns, 1, "bbbb")), first)
    _check_first(store.load_definition(first.definition_id, 1), first)

    second = _record(ns, 2, "bbbb")
    _check_first(store.publish_definition(second), second)
    _check_first(store.load_definition(first.definition_id, 1), first)
    assert store.load_definition(f"{ns}other", 1) is None


async def assert_async_definitions_contract(store, ns: str = "def-") -> None:
    first = _record(ns, 1, "aaaa")
    assert await store.load_definition(first.definition_id, 1) is None
    _check_first(await store.publish_definition(first), first)
    _check_first(await store.load_definition(first.definition_id, 1), first)

    _check_first(await store.publish_definition(_record(ns, 1, "bbbb")), first)
    _check_first(await store.load_definition(first.definition_id, 1), first)

    second = _record(ns, 2, "bbbb")
    _check_first(await store.publish_definition(second), second)
    _check_first(await store.load_definition(first.definition_id, 1), first)
    assert await store.load_definition(f"{ns}other", 1) is None
