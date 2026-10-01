"""LibsqlStore (sync) failure handling, over a local libSQL file — no Docker."""

import pytest

pytest.importorskip("libsql")

from harel.engine.execution import Execution  # noqa: E402
from harel.engine.store import LibsqlStore  # noqa: E402


def test_a_failed_commit_rolls_back_and_releases_the_write_lock(tmp_path):
    # an error other than StoreConflict mid-commit (an unserializable spawn context, after
    # the Execution row was already written) must roll the whole transaction back and leave
    # the in-memory version untouched, so a retry is not a false conflict
    path = str(tmp_path / "stm.db")
    store = LibsqlStore(path)
    e = Execution(definition_id="d")

    with pytest.raises(TypeError):
        store.commit(e, [], spawns=(("child", "", {"bad": object()}),))

    assert e.version == 0
    assert store.load(e.id) is None
    other = LibsqlStore(path)
    other.commit(Execution(id="y", definition_id="d"), [])  # the write lock is free
    other.close()
    store.commit(e, [])  # the retry succeeds
    assert store.load(e.id).version == 1
    store.close()


def test_a_failed_save_rolls_back(tmp_path):
    store = LibsqlStore(str(tmp_path / "stm.db"))
    e = Execution(definition_id="d", context={"bad": object()})

    with pytest.raises(Exception):
        store.save(e)

    assert e.version == 0
    assert store.load(e.id) is None
    store.close()
