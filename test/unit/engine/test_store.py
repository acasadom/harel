"""Unit tests for the `ExecutionStore` optimistic-concurrency contract.

A save commits `version+1` iff the stored row is still at the loaded `version`;
a stale write (the row moved on) raises `StoreConflict`. This is the single-
writer-per-Execution backstop that lets many workers race for the same
Execution without corrupting its state (Phase C, step 1).
"""

import pytest

from harel.engine.execution import Execution
from harel.engine.store import DictStore, SqliteStore, StoreConflict


@pytest.fixture(params=["dict", "sqlite"])
def store(request, tmp_path):
    if request.param == "dict":
        yield DictStore()
    else:
        s = SqliteStore(tmp_path / "stm.db")
        yield s
        s.close()


def test_first_save_inserts_and_bumps_version(store):
    e = Execution(definition_id="d")
    assert e.version == 0
    store.save(e)
    assert e.version == 1
    loaded = store.load(e.id)
    assert loaded is not None and loaded.version == 1


def test_sequential_saves_increment_version(store):
    e = Execution(definition_id="d")
    store.save(e)
    store.save(e)
    store.save(e)
    assert e.version == 3
    assert store.load(e.id).version == 3


def test_stale_write_raises_conflict(tmp_path):
    # SqliteStore-only: load() deserializes a fresh copy, so two callers can hold
    # the same row at the same version (the real concurrent-writer scenario). The
    # DictStore returns the same object by design, so it has no value semantics.
    store = SqliteStore(tmp_path / "stm.db")
    e = Execution(definition_id="d")
    store.save(e)  # version -> 1

    # a second worker loads the same row (version 1) and commits first -> 2
    other = store.load(e.id)
    other.context["w"] = "won"
    store.save(other)
    assert other.version == 2

    # the first worker, still holding version 1, now loses the CAS
    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        store.save(e)
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1  # the in-memory bump was rolled back
    assert store.load(e.id).context["w"] == "won"  # the winner's state stands
    store.close()
