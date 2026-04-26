"""RqliteStore contract tests, run against the real rqlite in the stack.

No in-process rqlite fake, so these run as the compose `test` service when
STM_STORE_BACKEND=rqlite (skipped otherwise). They cover the store contract
directly — version/CAS conflict (the guarded-upsert path that is unique to
rqlite's non-interactive transactions), outbox, dedupe — which the pipeline test
exercises only along the happy path. Unique (uuid) ids isolate runs.
"""

import os

import pytest

from harel.engine.execution import Execution
from harel.engine.store import StoreConflict
from harel.spec.states import Event

pytestmark = pytest.mark.stack


@pytest.fixture
def store():
    if os.environ.get("STM_STORE_BACKEND") != "rqlite":
        pytest.skip("not the rqlite backend")
    url = os.environ.get("STM_RQLITE_URL")
    if not url:
        pytest.skip("STM_RQLITE_URL not set")
    from harel.engine.store import RqliteStore

    s = RqliteStore.from_url(url)
    yield s
    s.close()


def test_first_save_inserts_and_bumps_version(store):
    e = Execution(definition_id="d", context={"n": 1})
    store.save(e)
    assert e.version == 1
    loaded = store.load(e.id)
    assert loaded is not None and loaded.version == 1 and loaded.context == {"n": 1}


def test_sequential_saves_increment_version(store):
    e = Execution(definition_id="d")
    store.save(e)
    store.save(e)
    assert e.version == 2
    assert store.load(e.id).version == 2


def test_stale_write_raises_conflict(store):
    e = Execution(definition_id="d")
    store.save(e)  # version -> 1

    other = store.load(e.id)  # a second view at version 1
    other.context["w"] = "won"
    store.save(other)  # commits version 2

    e.context["w"] = "stale"
    with pytest.raises(StoreConflict) as exc:
        store.save(e)
    assert exc.value.expected == 1 and exc.value.found == 2
    assert e.version == 1
    assert store.load(e.id).context["w"] == "won"


def test_commit_outbox_and_dedupe(store):
    e = Execution(definition_id="d")
    store.commit(e, [("parent-r", Event(kind="Finished"))], processed_event_id="evt-r")

    pending = [p for p in store.pending_outbox() if p.target_id == "parent-r"]
    assert len(pending) == 1 and pending[0].event.kind == "Finished"
    assert store.is_processed(e.id, "evt-r")
    assert not store.is_processed(e.id, "nope")

    store.ack_outbox(pending[0].seq)
    assert all(p.target_id != "parent-r" for p in store.pending_outbox())


def test_conflict_leaves_outbox_untouched(store):
    # the guarded-upsert invariant: a CAS miss must not write the outbox/dedupe either
    e = Execution(definition_id="d")
    store.save(e)  # version 1
    before = len(store.pending_outbox())

    winner = store.load(e.id)  # version 1
    winner.context["who"] = "winner"
    store.save(winner)  # commits version 2 with distinct data

    e.context["who"] = "stale"  # `e` is still at version 1 -> its commit must lose
    with pytest.raises(StoreConflict):
        store.commit(e, [("p", Event(kind="X"))], processed_event_id="ev")
    assert len(store.pending_outbox()) == before  # no emit leaked
    assert not store.is_processed(e.id, "ev")
