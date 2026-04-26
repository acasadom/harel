"""PostgresStore contract tests, run against the real Postgres in the stack.

There is no in-process Postgres fake (unlike fakeredis), so these run as part of
the compose `test` service when STM_STORE_BACKEND=postgres (skipped otherwise).
They cover the store contract directly — version/CAS conflict, transactional
outbox, dedupe — which the pipeline test in test_compose_stack.py exercises only
along the happy path. Unique (uuid) execution ids isolate runs on the shared db.
"""

import os

import pytest

from harel.engine.execution import Execution
from harel.engine.store import StoreConflict
from harel.spec.states import Event

pytestmark = pytest.mark.stack


@pytest.fixture
def store():
    if os.environ.get("STM_STORE_BACKEND") != "postgres":
        pytest.skip("not the postgres backend")
    dsn = os.environ.get("STM_POSTGRES_DSN")
    if not dsn:
        pytest.skip("STM_POSTGRES_DSN not set")
    from harel.engine.store import PostgresStore

    s = PostgresStore.from_dsn(dsn)
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
    store.commit(e, [("parent-x", Event(kind="Finished"))], processed_event_id="evt-1")

    pending = [p for p in store.pending_outbox() if p.target_id == "parent-x"]
    assert len(pending) == 1 and pending[0].event.kind == "Finished"
    assert store.is_processed(e.id, "evt-1")
    assert not store.is_processed(e.id, "nope")

    store.ack_outbox(pending[0].seq)
    assert all(p.target_id != "parent-x" for p in store.pending_outbox())
